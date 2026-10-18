package workq

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"time"
)

type Driver[T any] interface {
	DB() Conn
	Tx(tx T) Conn
	SQLite() bool
	Begin(ctx context.Context) (T, error)
	Commit(ctx context.Context, tx T) error
	Rollback(ctx context.Context, tx T) error
}

type Conn interface {
	Exec(ctx context.Context, query string, args ...any) (rowsAffected int64, err error)
	Query(ctx context.Context, query string, args []any, row func(scan func(dest ...any) error) error) error
}

type Schedule func(lastSuccess time.Time) time.Time

func Every(d time.Duration) Schedule {
	return func(t time.Time) time.Time { return t.Add(d) }
}

type Task struct {
	ID        int64
	Name      string
	Param     string
	Queue     string
	RunAt     time.Time
	CreatedAt time.Time
	FailedAt  time.Time
	Error     string
}

type EventType int

const (
	Enqueued EventType = iota
	Started
	Completed
	Failed
	Retried
)

type Event struct {
	Type     EventType
	Task     Task
	Err      error
	Duration time.Duration
}

type handler struct {
	fn       func(ctx context.Context, param string) error
	schedule Schedule
}

type execution struct {
	id, version int64
}

type Queue[T any] struct {
	PollInterval time.Duration
	LockTimeout  time.Duration
	OnEvent      func(Event)

	d        Driver[T]
	queue    string
	workers  int
	log      *slog.Logger
	handlers map[string]handler

	mu         sync.Mutex
	running    map[execution]struct{}
	wg         sync.WaitGroup
	wake       chan struct{}
	loopCancel context.CancelFunc
	runCancel  context.CancelFunc
	loopDone   chan struct{}
	hbDone     chan struct{}
}

//go:embed schema_postgres.sql
var schemaPostgres string

//go:embed schema_sqlite.sql
var schemaSQLite string

func NewPostgres(db *sql.DB, queue string, workers int, log *slog.Logger) *Queue[*sql.Tx] {
	return New[*sql.Tx](sqlDriver{db, false}, queue, workers, log)
}

func NewSQLite(db *sql.DB, queue string, workers int, log *slog.Logger) *Queue[*sql.Tx] {
	return New[*sql.Tx](sqlDriver{db, true}, queue, workers, log)
}

func New[T any](d Driver[T], queue string, workers int, log *slog.Logger) *Queue[T] {
	if log == nil {
		log = slog.Default()
	}
	return &Queue[T]{
		PollInterval: time.Second,
		LockTimeout:  5 * time.Minute,
		d:            d,
		queue:        queue,
		workers:      workers,
		log:          log,
		handlers:     map[string]handler{},
		running:      map[execution]struct{}{},
		wake:         make(chan struct{}, 1),
	}
}

func (q *Queue[T]) CreateTable(ctx context.Context) error {
	schema := schemaPostgres
	if q.d.SQLite() {
		schema = schemaSQLite
	}
	_, err := q.d.DB().Exec(ctx, schema)
	return err
}

func (q *Queue[T]) Register(name string, fn func(ctx context.Context, param string) error) error {
	return q.register(name, handler{fn: fn})
}

func (q *Queue[T]) register(name string, h handler) error {
	if _, ok := q.handlers[name]; ok {
		return fmt.Errorf("workq: task already registered: %s", name)
	}
	q.handlers[name] = h
	return nil
}

func (q *Queue[T]) Schedule(name string, s Schedule) error {
	h, ok := q.handlers[name]
	if !ok {
		return fmt.Errorf("workq: Schedule called before Register for %s", name)
	}
	if h.schedule != nil {
		return fmt.Errorf("workq: schedule already registered: %s", name)
	}
	if s == nil {
		return fmt.Errorf("workq: nil schedule for %s", name)
	}
	h.schedule = s
	q.handlers[name] = h
	return nil
}

func (q *Queue[T]) Start(ctx context.Context) error {
	var scheduled []string
	for name, h := range q.handlers {
		if h.schedule != nil {
			scheduled = append(scheduled, name)
		}
	}
	if err := q.createSchedules(ctx, scheduled); err != nil {
		return err
	}
	runCtx, runCancel := context.WithCancel(ctx)
	loopCtx, loopCancel := context.WithCancel(runCtx)
	q.runCancel, q.loopCancel = runCancel, loopCancel
	q.loopDone, q.hbDone = make(chan struct{}), make(chan struct{})
	go q.poll(loopCtx, runCtx)
	go q.heartbeat(runCtx)
	return nil
}

func (q *Queue[T]) Stop(ctx context.Context) error {
	q.loopCancel()
	select {
	case <-q.loopDone:
	case <-ctx.Done():
		q.runCancel()
		return ctx.Err()
	}
	done := make(chan struct{})
	go func() { q.wg.Wait(); close(done) }()
	select {
	case <-done:
		q.runCancel()
		<-q.hbDone
		return nil
	case <-ctx.Done():
		q.runCancel()
		return ctx.Err()
	}
}

func (q *Queue[T]) Enqueue(ctx context.Context, t Task) (int64, error) {
	return q.enqueue(ctx, q.d.DB(), t)
}

func (q *Queue[T]) EnqueueTx(ctx context.Context, tx T, t Task) (int64, error) {
	return q.enqueue(ctx, q.d.Tx(tx), t)
}

func (q *Queue[T]) enqueue(ctx context.Context, db Conn, t Task) (int64, error) {
	t.CreatedAt = time.UnixMilli(time.Now().UnixMilli())
	if t.RunAt.IsZero() {
		t.RunAt = t.CreatedAt
	}
	t.RunAt = time.UnixMilli(t.RunAt.UnixMilli())
	err := db.Query(ctx, rebind(q.d.SQLite(), `INSERT INTO workq (name, param, queue, run_at, created_at)
VALUES ($1, $2, $3, $4, $5) RETURNING id`),
		[]any{t.Name, t.Param, t.Queue, t.RunAt.UnixMilli(), t.CreatedAt.UnixMilli()},
		func(scan func(...any) error) error { return scan(&t.ID) })
	if err != nil {
		return 0, err
	}
	q.emit(Event{Type: Enqueued, Task: t})
	q.notify()
	return t.ID, nil
}

func (q *Queue[T]) Failed(ctx context.Context) ([]Task, error) {
	var tasks []Task
	err := q.d.DB().Query(ctx, `SELECT `+taskCols+` FROM workq WHERE failed_at IS NOT NULL ORDER BY id`, nil,
		func(scan func(...any) error) error {
			t, err := scanTask(scan)
			tasks = append(tasks, t)
			return err
		})
	return tasks, err
}

func (q *Queue[T]) Retry(ctx context.Context, id int64) (bool, error) {
	return q.retry(ctx, q.d.DB(), id)
}

func (q *Queue[T]) RetryTx(ctx context.Context, tx T, id int64) (bool, error) {
	return q.retry(ctx, q.d.Tx(tx), id)
}

func (q *Queue[T]) retry(ctx context.Context, db Conn, id int64) (bool, error) {
	var t Task
	found := false
	err := db.Query(ctx, rebind(q.d.SQLite(), `UPDATE workq SET error = NULL, failed_at = NULL, locked_at = NULL, run_at = $1
WHERE id = $2 AND failed_at IS NOT NULL RETURNING `+taskCols), []any{time.Now().UnixMilli(), id},
		func(scan func(...any) error) (err error) {
			t, err = scanTask(scan)
			found = true
			return err
		})
	if err != nil || !found {
		return false, err
	}
	q.emit(Event{Type: Retried, Task: t})
	q.notify()
	return true, nil
}

func (q *Queue[T]) Delete(ctx context.Context, id int64) (bool, error) {
	return q.exec(ctx, q.d.DB(), deleteSQL, id)
}

func (q *Queue[T]) DeleteTx(ctx context.Context, tx T, id int64) (bool, error) {
	return q.exec(ctx, q.d.Tx(tx), deleteSQL, id)
}

const deleteSQL = `DELETE FROM workq WHERE id = $1`

func (q *Queue[T]) Wait(ctx context.Context, names ...string) error {
	query := `SELECT 1 FROM workq WHERE failed_at IS NULL AND (run_at <= $1 OR locked_at IS NOT NULL)`
	args := []any{int64(0)}
	if len(names) > 0 {
		var ph string
		args, ph = in(args, names)
		query += ` AND name IN (` + ph + `)`
	}
	query = rebind(q.d.SQLite(), query+` LIMIT 1`)
	tick := time.NewTicker(q.PollInterval)
	defer tick.Stop()
	for {
		args[0] = time.Now().UnixMilli()
		pending := false
		if err := q.d.DB().Query(ctx, query, args, func(func(...any) error) error { pending = true; return nil }); err != nil {
			return err
		}
		if !pending {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func (q *Queue[T]) createSchedules(ctx context.Context, names []string) error {
	if len(names) == 0 {
		return nil
	}
	tx, err := q.d.Begin(ctx)
	if err != nil {
		return err
	}
	defer q.d.Rollback(ctx, tx)
	db := q.d.Tx(tx)
	now := time.Now()
	for _, name := range names {
		_, err := db.Exec(ctx, rebind(q.d.SQLite(), `INSERT INTO workq (name, param, queue, scheduled, run_at, created_at)
VALUES ($1, '', $2, TRUE, $3, $4) ON CONFLICT DO NOTHING`), name, q.queue, q.handlers[name].schedule(now).UnixMilli(), now.UnixMilli())
		if err != nil {
			return fmt.Errorf("workq: create schedule %q: %w", name, err)
		}
	}
	return q.d.Commit(ctx, tx)
}

func (q *Queue[T]) poll(ctx, runCtx context.Context) {
	defer close(q.loopDone)
	tick := time.NewTicker(q.PollInterval)
	defer tick.Stop()
	var cursor *Task
	for {
		claimCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), q.LockTimeout)
		cursor = q.claim(claimCtx, runCtx, cursor)
		cancel()
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			cursor = nil
		case <-q.wake:
		}
	}
}

const claimSQL = `UPDATE workq SET locked_at = $1, version = version + 1
WHERE id %s (
  SELECT id FROM workq t
  WHERE t.failed_at IS NULL AND t.run_at <= $1
    AND (t.locked_at IS NULL OR t.locked_at < $2)
    AND t.queue = $4 AND (t.run_at, t.id) > ($5, $6) AND t.name IN (%s)
  ORDER BY t.run_at, t.id LIMIT $3%s)%s
RETURNING ` + taskCols + `, version, scheduled`

func (q *Queue[T]) claim(ctx, runCtx context.Context, cursor *Task) *Task {
	q.mu.Lock()
	free := q.workers - len(q.running)
	q.mu.Unlock()
	if free <= 0 || len(q.handlers) == 0 {
		return cursor
	}
	now := time.Now().UnixMilli()
	after := []any{int64(math.MinInt64), int64(0)}
	if cursor != nil {
		after = []any{cursor.RunAt.UnixMilli(), cursor.ID}
	}
	args, ph := in(append([]any{now, now - q.LockTimeout.Milliseconds(), free, q.queue}, after...), slices.Collect(maps.Keys(q.handlers)))
	op, lock, end := "IN", "", ""
	if !q.d.SQLite() {
		op, lock, end = "= ANY(ARRAY", " FOR UPDATE SKIP LOCKED", ")"
	}
	type claimed struct {
		t         Task
		version   int64
		scheduled bool
	}
	var claimedTasks []claimed
	err := q.d.DB().Query(ctx, rebind(q.d.SQLite(), fmt.Sprintf(claimSQL, op, ph, lock, end)), args, func(scan func(...any) error) (err error) {
		var c claimed
		c.t, err = scanTask(scan, &c.version, &c.scheduled)
		claimedTasks = append(claimedTasks, c)
		return err
	})
	if err != nil {
		q.log.Error("workq: claim", "err", err)
	}
	var last *Task
	for _, c := range claimedTasks {
		if last == nil || c.t.RunAt.After(last.RunAt) || c.t.RunAt.Equal(last.RunAt) && c.t.ID > last.ID {
			last = &c.t
		}
		q.mu.Lock()
		q.running[execution{c.t.ID, c.version}] = struct{}{}
		q.mu.Unlock()
		q.wg.Add(1)
		go q.run(runCtx, c.t, c.version, c.scheduled)
	}
	if len(claimedTasks) < free {
		return nil
	}
	return last
}

func (q *Queue[T]) run(ctx context.Context, t Task, version int64, scheduled bool) {
	defer q.wg.Done()
	defer func() {
		q.mu.Lock()
		delete(q.running, execution{t.ID, version})
		q.mu.Unlock()
		q.notify()
	}()
	q.emit(Event{Type: Started, Task: t})
	start := time.Now()
	dbCtx := context.WithoutCancel(ctx)
	h := q.handlers[t.Name]
	var s Schedule
	if scheduled {
		s = h.schedule
	}
	var handlerErr, err error
	ok := false
	if scheduled && s == nil {
		handlerErr = fmt.Errorf("workq: %q is scheduled but has no schedule on this node", t.Name)
	} else if handlerErr = protect(func() error { return h.fn(ctx, t.Param) }); handlerErr == nil {
		ok, err = q.complete(dbCtx, q.d.DB(), t.ID, version, s)
	}
	if handlerErr != nil {
		t.Error, t.FailedAt = handlerErr.Error(), time.UnixMilli(time.Now().UnixMilli())
		ok, err = q.exec(dbCtx, q.d.DB(), `UPDATE workq SET error = $1, failed_at = $2, locked_at = NULL WHERE id = $3 AND version = $4`,
			t.Error, t.FailedAt.UnixMilli(), t.ID, version)
	}
	switch {
	case err != nil:
		q.log.Error("workq: finish task", "id", t.ID, "name", t.Name, "err", err)
	case !ok:
		q.log.Warn("workq: lost lock, result discarded", "id", t.ID, "name", t.Name)
	case handlerErr != nil:
		q.emit(Event{Type: Failed, Task: t, Err: handlerErr, Duration: time.Since(start)})
	default:
		q.emit(Event{Type: Completed, Task: t, Duration: time.Since(start)})
	}
}

func (q *Queue[T]) complete(ctx context.Context, db Conn, id, version int64, s Schedule) (bool, error) {
	if s == nil {
		return q.exec(ctx, db, `DELETE FROM workq WHERE id = $1 AND version = $2`, id, version)
	}
	return q.exec(ctx, db, `UPDATE workq SET run_at = $1, locked_at = NULL WHERE id = $2 AND version = $3`,
		s(time.Now()).UnixMilli(), id, version)
}

func (q *Queue[T]) heartbeat(ctx context.Context) {
	defer close(q.hbDone)
	tick := time.NewTicker(q.LockTimeout / 3)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		q.mu.Lock()
		pairs := slices.Collect(maps.Keys(q.running))
		q.mu.Unlock()
		for chunk := range slices.Chunk(pairs, 100) {
			args, where := matchAny([]any{time.Now().UnixMilli()}, chunk)
			if _, err := q.exec(ctx, q.d.DB(), `UPDATE workq SET locked_at = $1 WHERE locked_at IS NOT NULL AND (`+where+`)`, args...); err != nil && ctx.Err() == nil {
				q.log.Error("workq: heartbeat", "tasks", len(chunk), "err", err)
			}
		}
	}
}

func matchAny(args []any, pairs []execution) ([]any, string) {
	where := make([]string, len(pairs))
	for i, p := range pairs {
		args = append(args, p.id, p.version)
		where[i] = fmt.Sprintf("(id = $%d AND version = $%d)", len(args)-1, len(args))
	}
	return args, strings.Join(where, " OR ")
}

func (q *Queue[T]) exec(ctx context.Context, db Conn, query string, args ...any) (bool, error) {
	n, err := db.Exec(ctx, rebind(q.d.SQLite(), query), args...)
	return n > 0, err
}

func rebind(sqlite bool, query string) string {
	if sqlite {
		return strings.ReplaceAll(query, "$", "?")
	}
	return query
}

func (q *Queue[T]) emit(e Event) {
	if q.OnEvent != nil {
		q.OnEvent(e)
	}
}

func (q *Queue[T]) notify() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

func in(args []any, names []string) ([]any, string) {
	ph := make([]string, len(names))
	for i, n := range names {
		args = append(args, n)
		ph[i] = fmt.Sprintf("$%d", len(args))
	}
	return args, strings.Join(ph, ", ")
}

func protect(fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return fn()
}

const taskCols = `id, name, param, queue, run_at, created_at, failed_at, error`

func scanTask(scan func(...any) error, extra ...any) (Task, error) {
	var t Task
	var errMsg sql.NullString
	var runAt, createdAt int64
	var failedAt sql.NullInt64
	err := scan(append([]any{&t.ID, &t.Name, &t.Param, &t.Queue, &runAt, &createdAt, &failedAt, &errMsg}, extra...)...)
	t.Error = errMsg.String
	t.RunAt, t.CreatedAt = time.UnixMilli(runAt), time.UnixMilli(createdAt)
	if failedAt.Valid {
		t.FailedAt = time.UnixMilli(failedAt.Int64)
	}
	return t, err
}

type sqlDriver struct {
	db     *sql.DB
	sqlite bool
}

func (d sqlDriver) DB() Conn                                     { return sqlConn{d.db, d.lock()} }
func (d sqlDriver) Tx(tx *sql.Tx) Conn                           { return sqlConn{tx, nil} }
func (d sqlDriver) SQLite() bool                                 { return d.sqlite }
func (d sqlDriver) Begin(ctx context.Context) (*sql.Tx, error)   { return d.db.BeginTx(ctx, nil) }
func (d sqlDriver) Commit(_ context.Context, tx *sql.Tx) error   { return tx.Commit() }
func (d sqlDriver) Rollback(_ context.Context, tx *sql.Tx) error { return tx.Rollback() }

var sqliteLocks sync.Map

func (d sqlDriver) lock() chan struct{} {
	if !d.sqlite {
		return nil
	}
	l, _ := sqliteLocks.LoadOrStore(d.db, make(chan struct{}, 1))
	return l.(chan struct{})
}

type sqlConn struct {
	c interface {
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	}
	lock chan struct{}
}

func (c sqlConn) acquire(ctx context.Context) (release func(), err error) {
	if c.lock == nil {
		return func() {}, nil
	}
	select {
	case c.lock <- struct{}{}:
		return func() { <-c.lock }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c sqlConn) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	release, err := c.acquire(ctx)
	if err != nil {
		return 0, err
	}
	defer release()
	res, err := c.c.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (c sqlConn) Query(ctx context.Context, query string, args []any, row func(scan func(dest ...any) error) error) error {
	release, err := c.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	rows, err := c.c.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := row(rows.Scan); err != nil {
			return err
		}
	}
	return rows.Err()
}
