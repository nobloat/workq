// Package workq is a persistent task queue backed by PostgreSQL or SQLite,
// using only database/sql. Native pgx support lives in github.com/nobloat/workq/pgxq. See README.md.
package workq

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"
)

// Driver connects a Queue to a database client; T is its transaction type (*sql.Tx, pgx.Tx).
type Driver[T any] interface {
	DB() Conn     // the connection pool
	Tx(tx T) Conn // a transaction
	SQLite() bool // false for Postgres
	Begin(ctx context.Context) (T, error)
	Commit(ctx context.Context, tx T) error
	Rollback(ctx context.Context, tx T) error
}

// Conn runs queries written with $N placeholders.
type Conn interface {
	Exec(ctx context.Context, query string, args ...any) (rowsAffected int64, err error)
	// Query calls row for each result row; scan reads its columns.
	Query(ctx context.Context, query string, args []any, row func(scan func(dest ...any) error) error) error
}

// Schedule returns the next run time given the time of the last successful run.
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
	Err      error         // Failed only
	Duration time.Duration // Completed and Failed only
}

type handler[T any] struct {
	fn   func(ctx context.Context, param string) error
	txFn func(ctx context.Context, tx T, param string) error
}

type Queue[T any] struct {
	PollInterval time.Duration
	LockTimeout  time.Duration
	OnEvent      func(Event)

	d         Driver[T]
	db        Conn
	sqlite    bool
	queue     string
	workers   int
	log       *slog.Logger
	handlers  map[string]handler[T]
	schedules map[string]Schedule

	mu         sync.Mutex
	running    map[int64]int64 // id -> version
	wg         sync.WaitGroup
	wake       chan struct{}
	loopCancel context.CancelFunc
	runCancel  context.CancelFunc
	loopDone   chan struct{}
	hbDone     chan struct{}
}

// id orders tasks within a queue. SQLite needs AUTOINCREMENT so ids of deleted tasks are never reused.
const (
	schemaPostgres = `CREATE TABLE IF NOT EXISTS workq_tasks (
  id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,` + columns
	schemaSQLite = `CREATE TABLE IF NOT EXISTS workq_tasks (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,` + columns
)

const columns = `
  name       TEXT NOT NULL,
  param      TEXT NOT NULL,
  queue      TEXT NOT NULL DEFAULT '',
  scheduled  BOOLEAN NOT NULL DEFAULT FALSE,
  run_at     BIGINT NOT NULL,
  locked_at  BIGINT,
  version    BIGINT NOT NULL DEFAULT 0,
  error      TEXT,
  failed_at  BIGINT,
  created_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS workq_tasks_due ON workq_tasks (queue, run_at) WHERE failed_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS workq_tasks_scheduled ON workq_tasks (name) WHERE scheduled;
`

// NewPostgres returns an executor that runs tasks of the given queue ("" is the default queue) with its own worker pool.
func NewPostgres(db *sql.DB, queue string, workers int, log *slog.Logger) *Queue[*sql.Tx] {
	return New[*sql.Tx](sqlDriver{db, false}, queue, workers, log)
}

// NewSQLite is NewPostgres for SQLite 3.35 or later.
func NewSQLite(db *sql.DB, queue string, workers int, log *slog.Logger) *Queue[*sql.Tx] {
	return New[*sql.Tx](sqlDriver{db, true}, queue, workers, log)
}

// New is NewPostgres for any Driver.
func New[T any](d Driver[T], queue string, workers int, log *slog.Logger) *Queue[T] {
	if log == nil {
		log = slog.Default()
	}
	return &Queue[T]{
		PollInterval: time.Second,
		LockTimeout:  5 * time.Minute,
		d:            d,
		db:           d.DB(),
		sqlite:       d.SQLite(),
		queue:        queue,
		workers:      workers,
		log:          log,
		handlers:     map[string]handler[T]{},
		schedules:    map[string]Schedule{},
		running:      map[int64]int64{},
		wake:         make(chan struct{}, 1),
	}
}

func (q *Queue[T]) CreateTable(ctx context.Context) error {
	schema := schemaPostgres
	if q.sqlite {
		schema = schemaSQLite
	}
	_, err := q.db.Exec(ctx, schema)
	return err
}

// Register panics if name is already registered.
func (q *Queue[T]) Register(name string, fn func(ctx context.Context, param string) error) {
	q.register(name, handler[T]{fn: fn})
}

// RegisterTx panics if name is already registered.
func (q *Queue[T]) RegisterTx(name string, fn func(ctx context.Context, tx T, param string) error) {
	q.register(name, handler[T]{txFn: fn})
}

func (q *Queue[T]) register(name string, h handler[T]) {
	if _, ok := q.handlers[name]; ok {
		panic("workq: task already registered: " + name)
	}
	q.handlers[name] = h
}

// Schedule attaches s to the already registered task name.
func (q *Queue[T]) Schedule(name string, s Schedule) {
	if _, ok := q.handlers[name]; !ok {
		panic("workq: Schedule called before Register for " + name)
	}
	if _, ok := q.schedules[name]; ok {
		panic("workq: schedule already registered: " + name)
	}
	q.schedules[name] = s
}

func (q *Queue[T]) Start(ctx context.Context) {
	now := time.Now()
	for name, s := range q.schedules {
		_, err := q.db.Exec(ctx, q.rebind(`INSERT INTO workq_tasks (name, param, queue, scheduled, run_at, created_at)
VALUES ($1, '', $2, TRUE, $3, $4) ON CONFLICT DO NOTHING`), name, q.queue, s(now).UnixMilli(), now.UnixMilli())
		if err != nil {
			q.log.Error("workq: create schedule", "name", name, "err", err)
		}
	}
	runCtx, runCancel := context.WithCancel(ctx)
	loopCtx, loopCancel := context.WithCancel(runCtx)
	q.runCancel, q.loopCancel = runCancel, loopCancel
	q.loopDone, q.hbDone = make(chan struct{}), make(chan struct{})
	go q.poll(loopCtx, runCtx)
	go q.heartbeat(runCtx)
}

// Stop stops claiming tasks and waits for running ones; handler contexts are cancelled when ctx expires.
func (q *Queue[T]) Stop(ctx context.Context) error {
	q.loopCancel()
	<-q.loopDone
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

// Enqueue inserts t and returns its id; t.ID is ignored.
func (q *Queue[T]) Enqueue(ctx context.Context, t Task) (int64, error) {
	return q.enqueue(ctx, q.db, t)
}

// EnqueueTx is Enqueue inside tx: the task exists only if tx commits.
func (q *Queue[T]) EnqueueTx(ctx context.Context, tx T, t Task) (int64, error) {
	return q.enqueue(ctx, q.d.Tx(tx), t)
}

func (q *Queue[T]) enqueue(ctx context.Context, db Conn, t Task) (int64, error) {
	t.CreatedAt = time.UnixMilli(time.Now().UnixMilli())
	if t.RunAt.IsZero() {
		t.RunAt = t.CreatedAt
	}
	t.RunAt = time.UnixMilli(t.RunAt.UnixMilli())
	err := db.Query(ctx, q.rebind(`INSERT INTO workq_tasks (name, param, queue, run_at, created_at)
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
	err := q.db.Query(ctx, `SELECT `+taskCols+` FROM workq_tasks WHERE failed_at IS NOT NULL ORDER BY id`, nil,
		func(scan func(...any) error) error {
			t, err := scanTask(scan)
			tasks = append(tasks, t)
			return err
		})
	return tasks, err
}

// Retry clears the failure of task id so it runs again; false if it isn't failed.
func (q *Queue[T]) Retry(ctx context.Context, id int64) (bool, error) {
	return q.retry(ctx, q.db, id)
}

// RetryTx is Retry inside tx.
func (q *Queue[T]) RetryTx(ctx context.Context, tx T, id int64) (bool, error) {
	return q.retry(ctx, q.d.Tx(tx), id)
}

func (q *Queue[T]) retry(ctx context.Context, db Conn, id int64) (bool, error) {
	var t Task
	found := false
	err := db.Query(ctx, q.rebind(`UPDATE workq_tasks SET error = NULL, failed_at = NULL, locked_at = NULL, run_at = $1
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

// Delete removes task id; false if it doesn't exist.
func (q *Queue[T]) Delete(ctx context.Context, id int64) (bool, error) {
	return q.exec(ctx, q.db, deleteSQL, id)
}

// DeleteTx is Delete inside tx.
func (q *Queue[T]) DeleteTx(ctx context.Context, tx T, id int64) (bool, error) {
	return q.exec(ctx, q.d.Tx(tx), deleteSQL, id)
}

const deleteSQL = `DELETE FROM workq_tasks WHERE id = $1`

// Wait blocks until no due or running tasks with the given names remain (all tasks if none given).
func (q *Queue[T]) Wait(ctx context.Context, names ...string) error {
	query := `SELECT COUNT(*) FROM workq_tasks WHERE failed_at IS NULL AND (run_at <= $1 OR locked_at IS NOT NULL)`
	args := []any{int64(0)}
	if len(names) > 0 {
		var ph string
		args, ph = in(args, names)
		query += ` AND name IN (` + ph + `)`
	}
	query = q.rebind(query)
	tick := time.NewTicker(q.PollInterval)
	defer tick.Stop()
	for {
		args[0] = time.Now().UnixMilli()
		var n int64
		if err := q.db.Query(ctx, query, args, func(scan func(...any) error) error { return scan(&n) }); err != nil {
			return err
		}
		if n == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func (q *Queue[T]) poll(ctx, runCtx context.Context) {
	defer close(q.loopDone)
	tick := time.NewTicker(q.PollInterval)
	defer tick.Stop()
	for {
		q.claim(context.WithoutCancel(ctx), runCtx) // a claim cancelled mid-flight would leave tasks locked until LockTimeout
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-q.wake:
		}
	}
}

// claimSQL picks due, unfailed, unlocked (or stale) tasks of this queue.
const claimSQL = `UPDATE workq_tasks SET locked_at = $1, version = version + 1
WHERE id IN (
  SELECT id FROM workq_tasks t
  WHERE t.failed_at IS NULL AND t.run_at <= $1
    AND (t.locked_at IS NULL OR t.locked_at < $2)
    AND t.queue = $4 AND t.name IN (%s)
  ORDER BY t.id LIMIT $3%s)
RETURNING ` + taskCols + `, version, scheduled`

func (q *Queue[T]) claim(ctx, runCtx context.Context) {
	q.mu.Lock()
	free := q.workers - len(q.running)
	q.mu.Unlock()
	if free <= 0 || len(q.handlers) == 0 {
		return
	}
	now := time.Now().UnixMilli()
	args, ph := in([]any{now, now - q.LockTimeout.Milliseconds(), free, q.queue}, slices.Collect(maps.Keys(q.handlers)))
	lock := ""
	if !q.sqlite {
		lock = " FOR UPDATE SKIP LOCKED"
	}
	type claimed struct {
		t         Task
		version   int64
		scheduled bool
	}
	var cs []claimed
	err := q.db.Query(ctx, q.rebind(fmt.Sprintf(claimSQL, ph, lock)), args, func(scan func(...any) error) (err error) {
		var c claimed
		c.t, err = scanTask(scan, &c.version, &c.scheduled)
		cs = append(cs, c)
		return err
	})
	if err != nil {
		q.log.Error("workq: claim", "err", err)
	}
	// Workers start only after the result is closed: on SQLite an open result holds the write lock.
	for _, c := range cs {
		q.mu.Lock()
		q.running[c.t.ID] = c.version
		q.mu.Unlock()
		q.wg.Add(1)
		go q.run(runCtx, c.t, c.version, c.scheduled)
	}
}

func (q *Queue[T]) run(ctx context.Context, t Task, version int64, scheduled bool) {
	defer q.wg.Done()
	defer func() {
		q.mu.Lock()
		delete(q.running, t.ID)
		q.mu.Unlock()
		q.notify()
	}()
	q.emit(Event{Type: Started, Task: t})
	start := time.Now()
	dbCtx := context.WithoutCancel(ctx) // finishing writes must happen even after the handler was cancelled
	h := q.handlers[t.Name]
	var s Schedule
	if scheduled {
		s = q.schedules[t.Name]
	}
	var herr, err error
	ok := false
	switch {
	case scheduled && s == nil:
		herr = fmt.Errorf("workq: %q is scheduled but has no schedule on this node", t.Name)
	case h.txFn != nil:
		herr, ok, err = q.runTx(ctx, dbCtx, h.txFn, t, version, s)
	default:
		if herr = protect(func() error { return h.fn(ctx, t.Param) }); herr == nil {
			ok, err = q.done(dbCtx, q.db, t.ID, version, s)
		}
	}
	if herr != nil {
		t.Error, t.FailedAt = herr.Error(), time.UnixMilli(time.Now().UnixMilli())
		ok, err = q.exec(dbCtx, q.db, `UPDATE workq_tasks SET error = $1, failed_at = $2, locked_at = NULL WHERE id = $3 AND version = $4`,
			t.Error, t.FailedAt.UnixMilli(), t.ID, version)
	}
	switch {
	case err != nil:
		q.log.Error("workq: finish task", "id", t.ID, "name", t.Name, "err", err)
	case !ok:
		q.log.Warn("workq: lost lock, result discarded", "id", t.ID, "name", t.Name)
	case herr != nil:
		q.emit(Event{Type: Failed, Task: t, Err: herr, Duration: time.Since(start)})
	default:
		q.emit(Event{Type: Completed, Task: t, Duration: time.Since(start)})
	}
}

func (q *Queue[T]) runTx(ctx, dbCtx context.Context, fn func(context.Context, T, string) error, t Task, version int64, s Schedule) (herr error, ok bool, err error) {
	tx, err := q.d.Begin(dbCtx)
	if err != nil {
		return nil, false, err
	}
	defer q.d.Rollback(dbCtx, tx) // no-op after Commit
	if herr = protect(func() error { return fn(ctx, tx, t.Param) }); herr != nil {
		return herr, false, nil
	}
	if ok, err = q.done(dbCtx, q.d.Tx(tx), t.ID, version, s); !ok || err != nil {
		return nil, ok, err
	}
	return nil, true, q.d.Commit(dbCtx, tx)
}

// done removes a finished task, or reschedules it when s is set.
func (q *Queue[T]) done(ctx context.Context, db Conn, id, version int64, s Schedule) (bool, error) {
	if s == nil {
		return q.exec(ctx, db, `DELETE FROM workq_tasks WHERE id = $1 AND version = $2`, id, version)
	}
	return q.exec(ctx, db, `UPDATE workq_tasks SET run_at = $1, locked_at = NULL WHERE id = $2 AND version = $3`,
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
		running := maps.Clone(q.running)
		q.mu.Unlock()
		now := time.Now().UnixMilli()
		for id, version := range running {
			if _, err := q.exec(ctx, q.db, `UPDATE workq_tasks SET locked_at = $1 WHERE id = $2 AND version = $3`, now, id, version); err != nil && ctx.Err() == nil {
				q.log.Error("workq: heartbeat", "id", id, "err", err)
			}
		}
	}
}

func (q *Queue[T]) exec(ctx context.Context, db Conn, query string, args ...any) (bool, error) {
	n, err := db.Exec(ctx, q.rebind(query), args...)
	return n > 0, err
}

// rebind turns $N into SQLite's ?N, so both databases share one query text.
func (q *Queue[T]) rebind(query string) string {
	if q.sqlite {
		return strings.ReplaceAll(query, "$", "?")
	}
	return query
}

func (q *Queue[T]) emit(e Event) {
	if q.OnEvent != nil {
		q.OnEvent(e)
	}
}

// notify wakes the poll loop without blocking.
func (q *Queue[T]) notify() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// in appends names to args and returns their placeholders.
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

// sqlDriver is the Driver for database/sql.
type sqlDriver struct {
	db     *sql.DB
	sqlite bool
}

func (d sqlDriver) DB() Conn                                     { return sqlConn{d.db} }
func (d sqlDriver) Tx(tx *sql.Tx) Conn                           { return sqlConn{tx} }
func (d sqlDriver) SQLite() bool                                 { return d.sqlite }
func (d sqlDriver) Begin(ctx context.Context) (*sql.Tx, error)   { return d.db.BeginTx(ctx, nil) }
func (d sqlDriver) Commit(_ context.Context, tx *sql.Tx) error   { return tx.Commit() }
func (d sqlDriver) Rollback(_ context.Context, tx *sql.Tx) error { return tx.Rollback() }

// sqlConn is satisfied by *sql.DB and *sql.Tx.
type sqlConn struct {
	c interface {
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	}
}

func (c sqlConn) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	res, err := c.c.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (c sqlConn) Query(ctx context.Context, query string, args []any, row func(scan func(dest ...any) error) error) error {
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
