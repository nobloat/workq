package tests

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/nobloat/workq"
	"github.com/nobloat/workq/pgxq"
	_ "modernc.org/sqlite"
)

var pgDSN, pgSkip string

// TestMain starts a throwaway Postgres container through the docker CLI.
func TestMain(m *testing.M) {
	cleanup, err := startPostgres()
	if err != nil {
		if os.Getenv("CI") != "" {
			fmt.Fprintln(os.Stderr, "postgres:", err)
			os.Exit(1)
		}
		pgSkip = err.Error()
	}
	code := m.Run()
	cleanup()
	os.Exit(code)
}

func startPostgres() (func(), error) {
	nop := func() {}
	if err := exec.Command("docker", "info").Run(); err != nil {
		return nop, fmt.Errorf("docker not available: %w", err)
	}
	removeStaleContainers()
	out, err := exec.Command("docker", "run", "-d", "--rm", "--label", "workq-test="+strconv.Itoa(os.Getpid()),
		"-e", "POSTGRES_PASSWORD=pw", "-p", "127.0.0.1::5432", "postgres:17-alpine").Output()
	if err != nil {
		return nop, fmt.Errorf("docker run: %w", err)
	}
	id := strings.TrimSpace(string(out))
	cleanup := func() { exec.Command("docker", "rm", "-f", "-v", id).Run() } // -v: also the anonymous data volume
	out, err = exec.Command("docker", "port", id, "5432/tcp").Output()
	if err != nil {
		cleanup()
		return nop, fmt.Errorf("docker port: %w", err)
	}
	addr := strings.TrimSpace(strings.Split(string(out), "\n")[0])
	pgDSN = "postgres://postgres:pw@" + addr + "/postgres?sslmode=disable"
	db, err := sql.Open("pgx", pgDSN)
	if err != nil {
		cleanup()
		return nop, err
	}
	defer db.Close()
	// The image's init script runs a temporary server without TCP, so a successful ping means the real one is up.
	for deadline := time.Now().Add(60 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		if err = db.Ping(); err == nil {
			return cleanup, nil
		}
		if time.Now().After(deadline) {
			cleanup()
			return nop, fmt.Errorf("postgres not ready: %w", err)
		}
	}
}

// env is one backend. db is always database/sql, for assertions; executors use the backend's own client.
// removeStaleContainers removes containers of earlier runs whose process is gone, for example
// after a -timeout panic, which skips TestMain's cleanup. Containers of running test processes stay.
func removeStaleContainers() {
	out, _ := exec.Command("docker", "ps", "-a", "--filter", "label=workq-test", "--format", `{{.ID}} {{.Label "workq-test"}}`).Output()
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		id, pid, ok := strings.Cut(line, " ")
		if !ok {
			continue
		}
		if p, err := strconv.Atoi(pid); err == nil {
			if err := syscall.Kill(p, 0); err == nil || errors.Is(err, syscall.EPERM) {
				continue // still running
			}
		}
		exec.Command("docker", "rm", "-f", "-v", id).Run()
	}
}

type env[T any] struct {
	db   *sql.DB
	main node[T]
	open func(tb testing.TB, maxConns int) node[T] // a separate connection pool, as another node would have; 0 = default size
	exec func(ctx context.Context, tx T, query string, args ...any) error
}

// node is an executor factory bound to one connection pool.
type node[T any] struct {
	queue    func(queue string, workers int) *workq.Queue[T]
	begin    func(ctx context.Context) (T, error)
	rollback func(tx T)
	close    func()
}

func setupSQLite(tb testing.TB) env[*sql.Tx] {
	dsn := "file:" + filepath.Join(tb.TempDir(), "db.sqlite") + "?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_txlock=immediate"
	return sqlEnv(tb, "sqlite", dsn, workq.NewSQLite, "")
}

func setupPostgres(tb testing.TB) env[*sql.Tx] {
	if pgSkip != "" {
		tb.Skip(pgSkip)
	}
	return sqlEnv(tb, "pgx", pgDSN, workq.NewPostgres, resetPostgres)
}

func setupPgx(tb testing.TB) env[pgx.Tx] {
	if pgSkip != "" {
		tb.Skip(pgSkip)
	}
	open := func(tb testing.TB, maxConns int) node[pgx.Tx] {
		cfg, err := pgxpool.ParseConfig(pgDSN)
		if err != nil {
			tb.Fatal(err)
		}
		if maxConns > 0 {
			cfg.MaxConns = int32(maxConns)
		}
		pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
		if err != nil {
			tb.Fatal(err)
		}
		tb.Cleanup(pool.Close)
		return node[pgx.Tx]{
			queue: func(queue string, workers int) *workq.Queue[pgx.Tx] {
				return pgxq.New(pool, queue, workers, slog.Default())
			},
			begin:    func(ctx context.Context) (pgx.Tx, error) { return pool.Begin(ctx) },
			rollback: func(tx pgx.Tx) { tx.Rollback(context.Background()) },
			close:    pool.Close,
		}
	}
	exec := func(ctx context.Context, tx pgx.Tx, query string, args ...any) error {
		_, err := tx.Exec(ctx, query, args...)
		return err
	}
	return newEnv(tb, openSQL(tb, "pgx", pgDSN, 0), open, exec, resetPostgres)
}

const resetPostgres = "DROP SCHEMA public CASCADE; CREATE SCHEMA public"

func sqlEnv(tb testing.TB, driver, dsn string, newQ func(*sql.DB, string, int, *slog.Logger) *workq.Queue[*sql.Tx], reset string) env[*sql.Tx] {
	open := func(tb testing.TB, maxConns int) node[*sql.Tx] {
		db := openSQL(tb, driver, dsn, maxConns)
		return node[*sql.Tx]{
			queue:    func(queue string, workers int) *workq.Queue[*sql.Tx] { return newQ(db, queue, workers, slog.Default()) },
			begin:    func(ctx context.Context) (*sql.Tx, error) { return db.BeginTx(ctx, nil) },
			rollback: func(tx *sql.Tx) { tx.Rollback() },
			close:    func() { db.Close() },
		}
	}
	exec := func(ctx context.Context, tx *sql.Tx, query string, args ...any) error {
		_, err := tx.ExecContext(ctx, query, args...)
		return err
	}
	return newEnv(tb, openSQL(tb, driver, dsn, 0), open, exec, reset)
}

func newEnv[T any](tb testing.TB, db *sql.DB, open func(testing.TB, int) node[T], exec func(context.Context, T, string, ...any) error, reset string) env[T] {
	if reset != "" {
		if _, err := db.Exec(reset); err != nil {
			tb.Fatal(err)
		}
	}
	e := env[T]{db: db, main: open(tb, 0), open: open, exec: exec}
	if err := e.main.queue("", 1).CreateTable(tb.Context()); err != nil {
		tb.Fatal(err)
	}
	return e
}

func openSQL(tb testing.TB, driver, dsn string, maxConns int) *sql.DB {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		tb.Fatal(err)
	}
	if maxConns > 0 {
		db.SetMaxOpenConns(maxConns)
		db.SetMaxIdleConns(maxConns) // the default of 2 makes busy workers reconnect constantly
	}
	tb.Cleanup(func() { db.Close() })
	return db
}

// each runs the test against every backend inside a synctest bubble:
// time is fake and only advances while every goroutine is blocked, so minutes pass instantly.
// Pass the same generic test function twice; Go instantiates it once per transaction type.
func each(t *testing.T, sqlTest func(*testing.T, env[*sql.Tx]), pgxTest func(*testing.T, env[pgx.Tx])) {
	t.Run("sqlite", func(t *testing.T) { synctest.Test(t, func(t *testing.T) { sqlTest(t, setupSQLite(t)) }) })
	t.Run("postgres", func(t *testing.T) { synctest.Test(t, func(t *testing.T) { sqlTest(t, setupPostgres(t)) }) })
	t.Run("pgx", func(t *testing.T) { synctest.Test(t, func(t *testing.T) { pgxTest(t, setupPgx(t)) }) })
}

// node returns an executor on its own connection pool and a func that closes the pool.
func (e env[T]) node(t *testing.T, lockTimeout time.Duration) (*workq.Queue[T], func()) {
	n := e.open(t, 0)
	q := n.queue("", 2)
	q.PollInterval = time.Second
	q.LockTimeout = lockTimeout
	return q, n.close
}

func (e env[T]) queue(workers int) *workq.Queue[T] {
	return e.named("", workers)
}

func (e env[T]) named(queue string, workers int) *workq.Queue[T] {
	q := e.main.queue(queue, workers)
	q.PollInterval = time.Second
	return q
}

func start[T any](t *testing.T, q *workq.Queue[T]) {
	if err := q.Start(t.Context()); err != nil {
		t.Fatal("start:", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		if err := q.Stop(ctx); err != nil {
			t.Error("stop:", err)
		}
	})
}

func wait[T any](t *testing.T, q *workq.Queue[T], names ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
	defer cancel()
	if err := q.Wait(ctx, names...); err != nil {
		t.Fatal("wait:", err)
	}
}

func enqueue[T any](t *testing.T, q *workq.Queue[T], task workq.Task) int64 {
	t.Helper()
	id, err := q.Enqueue(t.Context(), task)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func count[T any](t *testing.T, e env[T], where string) int {
	t.Helper()
	var n int
	if err := e.db.QueryRow(`SELECT COUNT(*) FROM workq WHERE ` + where).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func failed[T any](t *testing.T, q *workq.Queue[T]) []workq.Task {
	t.Helper()
	f, err := q.Failed(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestRunAndEvents(t *testing.T) { each(t, testRunAndEvents, testRunAndEvents) }

func testRunAndEvents[T any](t *testing.T, e env[T]) {
	q := e.queue(2)
	var mu sync.Mutex
	var events []workq.EventType
	var got []string
	q.OnEvent = func(ev workq.Event) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, ev.Type)
	}
	q.Register("echo", func(ctx context.Context, p string) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, p)
		return nil
	})
	start(t, q)
	id := enqueue(t, q, workq.Task{Name: "echo", Param: "hello"})
	wait(t, q, "echo")
	mu.Lock()
	defer mu.Unlock()
	if id == 0 || len(got) != 1 || got[0] != "hello" {
		t.Fatalf("id=%d got=%v", id, got)
	}
	if fmt.Sprint(events) != fmt.Sprint([]workq.EventType{workq.Enqueued, workq.Started, workq.Completed}) {
		t.Fatalf("events %v", events)
	}
	if n := count(t, e, "1=1"); n != 0 {
		t.Fatalf("%d rows left", n)
	}
}

func TestFailureRetryDelete(t *testing.T) { each(t, testFailureRetryDelete, testFailureRetryDelete) }

func testFailureRetryDelete[T any](t *testing.T, e env[T]) {
	q := e.queue(2)
	var calls atomic.Int32
	var failedEvents atomic.Int32
	q.OnEvent = func(ev workq.Event) {
		if ev.Type == workq.Failed && ev.Err != nil {
			failedEvents.Add(1)
		}
	}
	q.Register("flaky", func(ctx context.Context, p string) error {
		if calls.Add(1) == 1 {
			return errors.New("boom")
		}
		return nil
	})
	q.Register("panics", func(ctx context.Context, p string) error { panic("oops") })
	start(t, q)
	flaky := enqueue(t, q, workq.Task{Name: "flaky"})
	panics := enqueue(t, q, workq.Task{Name: "panics"})
	wait(t, q)

	f := failed(t, q)
	if len(f) != 2 || f[0].ID != flaky || f[0].Error != "boom" || f[0].FailedAt.IsZero() ||
		f[1].ID != panics || !strings.Contains(f[1].Error, "oops") {
		t.Fatalf("failed: %+v", f)
	}
	if failedEvents.Load() != 2 {
		t.Fatalf("failed events %d", failedEvents.Load())
	}
	if ok, err := q.Retry(t.Context(), flaky); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if ok, _ := q.Retry(t.Context(), flaky); ok {
		t.Fatal("retry of a non-failed task succeeded")
	}
	wait(t, q)
	if calls.Load() != 2 || count(t, e, fmt.Sprint("id = ", flaky)) != 0 {
		t.Fatalf("calls=%d", calls.Load())
	}
	if ok, err := q.Delete(t.Context(), panics); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if len(failed(t, q)) != 0 {
		t.Fatal("failed task not deleted")
	}
}

func TestEnqueueTxRollback(t *testing.T) { each(t, testEnqueueTxRollback, testEnqueueTxRollback) }

func testEnqueueTxRollback[T any](t *testing.T, e env[T]) {
	q := e.queue(1)
	tx, err := e.main.begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.EnqueueTx(t.Context(), tx, workq.Task{Name: "x"}); err != nil {
		t.Fatal(err)
	}
	e.main.rollback(tx)
	if n := count(t, e, "1=1"); n != 0 {
		t.Fatalf("%d rows after rollback", n)
	}
}

func TestQueues(t *testing.T) { each(t, testQueues, testQueues) }

func testQueues[T any](t *testing.T, e env[T]) {
	var mu sync.Mutex
	ran := map[string]string{} // param -> queue that ran it
	def, mails := e.queue(4), e.named("mails", 1)
	for name, q := range map[string]*workq.Queue[T]{"": def, "mails": mails} {
		q.Register("send", func(ctx context.Context, p string) error {
			mu.Lock()
			defer mu.Unlock()
			ran[p] = name
			return nil
		})
		start(t, q)
	}
	enqueue(t, def, workq.Task{Name: "send", Param: "a"})
	enqueue(t, def, workq.Task{Name: "send", Param: "b", Queue: "mails"})
	wait(t, def)
	mu.Lock()
	defer mu.Unlock()
	if ran["a"] != "" || ran["b"] != "mails" || len(ran) != 2 {
		t.Fatalf("ran %v", ran)
	}
}

func TestRoutingAndMultiNode(t *testing.T) { each(t, testRoutingAndMultiNode, testRoutingAndMultiNode) }

func testRoutingAndMultiNode[T any](t *testing.T, e env[T]) {
	var aRuns, bRuns, shared atomic.Int32
	a, b := e.queue(4), e.queue(4)
	a.Register("only-a", func(ctx context.Context, p string) error { aRuns.Add(1); return nil })
	b.Register("only-b", func(ctx context.Context, p string) error { bRuns.Add(1); return nil })
	for _, q := range []*workq.Queue[T]{a, b} {
		q.Register("shared", func(ctx context.Context, p string) error { shared.Add(1); return nil })
	}
	for range 3 {
		enqueue(t, a, workq.Task{Name: "only-a"})
		enqueue(t, a, workq.Task{Name: "only-b"})
	}
	for range 50 {
		enqueue(t, a, workq.Task{Name: "shared"})
	}
	start(t, a)
	wait(t, a, "only-a", "shared")
	if n := count(t, e, "name = 'only-b'"); n != 3 {
		t.Fatalf("node a touched only-b tasks: %d left", n)
	}
	start(t, b)
	wait(t, a)
	if aRuns.Load() != 3 || bRuns.Load() != 3 || shared.Load() != 50 {
		t.Fatalf("a=%d b=%d shared=%d", aRuns.Load(), bRuns.Load(), shared.Load())
	}
}

func TestSchedule(t *testing.T) { each(t, testSchedule, testSchedule) }

func testSchedule[T any](t *testing.T, e env[T]) {
	var runs atomic.Int32
	var fail atomic.Bool
	h := func(ctx context.Context, p string) error {
		runs.Add(1)
		if fail.Load() {
			return errors.New("down")
		}
		return nil
	}
	a, b := e.queue(1), e.queue(1)
	for _, q := range []*workq.Queue[T]{a, b} {
		q.Register("tick", h)
		q.Schedule("tick", workq.Every(time.Minute))
		start(t, q)
	}
	if n := count(t, e, "scheduled"); n != 1 {
		t.Fatalf("%d schedule rows", n)
	}
	for runs.Load() < 3 {
		time.Sleep(time.Second)
	}
	wait(t, a) // returns even though a schedule exists

	fail.Store(true)
	for len(failed(t, a)) == 0 {
		time.Sleep(time.Second)
	}
	stopped := runs.Load()
	time.Sleep(5 * time.Minute)
	if runs.Load() != stopped {
		t.Fatal("failed schedule kept running")
	}
	fail.Store(false)
	f := failed(t, a)
	if ok, err := a.Retry(t.Context(), f[0].ID); !ok || err != nil {
		t.Fatal(ok, err)
	}
	for runs.Load() < stopped+2 {
		time.Sleep(time.Second)
	}
	if count(t, e, "scheduled") != 1 || len(failed(t, a)) != 0 {
		t.Fatal("schedule not back to normal")
	}
}

func TestStaleLockIsTakenOver(t *testing.T) {
	each(t, testStaleLockIsTakenOver, testStaleLockIsTakenOver)
}

func testStaleLockIsTakenOver[T any](t *testing.T, e env[T]) {
	q := e.queue(1)
	var runs atomic.Int32
	q.Register("x", func(ctx context.Context, p string) error { runs.Add(1); return nil })
	id := enqueue(t, q, workq.Task{Name: "x"})
	// a dead node locked it 10 minutes ago
	if _, err := e.db.Exec(fmt.Sprintf(`UPDATE workq SET locked_at = %d, version = 1 WHERE id = %d`, time.Now().Add(-10*time.Minute).UnixMilli(), id)); err != nil {
		t.Fatal(err)
	}
	start(t, q)
	wait(t, q)
	if runs.Load() != 1 {
		t.Fatal("stale task not run")
	}
}

func TestLostLockDiscardsResult(t *testing.T) {
	each(t, testLostLockDiscardsResult, testLostLockDiscardsResult)
}

func testLostLockDiscardsResult[T any](t *testing.T, e env[T]) {
	q := e.queue(2)
	q.LockTimeout = time.Hour
	takeover := `UPDATE workq SET version = version + 1 WHERE name = $1`
	q.Register("plain", func(ctx context.Context, p string) error {
		_, err := e.db.ExecContext(ctx, takeover, "plain")
		return err
	})
	enqueue(t, q, workq.Task{Name: "plain"})
	start(t, q)
	for count(t, e, "locked_at IS NOT NULL") != 1 || count(t, e, "version = 2") != 1 {
		time.Sleep(time.Second)
	}
	time.Sleep(time.Minute)
	if count(t, e, "1=1") != 1 || len(failed(t, q)) != 0 {
		t.Fatalf("rows=%d", count(t, e, "1=1"))
	}
}

func TestRunAtAndStop(t *testing.T) { each(t, testRunAtAndStop, testRunAtAndStop) }

func testRunAtAndStop[T any](t *testing.T, e env[T]) {
	q := e.queue(2)
	var ran atomic.Bool
	q.Register("later", func(ctx context.Context, p string) error { ran.Store(true); return nil })
	started := make(chan struct{})
	q.Register("slow", func(ctx context.Context, p string) error {
		close(started)
		time.Sleep(time.Minute)
		return nil
	})
	start(t, q)
	enqueue(t, q, workq.Task{Name: "later", RunAt: time.Now().Add(5 * time.Minute)})
	wait(t, q) // tasks scheduled for later don't count
	time.Sleep(time.Minute)
	if ran.Load() {
		t.Fatal("ran before RunAt")
	}
	time.Sleep(5 * time.Minute)
	wait(t, q)
	if !ran.Load() {
		t.Fatal("did not run")
	}

	// Stop drains running tasks
	enqueue(t, q, workq.Task{Name: "slow"})
	<-started
	if err := q.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	if n := count(t, e, "1=1"); n != 0 {
		t.Fatalf("stop did not drain: %d rows", n)
	}
	if err := q.Start(t.Context()); err != nil { // for the cleanup's Stop
		t.Fatal(err)
	}
}

func TestStopTimeoutCancelsHandlers(t *testing.T) {
	each(t, testStopTimeoutCancelsHandlers, testStopTimeoutCancelsHandlers)
}

func testStopTimeoutCancelsHandlers[T any](t *testing.T, e env[T]) {
	q := e.queue(1)
	started := make(chan struct{})
	q.Register("stuck", func(ctx context.Context, p string) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	})
	if err := q.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	enqueue(t, q, workq.Task{Name: "stuck"})
	<-started
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := q.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	for len(failed(t, q)) == 0 { // the cancelled handler's failure is still stored
		time.Sleep(time.Second)
	}
}

func TestCompletionAfterStopTimeout(t *testing.T) {
	each(t, testCompletionAfterStopTimeout, testCompletionAfterStopTimeout)
}

func testCompletionAfterStopTimeout[T any](t *testing.T, e env[T]) {
	q := e.queue(1)
	started, completed := make(chan struct{}), make(chan struct{})
	q.Register("job", func(ctx context.Context, p string) error {
		close(started)
		<-ctx.Done()
		return nil
	})
	q.OnEvent = func(ev workq.Event) {
		if ev.Type == workq.Completed {
			close(completed)
		}
	}
	if err := q.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	enqueue(t, q, workq.Task{Name: "job"})
	<-started
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := q.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	<-completed
	if n := count(t, e, "TRUE"); n != 0 {
		t.Fatalf("%d tasks remain after completion", n)
	}
}

func TestHeartbeatKeepsLock(t *testing.T) { each(t, testHeartbeatKeepsLock, testHeartbeatKeepsLock) }

func testHeartbeatKeepsLock[T any](t *testing.T, e env[T]) {
	// the task runs 3x longer than LockTimeout while the other node polls
	var runs atomic.Int32
	h := func(ctx context.Context, p string) error {
		runs.Add(1)
		time.Sleep(3 * time.Minute)
		return nil
	}
	for range 2 {
		q, _ := e.node(t, time.Minute)
		q.Register("long", h)
		start(t, q)
	}
	q := e.queue(1)
	enqueue(t, q, workq.Task{Name: "long"})
	wait(t, q)
	if runs.Load() != 1 {
		t.Fatalf("ran %d times", runs.Load())
	}
}

func TestDeadNodeIsTakenOver(t *testing.T) { each(t, testDeadNodeIsTakenOver, testDeadNodeIsTakenOver) }

func testDeadNodeIsTakenOver[T any](t *testing.T, e env[T]) {
	a, closeA := e.node(t, time.Minute)
	started, release := make(chan struct{}), make(chan struct{})
	a.Register("job", func(ctx context.Context, p string) error {
		close(started)
		<-release // ignores ctx, like a hung process
		return nil
	})
	start(t, a)
	t.Cleanup(func() { close(release) }) // runs before a's Stop
	enqueue(t, a, workq.Task{Name: "job"})
	<-started
	closeA() // node a dies: no more heartbeats, no finishing write

	var bRuns atomic.Int32
	b := e.queue(2)
	b.LockTimeout = time.Minute
	b.Register("job", func(ctx context.Context, p string) error { bRuns.Add(1); return nil })
	start(t, b)
	wait(t, b)
	if bRuns.Load() != 1 {
		t.Fatalf("b ran %d times", bRuns.Load())
	}
}

func TestSlowNodeCannotOverwrite(t *testing.T) {
	each(t, testSlowNodeCannotOverwrite, testSlowNodeCannotOverwrite)
}

func testSlowNodeCannotOverwrite[T any](t *testing.T, e env[T]) {
	// a never heartbeats in time (LockTimeout of an hour), b considers its lock stale after a minute
	a, _ := e.node(t, time.Hour)
	var aCompleted atomic.Int32
	a.OnEvent = func(ev workq.Event) {
		if ev.Type == workq.Completed {
			aCompleted.Add(1)
		}
	}
	started, release := make(chan struct{}), make(chan struct{})
	a.Register("job", func(ctx context.Context, p string) error {
		close(started)
		<-release
		return nil
	})
	start(t, a)
	enqueue(t, a, workq.Task{Name: "job"})
	<-started

	b := e.queue(2)
	b.LockTimeout = time.Minute
	b.Register("job", func(ctx context.Context, p string) error { return errors.New("b failed") })
	start(t, b)
	for len(failed(t, b)) == 0 {
		time.Sleep(time.Second)
	}
	close(release)
	if err := a.Stop(t.Context()); err != nil { // waits for a's handler and finishing write
		t.Fatal(err)
	}
	if f := failed(t, b); len(f) != 1 || f[0].Error != "b failed" || aCompleted.Load() != 0 {
		t.Fatalf("a overwrote b's result: failed=%+v completed=%d", f, aCompleted.Load())
	}
}

func TestRegistrationErrors(t *testing.T) {
	nop := func(context.Context, string) error { return nil }
	q := workq.NewSQLite(nil, "", 1, nil)
	if err := q.Register("x", nop); err != nil {
		t.Fatal(err)
	}
	if err := q.Register("x", nop); err == nil {
		t.Fatal("duplicate Register succeeded")
	}
	if err := q.Schedule("missing", workq.Every(time.Hour)); err == nil {
		t.Fatal("Schedule before Register succeeded")
	}
	if err := q.Schedule("x", nil); err == nil {
		t.Fatal("nil Schedule succeeded")
	}
	if err := q.Schedule("x", workq.Every(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := q.Schedule("x", workq.Every(time.Hour)); err == nil {
		t.Fatal("duplicate Schedule succeeded")
	}
}

func TestWorkersCap(t *testing.T) { each(t, testWorkersCap, testWorkersCap) }

func testWorkersCap[T any](t *testing.T, e env[T]) {
	var mu sync.Mutex
	active, peak := 0, 0
	q := e.queue(3)
	q.Register("job", func(ctx context.Context, p string) error {
		mu.Lock()
		active++
		peak = max(peak, active)
		mu.Unlock()
		time.Sleep(time.Minute)
		mu.Lock()
		active--
		mu.Unlock()
		return nil
	})
	for range 10 {
		enqueue(t, q, workq.Task{Name: "job"})
	}
	start(t, q)
	wait(t, q)
	if peak != 3 { // 3 proves both parallelism and the cap
		t.Fatalf("peak concurrency %d, want 3", peak)
	}
}

// faultDriver is a database/sql Driver for SQLite that calls hook before each statement;
// a non-nil error fails the statement. hook may block to delay it.
type faultDriver struct {
	db   *sql.DB
	hook func(query string) error
}

type faultConn struct {
	c interface {
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	}
	hook func(query string) error
}

func (d faultDriver) DB() workq.Conn                               { return faultConn{d.db, d.hook} }
func (d faultDriver) Tx(tx *sql.Tx) workq.Conn                     { return faultConn{tx, d.hook} }
func (d faultDriver) SQLite() bool                                 { return true }
func (d faultDriver) Begin(ctx context.Context) (*sql.Tx, error)   { return d.db.BeginTx(ctx, nil) }
func (d faultDriver) Commit(_ context.Context, tx *sql.Tx) error   { return tx.Commit() }
func (d faultDriver) Rollback(_ context.Context, tx *sql.Tx) error { return tx.Rollback() }

func (c faultConn) Exec(ctx context.Context, query string, args ...any) (int64, error) {
	if err := c.hook(query); err != nil {
		return 0, err
	}
	res, err := c.c.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (c faultConn) Query(ctx context.Context, query string, args []any, row func(scan func(dest ...any) error) error) error {
	if err := c.hook(query); err != nil {
		return err
	}
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

// faultQueue returns a SQLite executor whose statements pass through hook.
func faultQueue(e env[*sql.Tx], workers int, hook func(query string) error) *workq.Queue[*sql.Tx] {
	q := workq.New[*sql.Tx](faultDriver{e.db, hook}, "", workers, slog.Default())
	q.PollInterval = time.Second
	return q
}

const (
	claimQuery     = "SET locked_at = ?1, version"
	heartbeatQuery = "SET locked_at = ?1 WHERE"
)

// The tests below run on SQLite only; channels must be made inside the synctest bubble.

func TestStopTimeoutWithStuckClaim(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stuck := make(chan struct{})
		defer close(stuck)
		var claims atomic.Int32
		q := faultQueue(setupSQLite(t), 1, func(query string) error {
			if strings.Contains(query, claimQuery) && claims.Add(1) > 1 {
				<-stuck
			}
			return nil
		})
		q.Register("x", func(ctx context.Context, p string) error { return nil })
		if err := q.Start(t.Context()); err != nil {
			t.Fatal(err)
		}
		for claims.Load() < 2 {
			time.Sleep(time.Second)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		if err := q.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	})
}

func TestReclaimedOwnTaskKeepsWorkerCap(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := faultQueue(setupSQLite(t), 2, func(query string) error {
			if strings.Contains(query, heartbeatQuery) {
				return errors.New("heartbeat down")
			}
			return nil
		})
		q.LockTimeout = time.Minute
		var mu sync.Mutex
		active, peak := 0, 0
		release := make(chan struct{})
		q.Register("job", func(ctx context.Context, p string) error {
			mu.Lock()
			active++
			peak = max(peak, active)
			mu.Unlock()
			<-release
			mu.Lock()
			active--
			mu.Unlock()
			return nil
		})
		start(t, q)
		enqueue(t, q, workq.Task{Name: "job"})
		time.Sleep(2 * time.Minute) // the lock went stale and this executor reclaimed its own task
		enqueue(t, q, workq.Task{Name: "job"})
		time.Sleep(time.Minute)
		close(release)
		wait(t, q)
		if peak != 2 {
			t.Fatalf("peak concurrency %d, want 2", peak)
		}
	})
}

func TestLateHeartbeatKeepsScheduleUnlocked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := setupSQLite(t)
		finished, resume := make(chan struct{}), make(chan struct{})
		var first atomic.Bool
		q := faultQueue(e, 1, func(query string) error {
			if strings.Contains(query, heartbeatQuery) && first.CompareAndSwap(false, true) {
				<-resume // snapshot taken while the task ran; the update runs after it finished
			}
			return nil
		})
		q.LockTimeout = 3 * time.Minute // heartbeat every minute
		q.OnEvent = func(ev workq.Event) {
			if ev.Type == workq.Completed {
				close(finished)
			}
		}
		q.Register("tick", func(ctx context.Context, p string) error { time.Sleep(90 * time.Second); return nil })
		q.Schedule("tick", workq.Every(time.Hour))
		start(t, q)
		<-finished
		close(resume)
		time.Sleep(time.Second)
		if count(t, e, "locked_at IS NOT NULL") != 0 {
			t.Fatal("late heartbeat locked the rescheduled task")
		}
		wait(t, q)
	})
}

func TestStartFailsWhenScheduleCreationFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e := setupSQLite(t)
		var inserts atomic.Int32
		q := faultQueue(e, 1, func(query string) error {
			if strings.HasPrefix(query, "INSERT") && strings.Contains(query, "scheduled") && inserts.Add(1) == 2 {
				return errors.New("db down")
			}
			return nil
		})
		for _, name := range []string{"tick", "tock"} {
			if err := q.Register(name, func(context.Context, string) error { return nil }); err != nil {
				t.Fatal(err)
			}
			if err := q.Schedule(name, workq.Every(time.Minute)); err != nil {
				t.Fatal(err)
			}
		}
		if err := q.Start(t.Context()); err == nil {
			t.Fatal("Start succeeded")
		}
		if n := count(t, e, "TRUE"); n != 0 {
			t.Fatalf("%d schedule rows after failed Start", n)
		}
	})
}
