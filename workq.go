// Package workq is a persistent task queue backed by PostgreSQL or SQLite,
// using only database/sql. See README.md.
package workq

import (
	"context"
	"database/sql"
	"log/slog"
	"time"
)

type Dialect int

const (
	Postgres Dialect = iota
	SQLite
)

// DB is satisfied by *sql.DB and *sql.Tx.
type DB interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
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

type handler struct {
	fn   func(ctx context.Context, param string) error
	txFn func(ctx context.Context, tx *sql.Tx, param string) error
}

type Queue struct {
	PollInterval time.Duration
	LockTimeout  time.Duration
	OnEvent      func(Event)

	db        *sql.DB
	dialect   Dialect
	workers   int
	log       *slog.Logger
	handlers  map[string]handler
	schedules map[string]Schedule
}

// id orders tasks within a queue. SQLite needs AUTOINCREMENT so ids of deleted tasks are never reused.
var schema = map[Dialect]string{
	Postgres: `CREATE TABLE IF NOT EXISTS workq_tasks (
  id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,` + columns,
	SQLite: `CREATE TABLE IF NOT EXISTS workq_tasks (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,` + columns,
}

const columns = `
  name       TEXT NOT NULL,
  param      TEXT NOT NULL,
  queue      TEXT,
  scheduled  BOOLEAN NOT NULL DEFAULT FALSE,
  run_at     BIGINT NOT NULL,
  locked_at  BIGINT,
  version    BIGINT NOT NULL DEFAULT 0,
  error      TEXT,
  failed_at  BIGINT,
  created_at BIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS workq_tasks_due ON workq_tasks (run_at) WHERE failed_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS workq_tasks_scheduled ON workq_tasks (name) WHERE scheduled;
`

func New(db *sql.DB, d Dialect, workers int, log *slog.Logger) *Queue {
	return &Queue{
		PollInterval: time.Second,
		LockTimeout:  5 * time.Minute,
		db:           db,
		dialect:      d,
		workers:      workers,
		log:          log,
		handlers:     map[string]handler{},
		schedules:    map[string]Schedule{},
	}
}

func (q *Queue) CreateTable(ctx context.Context) error {
	panic("TODO")
}

func (q *Queue) Register(name string, fn func(ctx context.Context, param string) error) {
	panic("TODO")
}

func (q *Queue) RegisterTx(name string, fn func(ctx context.Context, tx *sql.Tx, param string) error) {
	panic("TODO")
}

// Schedule attaches s to the already registered task name.
func (q *Queue) Schedule(name string, s Schedule) {
	panic("TODO")
}

func (q *Queue) Start(ctx context.Context) {
	panic("TODO")
}

// Stop stops claiming tasks and waits for running ones; handler contexts are cancelled when ctx expires.
func (q *Queue) Stop(ctx context.Context) error {
	panic("TODO")
}

// Enqueue inserts t and returns its id; t.ID is ignored.
func (q *Queue) Enqueue(ctx context.Context, db DB, t Task) (int64, error) {
	panic("TODO")
}

func (q *Queue) Failed(ctx context.Context) ([]Task, error) {
	panic("TODO")
}

func (q *Queue) Retry(ctx context.Context, db DB, id int64) (bool, error) {
	panic("TODO")
}

func (q *Queue) Delete(ctx context.Context, db DB, id int64) (bool, error) {
	panic("TODO")
}

// Wait blocks until no pending or running tasks with the given names remain (all tasks if none given).
func (q *Queue) Wait(ctx context.Context, names ...string) error {
	panic("TODO")
}
