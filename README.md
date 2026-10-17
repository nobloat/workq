# workq

A small, persistent task queue for Go, backed by PostgreSQL or SQLite. A simple alternative to [River](https://riverqueue.com/), following the design of [failsafe-executor](https://github.com/oliverselinger/failsafe-executor).

- **One file, no dependencies.** Uses only `database/sql` from the standard library. No reflection. Native [pgx](https://github.com/jackc/pgx) support lives in the separate `pgxq` module.
- **One table.** All state lives in `workq`.
- **At-least-once execution.** A task is deleted only after it succeeds. Tasks held by a crashed node are picked up again once their lock times out.
- **Transactional.** Enqueue tasks in your own transaction. Run handlers inside a transaction that commits together with the task's removal.
- **Multi-node.** Nodes coordinate through the database: `FOR UPDATE SKIP LOCKED` on Postgres, `UPDATE … RETURNING` on SQLite, and a version column for optimistic locking.
- **Routing.** A node only picks up tasks whose names it has registered.
- **Queues.** Each executor runs one queue with its own worker pool.
- **Schedules.** Recurring tasks that reschedule themselves after each successful run.
- **Observable.** One hook receives every enqueue, start, completion, failure and retry.

## Install

```sh
go get github.com/nobloat/workq       # database/sql: Postgres or SQLite
go get github.com/nobloat/workq/pgxq  # native pgx (pgxpool, pgx.Tx)
```

Requires PostgreSQL, or SQLite 3.35 or later (for `RETURNING`).

## Quick start

```go
db, _ := sql.Open("pgx", dsn)
db.SetMaxOpenConns(8 + 2) // workers + poll loop + heartbeat
db.SetMaxIdleConns(8 + 2) // the default of 2 makes busy workers reconnect constantly

q := workq.NewPostgres(db, "", 8, slog.Default()) // "" = default queue
if err := q.CreateTable(ctx); err != nil {
    log.Fatal(err)
}

if err := q.Register("send-email", func(ctx context.Context, param string) error {
    return mailer.Send(ctx, param)
}); err != nil {
    log.Fatal(err)
}

if err := q.Start(ctx); err != nil {
    log.Fatal(err)
}
defer func() {
    sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    q.Stop(sctx)
}()

id, err := q.Enqueue(ctx, workq.Task{Name: "send-email", Param: `{"to":"a@b.c"}`})
```

### pgx

`workq.Queue[T]` is generic over the transaction type that `RegisterTx` handlers and the `…Tx` methods use. `NewPostgres` and `NewSQLite` return a `*workq.Queue[*sql.Tx]`. `pgxq.New` returns a `*workq.Queue[pgx.Tx]` on a `pgxpool.Pool`:

```go
pool, _ := pgxpool.New(ctx, dsn)
q := pgxq.New(pool, "", 8, slog.Default())

if err := q.RegisterTx("charge-order", func(ctx context.Context, tx pgx.Tx, param string) error {
    _, err := tx.Exec(ctx, `UPDATE orders SET paid = true WHERE id = $1`, param)
    return err
}); err != nil {
    log.Fatal(err)
}
```

Everything else works the same. Other clients can be plugged in by implementing `workq.Driver[T]`.

## Schema

`CreateTable` is idempotent. You can also copy the schema into your own migrations. The two databases differ only in the `id` column:

```sql
CREATE TABLE IF NOT EXISTS workq (
  id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- SQLite: INTEGER PRIMARY KEY AUTOINCREMENT
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
CREATE INDEX IF NOT EXISTS workq_due ON workq (queue, run_at, id) WHERE failed_at IS NULL;
CREATE INDEX IF NOT EXISTS workq_due_name ON workq (queue, name, run_at, id) WHERE failed_at IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS workq_scheduled ON workq (name) WHERE scheduled;
```

- **IDs** are assigned by the database. On SQLite, `AUTOINCREMENT` ensures the IDs of deleted tasks are never reused.
- **Times** are stored as Unix milliseconds, which avoids timestamp differences between drivers.

To upgrade an existing database, rename the table, drop its old indexes, then call `CreateTable` to create the renamed indexes:

```sql
ALTER TABLE workq_tasks RENAME TO workq;
DROP INDEX workq_tasks_due;
DROP INDEX workq_tasks_due_name;
DROP INDEX workq_tasks_scheduled;
```

## Registering tasks

A task is a name plus a handler that takes a single string parameter. Return `nil` for success. A returned error, or a recovered panic, marks the task as failed. Registration errors include duplicate names.

```go
if err := q.Register("send-email", func(ctx context.Context, param string) error {
    var m Mail
    if err := json.Unmarshal([]byte(param), &m); err != nil {
        return err
    }
    return mailer.Send(ctx, m)
}); err != nil {
    return err
}
```

### Transactional handlers

`RegisterTx` runs the handler inside a transaction. The task's removal (or, for a scheduled task, its rescheduling) commits in that same transaction. If this node lost the task's lock in the meantime, the transaction is rolled back, so the handler's database writes take effect exactly once.

```go
if err := q.RegisterTx("charge-order", func(ctx context.Context, tx *sql.Tx, param string) error {
    if _, err := tx.ExecContext(ctx, `UPDATE orders SET paid = true WHERE id = $1`, param); err != nil {
        return err
    }
    // Follow-up tasks commit together with the handler's writes
    _, err := q.EnqueueTx(ctx, tx, workq.Task{Name: "send-email", Param: param})
    return err
}); err != nil {
    return err
}
```

The transaction stays open for the whole run. Use `Register` for slow work such as HTTP calls, and keep `RegisterTx` handlers short.

## Enqueueing

`Enqueue` returns the task's ID. `EnqueueTx` does the same inside your transaction, so the task exists only if your transaction commits.

```go
tx, _ := db.BeginTx(ctx, nil)
createOrder(tx, order)
id, err := q.EnqueueTx(ctx, tx, workq.Task{
    Name:  "send-email",
    Param: `{"to":"a@b.c"}`,
    Queue: "mails",                   // optional; see Queues
    RunAt: time.Now().Add(time.Hour), // optional; defaults to now
})
tx.Commit()
```

## Queues

An executor runs only the tasks of the queue passed to `NewPostgres`/`NewSQLite`; `""` is the default queue. Give each queue its own executor to size its worker pool independently:

```go
q := workq.NewPostgres(db, "", 8, log)
mails := workq.NewPostgres(db, "mails", 1, log)
if err := mails.Register("send-email", send); err != nil {
    log.Fatal(err)
}

q.Enqueue(ctx, workq.Task{Name: "send-email", Queue: "mails"})
```

There is no ordering guarantee. A queue with one worker runs one task at a time per node, but several nodes still run it in parallel.

## Scheduled tasks

A schedule is a function that returns the next run time from the time of the last successful run:

```go
type Schedule func(lastSuccess time.Time) time.Time
```

Register the handler first, then attach a schedule to its name:

```go
if err := q.Register("cleanup", cleanup); err != nil {
    log.Fatal(err)
}
if err := q.Schedule("cleanup", workq.Every(time.Hour)); err != nil {
    log.Fatal(err)
}

if err := q.Register("report", report); err != nil {
    log.Fatal(err)
}
if err := q.Schedule("report", func(last time.Time) time.Time { // daily at 03:00
    next := time.Date(last.Year(), last.Month(), last.Day(), 3, 0, 0, 0, last.Location())
    if !next.After(last) {
        next = next.AddDate(0, 0, 1)
    }
    return next
}); err != nil {
    log.Fatal(err)
}
```

- A scheduled task is stored in the executor's queue, as one row per name, no matter how many nodes register it. A unique index enforces this. Its first run is `schedule(now)` at first registration. Restarts don't reset it. `Start` returns an error if it cannot create every initial schedule row.
- After a successful run, the task is rescheduled to `schedule(finishTime)`.
- A failed scheduled task stops until it is retried. After a retry it runs immediately, then returns to its schedule.
- A schedule change in code takes effect after the task's next run.

## Failures

Errors are stored in the database. Failed tasks stay there until they are retried or deleted.

```go
failed, err := q.Failed(ctx)
for _, t := range failed {
    log.Printf("%d %s failed at %s: %s", t.ID, t.Name, t.FailedAt, t.Error)
}

ok, err := q.Retry(ctx, id)  // clears the failure; the task runs again
ok, err = q.Delete(ctx, id)  // gives up on the task
// RetryTx and DeleteTx do the same inside your transaction
```

There is no automatic retry. To add one, enqueue a delayed copy from the handler or from the `OnEvent` hook.

## Events

Set `OnEvent` to observe everything workq does, for example for metrics, alerts or a history table:

```go
q.OnEvent = func(e workq.Event) {
    switch e.Type {
    case workq.Failed:
        alert(e.Task.Name, e.Err)
    case workq.Completed:
        db.Exec(`INSERT INTO task_history (id, name, took_ms) VALUES ($1, $2, $3)`,
            e.Task.ID, e.Task.Name, e.Duration.Milliseconds())
    }
}
```

| Event       | Fired                                                    |
|-------------|----------------------------------------------------------|
| `Enqueued`  | after the INSERT, on the node that enqueued              |
| `Started`   | when a worker begins running the task                    |
| `Completed` | after success is committed; `Duration` is set            |
| `Failed`    | after the failure is stored; `Err` and `Duration` are set |
| `Retried`   | after `Retry` succeeds                                   |

- The hook runs synchronously, so keep it fast.
- `Enqueued` and `Retried` inside a transaction fire before your commit, so the transaction can still be rolled back afterwards.
- To wrap one handler (tracing, timing), wrap the function you pass to `Register`.

## Start and stop

```go
if err := q.Start(ctx); err != nil {
    return err
}
err := q.Stop(sctx)
```

`Stop` stops claiming new tasks and waits for running ones to finish. When `sctx` expires, the handlers' contexts are cancelled and `Stop` returns, even if a claim is stuck in the database.

## Testing

`Wait` blocks until no due or running tasks remain. Failed tasks and tasks scheduled for later don't count.

```go
func TestSignup(t *testing.T) {
    q := workq.NewSQLite(db, "", 1, slog.Default())
    if err := q.Register("send-email", fakeSend); err != nil {
        t.Fatal(err)
    }
    if err := q.Start(ctx); err != nil {
        t.Fatal(err)
    }
    defer q.Stop(ctx)

    signup(ctx, q)

    if err := q.Wait(ctx, "send-email"); err != nil { // no names = wait for all tasks
        t.Fatal(err)
    }
    if f, _ := q.Failed(ctx); len(f) > 0 {
        t.Fatal(f[0].Error)
    }
}
```

## Configuration

```go
q := workq.NewPostgres(db, queue, workers, logger) // or NewSQLite
q.PollInterval = time.Second    // how often to look for due tasks (and how often Wait checks)
q.LockTimeout = 5 * time.Minute // a lock older than this is treated as a dead node
q.OnEvent = nil                 // event hook
```

Running tasks send a heartbeat every `LockTimeout / 3`.

## Database notes

- **Postgres:** each running `RegisterTx` handler holds a connection. Set `db.SetMaxOpenConns` to at least `workers + 2`, summed over all executors sharing the pool, and `db.SetMaxIdleConns` to the same value: with the default of 2, throughput collapses under load because connections are closed and reopened. With pgx, set `MaxConns` on the pool config.
- **SQLite:** use WAL with `synchronous=NORMAL` and set a `busy_timeout`, for example `?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_txlock=immediate` with modernc.org/sqlite. `synchronous=NORMAL` stops waiting for the disk on every commit: an app crash loses nothing, while a power loss can lose the last commits. That doubled enqueue throughput in the benchmark. `_txlock=immediate` makes transactions take the write lock when they begin, which avoids `SQLITE_BUSY` errors when a read turns into a write. SQLite has a single writer, so workq queues its own statements per `*sql.DB` in Go instead of letting them race for the write lock and back off in SQLite's busy handler. A `RegisterTx` handler that writes still blocks all other writers until it commits.

## Performance

[`tests/bench_test.go`](tests/bench_test.go) measures the raw overhead of workq with no-op tasks. `p` producers enqueue the tasks concurrently, then one executor with `p` workers runs them all:

```sh
cd tests && go test -run XXX -bench Throughput -benchtime 5000x -count 3 ./...
```

The results below predate the removal of batched completion deletes and have not been remeasured. They do not represent current throughput.

Medians of 3 runs with 5,000 tasks each, in tasks per second:

| p  | SQLite enqueue | SQLite exec | Postgres (`database/sql`) enqueue | Postgres (`database/sql`) exec | Postgres (`pgxq`) enqueue | Postgres (`pgxq`) exec |
|----|-------:|-------:|-------:|-------:|-------:|-------:|
| 1  | 34,363 | 9,084  | 2,903  | 1,216  | 3,038  | 1,171  |
| 4  | 33,296 | 13,118 | 7,672  | 3,648  | 8,096  | 3,816  |
| 16 | 34,922 | 35,424 | 15,686 | 13,836 | 15,499 | 13,875 |
| 64 | 33,677 | 70,276 | 26,885 | 30,978 | 26,068 | 24,735 |

Machine: MacBook Pro (Mac15,10), Apple M3 Max (10 performance + 4 efficiency cores), 36 GB RAM, macOS 26.7.1, Go 1.27.2. SQLite via modernc.org/sqlite v1.60.1, a file on the local SSD with the settings from [Database notes](#database-notes). PostgreSQL 17.11 (`postgres:17-alpine`, default config) in Docker Desktop 29.4.0, accessed with pgx v5.11.0.

- **More workers, more throughput.** One claim fills several free workers. These historical runs also batched completion deletes; the current implementation deletes each completed task directly.
- **Postgres pays for round trips.** At p=1 every task waits for a claim and a delete round trip. Docker Desktop adds network overhead to each of them, so expect higher numbers with Postgres on a Linux host.
- **`database/sql` and `pgxq` perform the same.** The driver isn't the bottleneck. Pick `pgxq` for `pgx.Tx` in handlers, not for speed. This only holds when `SetMaxIdleConns` matches `SetMaxOpenConns`; with the default of 2 idle connections, `database/sql` reached only about 2,000 tasks/s at p=64.
- **Enqueue is one INSERT per task.** It is bounded by round trips on Postgres and by the single writer on SQLite. There is no bulk insert.
- **Large backlogs don't slow it down.** In separate runs that worked through 1,000,000 tasks already inserted, the rate stayed level from the first task to the last: on Postgres about 30,000 tasks/s with 64 workers and about 66,000 with 2,000 workers, on SQLite about 70,000 with 64 workers. See [How it works](#how-it-works) for what keeps claims cheap. These runs had one task name; with routing, `workq_due_name` keeps a node from scanning the due tasks of names it doesn't register.
- **These are upper bounds.** Real tasks add their own run time, and with tasks that take milliseconds or more, the number of workers matters more than workq's overhead.

### Compared to River

[River](https://riverqueue.com/) ships its own benchmark, `river bench -n 1000000`. It inserts one million no-op jobs, then works them down with 2,000 workers on a default pgx pool. I ran it and the same scenario for workq on the machine and Postgres container above. Only the time to work down the jobs counts; inserting them doesn't:

| | 1,000,000 tasks | tasks/s |
|---|---:|---:|
| River v0.49.0 (`river bench`) | 19.3 s | 51,839 |
| workq, `pgxq`, 2,000 workers, 14 connections | 15.2 s | 65,712 |
| workq, `database/sql`, 2,000 workers, 14 connections | 15.1 s | 66,009 |

These results also predate the removal of batched completion deletes. Each number is a single run. workq's run inserted the tasks with one SQL `INSERT … SELECT generate_series(…)` and counted completions through `OnEvent`; that throwaway test isn't in the repo. Results at p=64 vary by about ±15% between runs.

The comparison only measures overhead per job. River does more for each job: it keeps completed jobs for later inspection instead of deleting them, and it tracks attempts, retries with backoff, priorities and unique jobs. It also has typed job arguments, a web UI and LISTEN/NOTIFY wake-ups. workq has none of these. It offers one file without dependencies, `database/sql` support, and the same API for SQLite and Postgres.

## How it works

1. **Claiming:** every `PollInterval`, an executor claims up to `free workers` due tasks with one statement:
   - **Postgres:** `UPDATE … WHERE id = ANY(ARRAY(SELECT … ORDER BY run_at, id LIMIT n FOR UPDATE SKIP LOCKED)) RETURNING …`.
   - **SQLite:** `UPDATE … WHERE id IN (SELECT … ORDER BY run_at, id LIMIT n) RETURNING …`. It is atomic because SQLite has only one writer.

   Postgres keeps index entries of deleted tasks until vacuum runs, and they sit at the front of the due-task index, exactly where each claim starts. With a backlog of a million tasks, claims slowed down by more than half until two changes:
   - **Cursor:** each claim continues after the `(run_at, id)` where the previous one stopped. The `PollInterval` tick and every claim that returns fewer tasks than requested start from the beginning again, which picks up retried tasks, stale locks and tasks with an earlier `RunAt`.
   - **`= ANY(ARRAY(…))` instead of `IN (…)`:** `IN` makes Postgres plan a join and, to estimate it, read the lowest ids from the primary key, which is where the dead entries are. That took 1–2 ms per claim. With `ANY(ARRAY(…))` the subquery runs once before the update and planning takes 0.04 ms.

   Claiming sets `locked_at` and increments `version`.
2. **Eligibility:** a task can be claimed when all of these hold:
   - it belongs to this executor's queue
   - its name is registered on this executor
   - it is due
   - it hasn't failed
   - it is unlocked, or its lock is stale
3. **Heartbeat:** a heartbeat refreshes `locked_at` while the task runs.
4. **Finishing:** every finishing write checks `WHERE id = ? AND version = ?`, so a node whose lock was taken over cannot overwrite the result. Each worker directly deletes its successful task, or updates its next run time if scheduled. `RegisterTx` tasks finish inside their own transaction.
