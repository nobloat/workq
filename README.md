# workq

A small, persistent task queue for Go, backed by PostgreSQL or SQLite. A simple alternative to [River](https://riverqueue.com/), following the design of [failsafe-executor](https://github.com/oliverselinger/failsafe-executor).

- **One file, no dependencies.** Uses only `database/sql` from the standard library. No reflection. Native [pgx](https://github.com/jackc/pgx) support lives in the separate `pgxq` module.
- **One table.** All state lives in `workq`.
- **At-least-once execution.** A task is deleted only after it succeeds. Tasks held by a crashed node are picked up again once their lock times out.
- **Transactional enqueue.** Enqueue, retry, or delete tasks in your own transaction.
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

Requires PostgreSQL, or SQLite 3.35 or later (for `RETURNING`). See the runnable [database/sql](examples/sql/), [pgx](examples/pgx/), [multiple-queue](examples/queues/), and [testing](examples/testing/) examples for complete programs.

## Quick start

```go
db, err := sql.Open("pgx", dsn)
if err != nil {
    log.Fatal(err)
}
defer db.Close()

q := workq.NewPostgres(db, "", 1, slog.Default())
if err := q.CreateTable(ctx); err != nil {
    log.Fatal(err)
}
if err := q.Register("hello", func(context.Context, string) error {
    return nil
}); err != nil {
    log.Fatal(err)
}
if err := q.Start(ctx); err != nil {
    log.Fatal(err)
}
defer q.Stop(context.Background())

_, err = q.Enqueue(ctx, workq.Task{Name: "hello"})
```

## Schema

`CreateTable` is idempotent. For application-managed migrations, use [schema_postgres.sql](schema_postgres.sql) or [schema_sqlite.sql](schema_sqlite.sql).

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

## Enqueueing

`Enqueue` returns the task's ID. `EnqueueTx` does the same inside your transaction, so the task exists only if your transaction commits.

```go
tx, _ := db.BeginTx(ctx, nil)
createOrder(tx, order)
id, err := q.EnqueueTx(ctx, tx, workq.Task{
    Name:  "send-email",
    Param: `{"to":"a@b.c"}`,
    Queue: "mails",                   // optional; see Multiple queues
    RunAt: time.Now().Add(time.Hour), // optional; defaults to now
})
tx.Commit()
```

## Multiple queues

An executor runs only the tasks of the queue passed to `NewPostgres`/`NewSQLite`; `""` is the default queue. Give each queue its own executor to size its worker pool independently:

```go
orders := workq.NewPostgres(db, "", 8, log)       // default queue
mails := workq.NewPostgres(db, "mails", 1, log)   // one mail worker
if err := orders.CreateTable(ctx); err != nil {     // once per database
    log.Fatal(err)
}

if err := orders.Register("charge-order", chargeOrder); err != nil {
    log.Fatal(err)
}
if err := mails.Register("send-email", sendEmail); err != nil {
    log.Fatal(err)
}
if err := orders.Start(ctx); err != nil {
    log.Fatal(err)
}
if err := mails.Start(ctx); err != nil {
    log.Fatal(err)
}

if _, err := orders.Enqueue(ctx, workq.Task{Name: "charge-order", Param: orderID}); err != nil {
    log.Fatal(err)
}
if _, err := orders.Enqueue(ctx, workq.Task{Name: "send-email", Param: orderID, Queue: "mails"}); err != nil {
    log.Fatal(err)
}
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

## Testing and waiting for tasks

Use a real test database, start the queue, then call `Wait` after the code under test enqueues work. `Wait(ctx)` waits for every due task; `Wait(ctx, "send-email")` waits only for the named tasks. Failed tasks and tasks scheduled for later don't count.

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

- **Postgres:** set `db.SetMaxOpenConns` to at least `workers + 2`, summed over all executors sharing the pool, and `db.SetMaxIdleConns` to the same value: with the default of 2, throughput collapses under load because connections are closed and reopened. With pgx, set `MaxConns` on the pool config.
- **SQLite:** use WAL with `synchronous=NORMAL` and set a `busy_timeout`, for example `?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_txlock=immediate` with modernc.org/sqlite. `synchronous=NORMAL` stops waiting for the disk on every commit: an app crash loses nothing, while a power loss can lose the last commits. That doubled enqueue throughput in the benchmark. `_txlock=immediate` makes transactions take the write lock when they begin, which avoids `SQLITE_BUSY` errors when a read turns into a write. SQLite has a single writer, so workq queues its own statements per `*sql.DB` in Go instead of letting them race for the write lock and back off in SQLite's busy handler.

## Performance

[`tests/bench_test.go`](tests/bench_test.go) measures the raw overhead of workq with no-op tasks. `p` producers enqueue the tasks concurrently, then one executor with `p` workers runs them all:

```sh
cd tests && go test -run XXX -bench Throughput -benchtime 5000x -count 3 ./...
```

Medians of 3 runs with 5,000 tasks each, in tasks per second:

| p  | SQLite enqueue | SQLite exec | Postgres (`database/sql`) enqueue | Postgres (`database/sql`) exec | Postgres (`pgxq`) enqueue | Postgres (`pgxq`) exec |
|----|-------:|-------:|-------:|-------:|-------:|-------:|
| 1  | 30,877 | 8,188 | 2,852 | 1,330 | 2,876 | 1,326 |
| 4  | 28,729 | 11,896 | 8,079 | 3,874 | 8,086 | 3,845 |
| 16 | 27,323 | 19,911 | 15,415 | 8,459 | 15,830 | 8,580 |
| 64 | 27,268 | 26,131 | 25,726 | 16,108 | 25,534 | 16,598 |

Machine: MacBook Pro (Mac15,10), Apple M3 Max (10 performance + 4 efficiency cores), 36 GB RAM, macOS 26.7.1, Go 1.27.2. SQLite via modernc.org/sqlite v1.60.1, a file on the local SSD with the settings from [Database notes](#database-notes). PostgreSQL 17.11 (`postgres:17-alpine`, default config) in Docker Desktop 29.4.0, accessed with pgx v5.11.0.

- **Wake-ups start work promptly.** Enqueuing, retrying, and completing a task signal the poller; one claim fills all free worker slots. The current implementation deletes each completed task directly.
- **Postgres pays for round trips.** Docker Desktop adds network overhead, so expect higher numbers with Postgres on a Linux host.
- **`database/sql` and `pgxq` perform the same.** The driver isn't the bottleneck. Pick the client your application already uses. This only holds when `SetMaxIdleConns` matches `SetMaxOpenConns`; with the default of 2 idle connections, `database/sql` reached only about 2,000 tasks/s at p=64.
- **Enqueue is one INSERT per task.** It is bounded by round trips on Postgres and by the single writer on SQLite. There is no bulk insert.
- **These are upper bounds.** Real tasks add their own run time, and with tasks that take milliseconds or more, the number of workers matters more than workq's overhead.

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
4. **Finishing:** every finishing write checks `WHERE id = ? AND version = ?`, so a node whose lock was taken over cannot overwrite the result. Each worker directly deletes its successful task, or updates its next run time if scheduled.
