# workq

A small, persistent task queue for Go, backed by PostgreSQL or SQLite. A simple alternative to [River](https://riverqueue.com/), following the design of [failsafe-executor](https://github.com/oliverselinger/failsafe-executor).

- **One file, no dependencies.** Uses only `database/sql` from the standard library. No reflection.
- **One table.** All state lives in `workq_tasks`.
- **At-least-once execution.** A task is deleted only after it succeeds. Tasks held by a crashed node are picked up again once their lock times out.
- **Transactional.** Enqueue tasks in your own transaction. Run handlers inside a transaction that commits together with the task's removal.
- **Multi-node.** Nodes coordinate through the database: `FOR UPDATE SKIP LOCKED` on Postgres, `UPDATE … RETURNING` on SQLite, and a version column for optimistic locking.
- **Routing.** A node only picks up tasks whose names it has registered.
- **Queues.** Tasks with the same queue name run one at a time, in the order they were enqueued.
- **Schedules.** Recurring tasks that reschedule themselves after each successful run.
- **Observable.** One hook receives every enqueue, start, completion, failure and retry.

## Install

```sh
go get github.com/nobloat/workq
```

Requires PostgreSQL, or SQLite 3.35 or later (for `RETURNING`).

## Quick start

```go
db, _ := sql.Open("pgx", dsn)
db.SetMaxOpenConns(8 + 2) // workers + poll loop + heartbeat

q := workq.New(db, workq.Postgres, 8, slog.Default())
if err := q.CreateTable(ctx); err != nil {
    log.Fatal(err)
}

q.Register("send-email", func(ctx context.Context, param string) error {
    return mailer.Send(ctx, param)
})

q.Start(ctx)
defer func() {
    sctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    q.Stop(sctx)
}()

id, err := q.Enqueue(ctx, db, workq.Task{Name: "send-email", Param: `{"to":"a@b.c"}`})
```

## Schema

`CreateTable` is idempotent. You can also copy the schema into your own migrations. The two databases differ only in the `id` column:

```sql
CREATE TABLE IF NOT EXISTS workq_tasks (
  id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- SQLite: INTEGER PRIMARY KEY AUTOINCREMENT
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
```

- **IDs** are assigned by the database and only ever increase, so they define the order of tasks within a queue. On SQLite, `AUTOINCREMENT` ensures the IDs of deleted tasks are never reused.
- **Times** are stored as Unix milliseconds, which avoids timestamp differences between drivers.

## Registering tasks

A task is a name plus a handler that takes a single string parameter. Return `nil` for success. A returned error, or a recovered panic, marks the task as failed.

```go
q.Register("send-email", func(ctx context.Context, param string) error {
    var m Mail
    if err := json.Unmarshal([]byte(param), &m); err != nil {
        return err
    }
    return mailer.Send(ctx, m)
})
```

### Transactional handlers

`RegisterTx` runs the handler inside a transaction. The task's removal (or, for a scheduled task, its rescheduling) commits in that same transaction. If this node lost the task's lock in the meantime, the transaction is rolled back, so the handler's database writes take effect exactly once.

```go
q.RegisterTx("charge-order", func(ctx context.Context, tx *sql.Tx, param string) error {
    if _, err := tx.ExecContext(ctx, `UPDATE orders SET paid = true WHERE id = $1`, param); err != nil {
        return err
    }
    // Follow-up tasks commit together with the handler's writes
    _, err := q.Enqueue(ctx, tx, workq.Task{Name: "send-email", Param: param})
    return err
})
```

The transaction stays open for the whole run. Use `Register` for slow work such as HTTP calls, and keep `RegisterTx` handlers short.

## Enqueueing

`Enqueue` accepts a `*sql.DB` or a `*sql.Tx` and returns the task's ID. Inside a transaction, the task exists only if your transaction commits.

```go
tx, _ := db.BeginTx(ctx, nil)
createOrder(tx, order)
id, err := q.Enqueue(ctx, tx, workq.Task{
    Name:  "send-email",
    Param: `{"to":"a@b.c"}`,
    Queue: "user-42",                 // optional; see Queues
    RunAt: time.Now().Add(time.Hour), // optional; defaults to now
})
tx.Commit()
```

## Queues

Tasks that share a `Queue` value run one at a time, across all nodes, in ID order. Tasks enqueued one after another run in that order. Two transactions enqueueing to the same queue at the same moment can commit out of ID order. Tasks without a queue run in parallel. A failed task does not block its queue; the next task in that queue runs.

## Scheduled tasks

A schedule is a function that returns the next run time from the time of the last successful run:

```go
type Schedule func(lastSuccess time.Time) time.Time
```

Register the handler first, then attach a schedule to its name:

```go
q.Register("cleanup", cleanup)
q.Schedule("cleanup", workq.Every(time.Hour))

q.Register("report", report)
q.Schedule("report", func(last time.Time) time.Time { // daily at 03:00
    next := time.Date(last.Year(), last.Month(), last.Day(), 3, 0, 0, 0, last.Location())
    if !next.After(last) {
        next = next.AddDate(0, 0, 1)
    }
    return next
})
```

- A scheduled task is stored as one row per name, no matter how many nodes register it. A unique index enforces this. Its first run is `schedule(now)` at first registration. Restarts don't reset it.
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

ok, err := q.Retry(ctx, db, id)  // clears the failure; the task runs again
ok, err = q.Delete(ctx, db, id)  // gives up on the task
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
q.Start(ctx)
err := q.Stop(sctx)
```

`Stop` stops claiming new tasks and waits for running ones to finish. When `sctx` expires, the handlers' contexts are cancelled.

## Testing

`Wait` blocks until no pending or running tasks remain. Failed tasks don't count as pending.

```go
func TestSignup(t *testing.T) {
    q := workq.New(db, workq.SQLite, 1, slog.Default())
    q.Register("send-email", fakeSend)
    q.Start(ctx)
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
q := workq.New(db, dialect, workers, logger)
q.PollInterval = time.Second    // how often to look for due tasks
q.LockTimeout = 5 * time.Minute // a lock older than this is treated as a dead node
q.OnEvent = nil                 // event hook
```

Running tasks send a heartbeat every `LockTimeout / 3`.

## Database notes

- **Postgres:** each running `RegisterTx` handler holds a connection. Set `db.SetMaxOpenConns` to at least `workers + 2`.
- **SQLite:** set a `busy_timeout` (for example `_pragma=busy_timeout(5000)` with modernc.org/sqlite). SQLite has a single writer, so a `RegisterTx` handler that writes blocks other writers until it commits.

## How it works

1. **Claiming:** every `PollInterval`, a node claims up to `free workers` due tasks with one statement:
   - **Postgres:** `UPDATE … WHERE id IN (SELECT … FOR UPDATE SKIP LOCKED LIMIT n) RETURNING …`.
   - **SQLite:** the same statement without `SKIP LOCKED`. It is atomic because SQLite has only one writer.

   Claiming sets `locked_at` and increments `version`.
2. **Eligibility:** a task can be claimed when all of these hold:
   - its name is registered on this node
   - it is due
   - it hasn't failed
   - it is unlocked, or its lock is stale
   - no older task in its queue is running
3. **Heartbeat:** a heartbeat refreshes `locked_at` while the task runs.
4. **Finishing:** every finishing write checks `WHERE id = ? AND version = ?`, so a node whose lock was taken over cannot overwrite the result.
