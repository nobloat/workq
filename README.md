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

Requires PostgreSQL, or SQLite 3.35 or later (for `RETURNING`).

## Examples

- [`examples/sql/main.go`](examples/sql/main.go): a PostgreSQL queue using `database/sql`.
- [`examples/pgx/main.go`](examples/pgx/main.go): a PostgreSQL queue using native pgx.
- [`examples/queues/main.go`](examples/queues/main.go): separate worker pools for order and mail queues.
- [`examples/testing/workq_test.go`](examples/testing/workq_test.go): testing application code that enqueues work with SQLite.

## Multiple queues

An executor runs only the tasks of the queue passed to `NewPostgres`/`NewSQLite`; `""` is the default queue. Give each queue its own executor to size its worker pool independently. See [`examples/queues/main.go`](examples/queues/main.go).

There is no ordering guarantee. A queue with one worker runs one task at a time per node, but several nodes still run it in parallel.

## Scheduled tasks

A `Schedule` function returns the next run time from the time of the last successful run. Register the handler before attaching its schedule with `Schedule`.

- A scheduled task is stored in the executor's queue, as one row per name, no matter how many nodes register it. A unique index enforces this. Its first run is `schedule(now)` at first registration. Restarts don't reset it. `Start` returns an error if it cannot create every initial schedule row.
- After a successful run, the task is rescheduled to `schedule(finishTime)`.
- A failed scheduled task stops until it is retried. After a retry it runs immediately, then returns to its schedule.
- A schedule change in code takes effect after the task's next run.

## Failures

Errors are stored in the database. Failed tasks stay there until `Retry` (or `RetryTx`) clears the failure, or `Delete` (or `DeleteTx`) removes the task.

There is no automatic retry. To add one, enqueue a delayed copy from the handler or from the `OnEvent` hook.

## Events

Set `OnEvent` to observe everything workq does, for example for metrics, alerts, or a history table.

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

`Start` begins polling. `Stop` stops claiming new tasks and waits for running ones to finish. When its context expires, handlers' contexts are cancelled and `Stop` returns, even if a claim is stuck in the database.

## Testing and waiting for tasks

Use a real test database, start the queue, then call `Wait` after the code under test enqueues work. `Wait(ctx)` waits for every due task; `Wait(ctx, "send-email")` waits only for the named tasks. Failed tasks and tasks scheduled for later don't count. See [`examples/testing/workq_test.go`](examples/testing/workq_test.go).

## Configuration

`PollInterval` controls how often the queue looks for due tasks (and `Wait` checks). `LockTimeout` makes an older lock eligible for takeover. `OnEvent` receives queue events. Running tasks send a heartbeat every `LockTimeout / 3`.

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


## Implementation details

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
