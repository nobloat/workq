workq - a simple alternative to [riverqueue](https://riverqueue.com/). following the designe of [failsafe-executor](https://github.com/oliverselinger/failsafe-executor)


## Requirements

- implemented in go
- just using database/sql interface of go stdlib
- support postgresql + sqlite
- using optimized queries for sqlite and postgresql (e.g. select for update etc.)
- self-contained in a single go file
- tasks are registered by name and a lambda function.
- tasks take a single string parameter and return an error (nil if everything went fine)
- list of (failed) tasks can be fetched
- error is stored in database
- test utilities like waitForTasks() - by name or for all to complete
- creating a failsafe executor takes
  - worker count (how many in parallel)
  - logger
  - database connection
- start / stop (drain)
- support for cronjob’s (same thing but auto reschedule themselve)

## Features taken from failsafe-executor

*   **Failsafe** tasks. Requires only one database-table for persistence.
*   **Reliable** execution. Guarantees at least once execution of a submitted task.
*   **Multi-node compatible**. Coordination between nodes with optimistic locking.
*   **Retry-able**. Exceptions are captured. Failed tasks can be retried.
*   **Routing** tasks amongst different nodes. An executor only picks up registered tasks.
*   **Queues** ensure tasks are executed sequentially when applied.
*   **Lightweight**. Small code base.
*   **No dependencies**.
*   **No reflection**.
*   **Single file**.


Go make a plan and suggest me some code snippets how the usage side would look like (creating tasks, registering, start/stop, handling errors etc.)
