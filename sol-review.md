# workq review

What this change does: workq stores tasks in one database table and runs named handlers across workers and nodes. It supports transactional handlers, recurring schedules, failure inspection, and manual retries on SQLite and PostgreSQL.

Reviewed the full working-tree implementation, including the existing uncommitted changes, against `README.md`, `TODO.md`, and `PROMPT.md`. `PROMPT.md` explicitly makes the README authoritative: the README's per-node queue concurrency rule supersedes the older TODO's broader sequential-queue wording.

Assumed load: multiple executors, routed task names, long-running handlers, and backlogs up to the README's million-task example. Temporary database failures and stale-lock recovery are part of the promised operating model.

## Verification

- `go vet ./...` at the root: passed.
- `cd pgxq && go vet ./...`: passed.
- `cd tests && go test -race ./...`: passed, including SQLite, PostgreSQL through `database/sql`, and native pgx. Docker was available; PostgreSQL tests were not skipped.
- Additional race-enabled reproduction tests ran outside the repository in `/tmp/workq-sol-review`, using the current implementation and a copy of the existing test helpers. They confirmed the cases below. Fault injection is identified explicitly; it was not substituted for evidence of ordinary database behavior.
- No implementation or existing test files were changed by this review.

## Must fix

### 1. Stop can ignore its deadline indefinitely

**Location:** `workq.go:223–237`, `workq.go:349–365`.

- **What this is:** `Stop` should stop claims, drain running work, and cancel handlers when its context expires.
- **Problem:** It waits unconditionally for `loopDone` before checking that context. The poller runs its database claim with `context.WithoutCancel`, so a blocked claim can prevent both shutdown and handler cancellation indefinitely.
- **Fix:** Observe `ctx.Done()` while waiting for the poll loop, cancel the run context on expiry, and return the context error. Bound database operations too; do not solve this by waiting for workers while the poller can still add them.
- **If we skip it:** The documented 30-second shutdown can hang until an external lock or database problem clears, leaving handlers uncancelled past the deadline.

**Verified:** With the unmodified `NewPostgres` driver, held `LOCK TABLE workq IN ACCESS EXCLUSIVE MODE` in a separate transaction, started a queue with a registered handler, and checked `pg_stat_activity` to confirm its claim was waiting on a lock. Called `Stop` with a 30 ms timeout: it was still blocked 100 ms after the deadline and returned only after the transaction released the table lock. A separate blocked-driver test confirmed the same control-flow defect.

### 2. Reclaiming a local stale task breaks worker accounting

**Location:** `workq.go:89–90`, `workq.go:382–385`, `workq.go:420–424`, `workq.go:432–437`.

- **What this is:** The `running` map supplies the count of occupied workers and the task versions to heartbeat.
- **Problem:** It has one entry per task ID, not per execution. If a heartbeat fails and this executor reclaims its own stale task, the new execution overwrites the old entry; both handlers run, but only one worker is counted. The old execution's unconditional cleanup can then remove the new execution's heartbeat entry.
- **Fix:** Track running executions by `(id, version)`, count those entries for worker capacity, and remove only the finishing execution's pair. The heartbeat already works with ID/version pairs.
- **If we skip it:** A database outage can make a queue exceed its configured concurrency and lose heartbeats for replacement executions, causing further duplicates and load precisely during recovery.

**Verified:** On SQLite, configured two workers, `LockTimeout = 90 ms`, and `PollInterval = 5 ms`. Injected failures only into heartbeat updates while claims and task completion remained functional. Held the first handler open, waited for the same row to be reclaimed, then enqueued another task: **three handlers ran concurrently with a limit of two**. All were released and drained afterwards. This is not a complaint about allowed at-least-once execution; it is a separate violation of the worker limit and execution bookkeeping.

### 3. A late heartbeat can lock a successfully rescheduled task again

**Location:** `workq.go:496–502`, `workq.go:557–575`; also affects `Wait` at `workq.go:319–346`.

- **What this is:** A successful recurring task moves its next run into the future and clears `locked_at`. Heartbeats refresh locks for executions that were running when the heartbeat took its snapshot.
- **Problem:** Rescheduling does not change `version`, and heartbeat updates check only ID/version. A heartbeat queued before completion can therefore run afterwards and restore `locked_at` on an idle, future task; subsequent heartbeats do not clear it because the execution has left `running`.
- **Fix:** Heartbeat only rows whose `locked_at IS NOT NULL`, with the entire OR-list of ID/version matches grouped in parentheses. This prevents an old heartbeat from restoring a cleared lock.
- **If we skip it:** `Wait` treats the future schedule as still running. A schedule interval shorter than `LockTimeout` can also be delayed until that spurious lock becomes stale.

**Verified two ways:**

1. With the ordinary `NewSQLite` driver and the README's WAL/immediate-transaction settings, ran a scheduled `RegisterTx` handler for 200 ms with `LockTimeout = 300 ms`. Its open write transaction blocked the 100 ms heartbeat; after the handler completed and rescheduled an hour ahead, the heartbeat restored `locked_at`. Repeated this test three times successfully, without driver injection.
2. With a controlled pause immediately before the heartbeat update, confirmed the same interleaving on both SQLite and PostgreSQL: completion first cleared the lock, then the delayed heartbeat restored it. `Wait` subsequently expired instead of ignoring the future task.

### 4. One temporary startup failure can suppress a schedule forever

**Location:** `workq.go:204–220`; polling at `workq.go:349–365` never retries schedule creation.

- **What this is:** `Start` inserts one persistent row for each registered schedule before launching the executor.
- **Problem:** If that insert fails, `Start` logs the error and continues. It neither reports failure to its caller nor retries the missing row, so a recovered database does not restore the schedule.
- **Fix:** Retain failed schedule inserts and retry them from the poll loop until successful, using the existing idempotent insert. This preserves the fixed `Start` API and does not require another goroutine.
- **If we skip it:** Maintenance or other recurring work can silently remain absent until a restart or another node happens to create its row. `Failed` cannot reveal it because no task exists.

**Verified:** Injected a temporary error on only the first schedule INSERT; every subsequent database operation, including any hypothetical second INSERT, was allowed to succeed. With a 5 ms poll interval and a schedule due after 1 ms, after 100 ms there were zero schedule rows and exactly one insertion attempt. Repeated three times. The only signal was the startup log message.

## Should fix

### 5. Routed polling scans unrelated backlogs on every empty claim

**Location:** `workq.go:132`, `workq.go:372–378`, `workq.go:426–427`; performance claims in `README.md`, “Large backlogs don't slow it down.”

- **What this is:** Nodes filter by registered task names, but the due-task index contains only queue, run time, and ID.
- **Problem:** A node with no eligible name can scan the whole due backlog belonging to other handlers. Empty claims reset the cursor, so every poll repeats that work; the cursor optimization does not protect this supported routing case.
- **Fix:** Add a name-aware partial index such as `(queue, name, run_at, id) WHERE failed_at IS NULL`, checking plans for both single-name and multi-name registration. Keep the existing ordering index unless measurements justify replacing it, and qualify the README's unconditional backlog claim.
- **If we skip it:** Idle routed executors consume database CPU in proportion to unrelated pending work. More routed nodes multiply that cost, despite having no tasks they can run.

**Verified:** Inserted 100,000 due, unlocked tasks named `other` in the default queue, ran `ANALYZE`, then explained the actual PostgreSQL claim shape for a node registering only `mine`, with a limit of 64. `EXPLAIN (ANALYZE, BUFFERS)` showed:

```text
Seq Scan on workq t
  Rows Removed by Filter: 100000
Buffers: shared hit=1031
Execution Time: 7.955 ms
```

After adding the name-aware index, the same empty claim used that index, touched three index buffers, and took **0.046 ms** in this run. These timings are illustrative, not a throughput benchmark; the important result is eliminating the full scan.

The reproduction used this query, which matches `claimSQL`'s selection and update logic:

```sql
EXPLAIN (ANALYZE, BUFFERS)
UPDATE workq SET locked_at = 1000, version = version + 1
WHERE id = ANY(ARRAY(
  SELECT id FROM workq t
  WHERE t.failed_at IS NULL AND t.run_at <= 1000
    AND (t.locked_at IS NULL OR t.locked_at < 0)
    AND t.queue = ''
    AND (t.run_at, t.id) > (-9223372036854775808, 0)
    AND t.name IN ('mine')
  ORDER BY t.run_at, t.id LIMIT 64 FOR UPDATE SKIP LOCKED
)) RETURNING id;
```

## Test gaps tied to these findings

The passing suite tests ordinary shutdown, heartbeat survival, stale takeover, schedules, and worker capacity separately. It does not combine blocked claims with shutdown deadlines, overlapping versions of one task with worker accounting, transactional rescheduling with an in-flight heartbeat, failed startup inserts with recovery, or routed backlogs with query-plan checks. Add the reproductions above as regression tests alongside the corresponding fixes.

**Verdict: fix 1–4 before shipping; address 5 before relying on large routed backlogs.**

**Not checked:** Re-running the published million-task/River benchmarks, power-loss durability, remote Docker hosts, or a full operating-system/database-version compatibility matrix. No fixes were applied; the verified defects remain in the implementation.
