Implement workq.go per README.md. The README is the spec; the existing workq.go scaffold fixes the API signatures.

- Module `github.com/nobloat/workq`, Go 1.27, with a root go.mod that has no requirements. workq.go imports only the standard library.
- Native pgx support in a separate module at `pgxq/` (`github.com/nobloat/workq/pgxq`, `replace github.com/nobloat/workq => ../`): one file implementing `workq.Driver[pgx.Tx]` on a `pgxpool.Pool`, so pgx never appears in the root go.mod.
- Put the tests in a separate module at `tests/`: `tests/go.mod`, module `github.com/nobloat/workq/tests`, with replaces for `github.com/nobloat/workq => ../` and `github.com/nobloat/workq/pgxq => ../pgxq`. Test-only dependencies must not appear in the root go.mod.
- One test suite runs against SQLite (`modernc.org/sqlite`), Postgres via `database/sql` (pgx stdlib driver) and Postgres via `pgxq`, inside `testing/synctest` bubbles (fake time). A benchmark compares raw enqueue/execution throughput of no-op tasks across the three at several levels of parallelism. No testcontainers: `TestMain` starts and removes a Postgres container through the `docker` CLI, and each test resets the schema itself. Skip Postgres when Docker isn't available, except when the `CI` env var is set: then fail.
- Add `.github/workflows/test.yml`, triggered on push and pull_request, running on ubuntu-latest:
  1. setup-go
  2. `go vet ./...` at the root and in `pgxq/`
  3. `cd tests && go test -race ./...`
