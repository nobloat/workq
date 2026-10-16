Implement workq.go per README.md. The README is the spec; the existing workq.go scaffold fixes the API signatures.

- Module `github.com/nobloat/workq`, Go 1.27, with a root go.mod that has no requirements. workq.go imports only the standard library.
- Put the tests in a separate module at `tests/`: `tests/go.mod`, module `github.com/nobloat/workq/tests`, with `replace github.com/nobloat/workq => ../`. Test-only dependencies must not appear in the root go.mod.
- One test suite runs against both SQLite (`modernc.org/sqlite`) and Postgres (pgx stdlib driver). No testcontainers: `TestMain` starts and removes a Postgres container through the `docker` CLI, and each test resets the schema itself. Skip Postgres when Docker isn't available, except when the `CI` env var is set: then fail.
- Add `.github/workflows/test.yml`, triggered on push and pull_request, running on ubuntu-latest:
  1. setup-go
  2. `go vet ./...` at the root
  3. `cd tests && go test -race ./...`
