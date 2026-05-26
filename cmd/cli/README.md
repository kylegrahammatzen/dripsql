# CLI

<div align="center">
  <a href="../../README.md">DripSQL</a>
  /
  <a href="../../examples/README.md">Examples</a>
  /
  <a href="../bench/README.md">Benchmarks</a>
</div>

Single-shot SQL runner that parses one SQL string per invocation and runs it against the `-db` directory, which is created automatically if it does not exist.

```
go run ./cmd/cli -db <path> -exec "<sql>"
```

| Flag | Required | Description |
| --- | --- | --- |
| `-db` | yes | Database directory |
| `-exec` | yes | SQL string to run |

Example:

```
go run ./cmd/cli -db ./demo-db -exec "CREATE TABLE events (id INT64 NOT NULL)"
go run ./cmd/cli -db ./demo-db -exec "INSERT INTO events (id) VALUES (1), (2), (42)"
go run ./cmd/cli -db ./demo-db -exec "SELECT count(*) FROM events"
```
