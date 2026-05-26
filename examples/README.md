<div align="center">
  <a href="../README.md">DripSQL</a>
  /
  <a href="../cmd/bench/README.md">Benchmarks</a>
  /
  <a href="../cmd/cli/README.md">CLI</a>
</div>

# DripSQL - Examples

Runnable demos of the public API. Each example is its own `package main`.

Each example takes an optional `-db <path>`; when omitted it uses a fresh temp directory and prints the location.

```
go run ./examples/<name>
```

| Name | Demonstrates |
| --- | --- |
| `embed` | `Open`, `Exec`, `QueryRow` with a parameterized argument |
| `read_only` | `SetReadOnly` plus the `ErrReadOnly` sentinel via `errors.Is` |
| `transactions` | `Update` closure that commits on nil and rolls back on error |
| `view` | `View` closure for read-only work that stays permitted under `SetReadOnly` |
| `durability` | Reopen a database directory and read back rows written by a prior session |
| `group_by` | Streaming `Rows.Next` loop over a multi-row aggregate result |
| `explain` | `EXPLAIN` output for a filter plus group-by plan |
| `rename_column` | `ALTER TABLE RENAME COLUMN` as a metadata-only operation that scans existing segments by stable column id |
| `time_travel` | `QueryAt` and SQL `AS OF` reading a snapshot pinned to an earlier commit timestamp |
| `alter_table` | `ALTER TABLE ADD COLUMN` with a default, `ALTER COLUMN TYPE` widening, and `DROP COLUMN` |
