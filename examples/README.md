<div align="center">
  <a href="../README.md">DripSQL</a>
  /
  <a href="../cmd/bench/README.md">Benchmarks</a>
  /
  <a href="../cmd/cli/README.md">CLI</a>
</div>

# DripSQL - Examples

Runnable demos of the public API. Each example is its own `package main`.

```
go run ./examples/<name> -db ./demo-db
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
