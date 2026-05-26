<div align="center">
  <a href="./examples/README.md">Examples</a>
  /
  <a href="./cmd/bench/README.md">Benchmarks</a>
  /
  <a href="./cmd/cli/README.md">CLI</a>
</div>

# DripSQL

Embeddable single-node SQL analytics engine for Go with columnar storage and vectorized execution.

- Embedded analytical queries in a single binary
- Time-travel reads against any committed snapshot
- Columnar storage with FOR, Delta, Dictionary, FSST, ALP, and Pcodec cascades
- ACID transactions with snapshot isolation and atomic multi-table commit
- Window functions, CTEs, hash joins, and correlated subqueries
- Metadata-only ALTER TABLE rename, add, drop, and widening type changes

## Tests

```
go test -count=1 ./...
```

`-count=1` bypasses Go's test cache so every invocation re-runs.

## License

MIT
