# DripSQL

Experimental single-node SQL analytics engine for exploring columnar storage,
compression, and memory-efficient query execution. APIs and storage formats
will change.

## Layout

- `cmd/cli` — admin shell (`version`, `exec`, `query`).
- `cmd/bench` — workload benchmark driver.
- `internal/engine` — DB lifecycle, query/exec, EXPLAIN.
- `internal/explain` — report types and renderers.
- `internal/storage` — immutable columnar segments, predicate pushdown.
- `internal/sql` — parser, binder, logical plan.
- `internal/types` — table specs, typed batches, and vectors.

## Tests

```
go test -count=1 ./...
```

`-count=1` bypasses Go's test cache so every invocation re-runs.

## Performance commands

```
go test ./internal/storage ./internal/storage/codec ./internal/sql -run ^$ -bench . -benchmem -benchtime=100ms -count=1
```

Use longer `-benchtime` and higher `-count` for noisy paths.

## Workload benchmark

Benchmark usage and workflow are documented in [`cmd/bench/README.md`](cmd/bench/README.md).

### Baseline snapshot (2026-05-10)

Hardware: `AMD Ryzen 7 3700X`, `Windows amd64`.

| Scope | Command / profile | Snapshot metric |
| --- | --- | --- |
| Event type count metadata | `go run ./cmd/bench -runs 3 -profile structured` (10,000,000-row point in sweep) | `count(*) WHERE event_type = 'checkout'` best `3.254ms`, avg `3.439ms`, payload `0 B`, result `2,500,000`. |
| Text summary group scan | `go run ./cmd/bench -runs 3 -profile structured` (10,000,000-row point in sweep) | `event_type, count(*) GROUP BY event_type` best `2.666ms`, avg `2.684ms`, payload `0 B`, result `4` rows. |
| Grouped text count + sum | `go run ./cmd/bench -runs 5 -profile structured -query "country aggregate summary"` (1,000,000-row point in sweep) | first `51.9ms`, best `8.040ms`, avg `17.3ms`, p95 `51.9ms`, payload `1.82 MiB`. |
| Multi-aggregate coverage | `go run ./cmd/bench -runs 3 -profile all` (100,000-row point in sweep) | `count(*), sum(amount), min(amount), max(amount)` and `country, count(*), sum(amount) GROUP BY country` now report p95 + fractional bytes/match in output. |

These are baseline reference points, not new benchmark claims.

## CLI

```
go run ./cmd/cli version
go run ./cmd/cli exec <db> "CREATE TABLE events (id INT64 NOT NULL)"
go run ./cmd/cli query <db> "SELECT count(*) FROM events"
go run ./cmd/cli query <db> "EXPLAIN ANALYZE SELECT count(*) FROM events WHERE id = 42"
```

## License

MIT
