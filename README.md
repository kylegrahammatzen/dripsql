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
- `internal/{kernel,vector}` — typed kernels and vectors.

## Tests

```
go test -count=1 ./...
```

`-count=1` bypasses Go's test cache so every invocation re-runs.

## Microbenchmarks

```
go test ./internal/kernel ./internal/storage -run ^$ -bench . -benchmem -benchtime=100ms -count=1
```

Use longer `-benchtime` and higher `-count` for noisy paths.

## Workload benchmark

```
go run ./cmd/bench -rows 100000 -runs 5
```

Flags:

- `-rows N[,N...]` — row counts to benchmark.
- `-runs N` — timing samples per query.
- `-profile structured|random|skewed` — `structured` rotates a fixed value set, `random` is uniform splitmix64, `skewed` is heavy-tailed (representative of real event analytics).
- `-emit text|json` — output format; JSON is the input shape for `-baseline`.
- `-baseline previous.json` — diff this run against a saved JSON report; only deltas above 5% render unless `-show-all` is passed.
- `-dir <path>` and `-keep` — run against a chosen directory and leave it on disk.

Baseline workflow:

```
go run ./cmd/bench -rows 100000 -runs 5 -emit json > run-a.json
go run ./cmd/bench -rows 100000 -runs 5 -baseline run-a.json
```

## CLI

```
go run ./cmd/cli version
go run ./cmd/cli exec <db> "CREATE TABLE events (id INT64 NOT NULL)"
go run ./cmd/cli query <db> "SELECT count(*) FROM events"
go run ./cmd/cli query <db> "EXPLAIN ANALYZE SELECT count(*) FROM events WHERE id = 42"
```

## Notes

- Active status: `current.md`.
- Forward-looking goals: `goals.md`.
- Benchmark history: `benchmarks.md`.

## License

MIT
