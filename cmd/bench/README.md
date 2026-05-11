## Workload benchmark driver

Run from the repository root with:

```bash
go run ./cmd/bench
```

Useful options:

- `-rows`: comma-separated target row counts (defaults to the built-in sweep)
- `-runs`: timing samples per query after the database has been loaded once
- `-profile`: `structured|random|skewed|wide-text|sorted|all`; `all` runs `structured`, `random`, and `skewed` (default: `all`)
- `-mode`: `same-process|warm-reopen|cold-ish|all`
- `-query`: substring filter for query names
- `-segment-rows`: rows per sealed segment (`0` uses the storage default; larger values reduce seal count during ingest)
- `-dir`: benchmark database root (default: `db/bench`, reused across runs)
- `-json`: emit machine-readable output
- `-baseline <file-or-dir>`: compare against saved JSON output
- `-show-all`, `-details`: expand baseline comparisons

Common runs:

```bash
go run ./cmd/bench
go run ./cmd/bench -runs 3
go run ./cmd/bench -rows 10000000 -segment-rows 2097152
go run ./cmd/bench -query country -mode all
go run ./cmd/bench -baseline baselines -show-all -details
```

Update baselines:

```bash
go run ./cmd/bench -profile structured -json > baselines/structured.json
go run ./cmd/bench -profile random -json > baselines/random.json
go run ./cmd/bench -profile skewed -json > baselines/skewed.json
```

Text output separates setup from query timing. Benchmarks reuse databases under
`db/bench/<profile>/<segment-size>` by default; delete `db/` to force a fresh
load. If the database already has at least the requested rows, setup appends
nothing. If it has fewer rows, setup appends only the missing row range.
`Setup > Wall` is the actual setup wall-clock time for appended rows.
`Generate workers` is cumulative worker time and can be larger than wall time
because batch generation runs in parallel and overlaps with append. Query
timings are reported under `Query Benchmark`.
