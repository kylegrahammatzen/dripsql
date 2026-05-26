# Benchmark Logs

This directory keeps curated benchmark captures that are useful as local history.

Keep only fresh, intentional artifacts here. Prefer timestamped names so every run is traceable.

## Capture A Workload

```
go run ./cmd/bench -query top_age -rows 1000000 -runs 50 -mode hot -json > .bench/workload-YYYYMMDD-HHMMSS.json
```

Use JSONL when recording several workload queries into one file.

```
go run ./cmd/bench -query count -rows 100000 -runs 200 -mode hot -json > .bench/workload-YYYYMMDD-HHMMSS.jsonl
go run ./cmd/bench -query top_age -rows 1000000 -runs 50 -mode hot -json >> .bench/workload-YYYYMMDD-HHMMSS.jsonl
```

## Compare Runs

```
go run ./cmd/bench compare .bench/base.jsonl .bench/head.jsonl -threshold 10
```

Do not treat old captures as permanent truth. Use them as history, then refresh the docs from a new run when performance work lands.
