## Workload benchmark driver

Run from the repository root with:

```bash
go run ./cmd/bench
```

Useful options:

- `-rows`: comma-separated row counts (defaults to the built-in sweep)
- `-runs`: timing samples per query
- `-profile`: `structured|random|skewed|wide-text|sorted|all`; `all` runs `structured`, `random`, and `skewed` (default: `all`)
- `-mode`: `same-process|warm-reopen|cold-ish|all`
- `-query`: substring filter for query names
- `-segment-rows`: rows per sealed segment (`0` uses the storage default; larger values reduce seal count during ingest)
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
