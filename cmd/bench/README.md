<div align="center">
  <a href="../../README.md">DripSQL</a>
  /
  <a href="../../examples/README.md">Examples</a>
  /
  <a href="../cli/README.md">CLI</a>
</div>

# DripSQL - Benchmarks

All numbers were captured on `AMD Ryzen 7 3700X` / `Windows amd64` / `go1.26.0`. Medians across 10 runs at `-benchtime=1s`.

## Workload driver

```
go run ./cmd/bench -query <name> -rows <N> -runs <R> -mode hot -json
```

| Flag | Description |
| --- | --- |
| `-query` | Cataloged query name; see `go run ./cmd/bench list` |
| `-rows` | Synthetic dataset row count |
| `-runs` | Timed-run count for the median |
| `-mode` | `hot` or `cold-soft` (close and reopen DB between runs) |
| `-json` | Emit results as JSON for downstream tooling |

## Comparing workload runs

```
go run ./cmd/bench -query top_age -rows 1000000 -runs 50 -mode hot -json > head.json
go run ./cmd/bench compare base.json head.json -threshold 10
```

Use fresh JSON or JSONL artifacts because `compare` reports median deltas against a threshold.

## Snapshot (hot mode, median wall time across timed runs)

| Query | Rows | Runs | Median | io | decode | exec |
| --- | --- | --- | --- | --- | --- | --- |
| `count` | 100k | 200 | <1 us | <1 us | <1 us | <1 us |
| `id_lookup` | 100k | 200 | <1 us | 25.3 us | 50.3 us | <1 us |
| `category_groupby` | 100k | 100 | 3.79 ms | 2.01 ms | 1.44 ms | 339 us |
| `category_groupby` | 10M | 10 | 275.76 ms | 86.87 ms | 97.71 ms | 91.17 ms |
| `top_age` | 100k | 200 | 1.00 ms | 213 us | 302 us | 485 us |
| `top_age` | 1M | 50 | 8.48 ms | 1.39 ms | 3.20 ms | 3.89 ms |
| `top_age` | 10M | 10 | 78.53 ms | 15.28 ms | 28.83 ms | 34.42 ms |

- `count` and `id_lookup` short-circuit via the `.sm` numsum and Binary Fuse 8 sidecars.
- `count(*)` stays on the metadata-only path even after `DELETE` by popcounting the segment's deletion vector instead of scanning pages.
- `category_count` reads from the dict-histogram sidecar when no `WHERE` is present.
- TPC-H Q1 and Q6 run against a synthetic `lineitem` with dates as int64 days since 1992-01-01.
- `<1 us` means the workload driver rounded the median below the microsecond measurement floor.
- For parallel scans, `io` and `decode` are summed worker time rather than wall time.

## Exec microbenchmarks

```
go test ./internal/exec '-bench=.' '-benchmem' '-run=^$' '-count=10'
```

| Bench | Time | B/op | allocs/op |
| --- | --- | --- | --- |
| `Filter_Int64Less` | 2.55 us | 256 | 1 |
| `Filter_Int64Between` | 3.10 us | 256 | 1 |
| `Filter_Int64AndCompound` | 4.34 us | 256 | 1 |
| `Filter_Int64Equal` | 2.44 us | 256 | 1 |
| `Sort_FullAsc_Int64_10k` | 1.35 ms | 250 K | 32 |
| `Sort_FullDesc_Int64_10k` | 1.26 ms | 250 K | 32 |
| `Sort_TopK_Int64_10k_K100` | 97.8 us | 6.4 K | 12 |
| `Sort_TopK_Int64_100k_K100` | 733 us | 17.1 K | 15 |
| `Sort_TopK_Int64_100k_K100_Off50` | 762 us | 18.5 K | 15 |
| `Sort_TopK_Int64_100k_K100_NullsEvery10` | 775 us | 17.1 K | 15 |
| `Sort_FullAsc_Text_10k` | 1.38 ms | 291 K | 38 |

## Storage microbenchmarks

```
go test ./internal/storage '-bench=.' '-benchmem' '-run=^$' '-count=10'
go test ./internal/storage/codec '-bench=.' '-benchmem' '-run=^$' '-count=10'
```

Rows are ordered from codec decode to page read to scan to write so low level costs explain the higher level paths.

| Bench | Time | B/op | allocs/op |
| --- | --- | --- | --- |
| `Codec_Decode/FOR/Int64` | 20 us | 16 K | 1 |
| `Codec_Decode/Delta/Int64` | 13 us | 33 K | 2 |
| `Codec_Decode/Pcodec/Int64` | 6.5 us | 16 K | 1 |
| `Codec_Decode/Constant/Int64` | 5.2 us | 0 | 0 |
| `Codec_Decode/Sequence/Int64` | 1.6 us | 0 | 0 |
| `Codec_Decode/Plain/Int64` | 170 ns | 0 | 0 |
| `Codec_Decode/Dict/Text` | 7.4 us | 33 K | 5 |
| `Codec_Decode/Plain/Text` | 39 us | 55 K | 3 |
| `Storage_ReadPage/Int64` | 6.23 us | 16 K | 2 |
| `Storage_ReadPage/Text` | 8.64 us | 33 K | 6 |
| `Storage_Scan/One/Int64` | 23.4 us | 17 K | 12 |
| `Storage_Scan/One/Text` | 40.6 us | 135 K | 31 |
| `Storage_Scan/All` | 239 us | 369 K | 64 |
| `Storage_Scan/Eq/Int64/Hit` | 274 us | 369 K | 66 |
| `Storage_Scan/Eq/Int64/Miss` | 698 ns | 232 | 6 |
| `Storage_Scan/Eq/Text/Hit` | 375 us | 367 K | 65 |
| `Storage_Scan/Lt/Int64/AllByMeta` | 40.8 us | 132 K | 27 |
| `Storage_Write/Int64/Random` | 1.99 ms | 290 K | 91 |
| `Storage_Write/Int64/Constant` | 1.91 ms | 116 K | 68 |
| `Storage_Write/Int64/Sequence` | 1.77 ms | 240 K | 82 |
| `Storage_Write/Int64/SparseNulls` | 1.62 ms | 132 K | 53 |
| `Storage_Write/Float64/Plain` | 2.30 ms | 115 K | 80 |
| `Storage_Write/Float64/Decimal` | 2.11 ms | 97 K | 60 |
| `Storage_Write/Text/Dict` | 2.52 ms | 426 K | 206 |

`Storage_Scan/Eq/Int64/Miss` is the Binary Fuse 8 page-prune path and sidecar loaders run once per segment open.

## Iterating on a single bench

Full sweeps are slow because every benchmark runs for about `benchtime` per count.

**Precompile And Reuse**

```
go test -c -o exec.test.exe ./internal/exec
.\exec.test.exe '-test.run=^$' '-test.bench=Filter_Int64Less' '-test.benchtime=1s' '-test.count=10' '-test.benchmem'
```

`-test.run=^$` runs only benchmarks, and PowerShell needs dotted flags single quoted.

**Targeted filter for smoke checks** during perf iteration:

```
go test '-run=^$' '-bench=Benchmark(Filter_|Storage_Scan|Codec_Decode)' '-benchtime=500ms' '-count=3' ./internal/...
```

- Runs in ~10s instead of 10 min.
- Anchor the regex with `Benchmark(...)` because a bare `Filter` also matches things like `Storage_LoadIntFilter`.
- Reserve this form for "did I regress" iteration and use the full-sweep form above when capturing a fresh baseline.
