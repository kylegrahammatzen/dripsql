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

`compare` reads JSON or JSONL outputs from the workload driver and reports median deltas with a simple threshold verdict. Keep comparison artifacts fresh because workload variance and implementation details move quickly.

## Snapshot (hot mode, median wall time across timed runs)

| Query | Rows | Runs | Median | io | decode | exec |
| --- | --- | --- | --- | --- | --- | --- |
| `count` | 100k | 200 | <1 us | <1 us | <1 us | <1 us |
| `id_lookup` | 100k | 200 | <1 us | 30 us | 49 us | <1 us |
| `category_groupby` | 100k | 100 | 5.93 ms | 1.74 ms | 2.40 ms | 1.78 ms |
| `category_groupby` | 10M | 10 | 622.19 ms | 147.01 ms | 593.72 ms | <1 us |
| `top_age` | 100k | 200 | 2.14 ms | 441 us | 1.29 ms | 413 us |
| `top_age` | 1M | 50 | 28.03 ms | 6.05 ms | 15.92 ms | 6.06 ms |
| `top_age` | 10M | 10 | 257.08 ms | 54.96 ms | 151.15 ms | 50.96 ms |

- `count` and `id_lookup` short-circuit via the `.sm` numsum and Binary Fuse 8 sidecars.
- `count(*)` stays on the metadata-only path even after `DELETE` by popcounting the segment's deletion vector instead of scanning pages.
- `category_groupby` reads from the dict-histogram sidecar when no `WHERE` is present.
- TPC-H Q1 and Q6 run against a synthetic `lineitem` with dates as int64 days since 1992-01-01.
- `<1 us` means the workload driver reported a zero microsecond median, which is below the harness measurement floor rather than literal zero work.

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
| `Sort_FullAsc_Int64_10k` | 1.45 ms | 251 K | 33 |
| `Sort_FullDesc_Int64_10k` | 1.42 ms | 251 K | 33 |
| `Sort_TopK_Int64_10k_K100` | 97.8 us | 6.4 K | 12 |
| `Sort_TopK_Int64_100k_K100` | 733 us | 17.1 K | 15 |
| `Sort_TopK_Int64_100k_K100_Off50` | 762 us | 18.5 K | 15 |
| `Sort_TopK_Int64_100k_K100_NullsEvery10` | 775 us | 17.1 K | 15 |
| `Sort_FullAsc_Text_10k` | 3.44 ms | 251 K | 38 |

## Storage microbenchmarks

```
go test ./internal/storage '-bench=.' '-benchmem' '-run=^$' '-count=10'
go test ./internal/storage/codec '-bench=.' '-benchmem' '-run=^$' '-count=10'
```

| Bench | Time | B/op | allocs/op |
| --- | --- | --- | --- |
| `Storage_ScanFull` | 279 us | 467 K | 70 |
| `Storage_ScanEqInt64Hit` | 289 us | 467 K | 72 |
| `Storage_ScanEqInt64Miss` | 671 ns | 232 | 6 |
| `Storage_ScanEqBytesHit` | 318 us | 465 K | 71 |
| `Storage_ScanLtInt64AllMatchUnprojected` | 65 us | 132 K | 27 |
| `Storage_WriteSegment_Int64Random` | 2.99 ms | 294 K | 91 |
| `Storage_WriteSegment_Int64Constant` | 2.20 ms | 150 K | 85 |
| `Storage_WriteSegment_Int64Monotonic` | 2.28 ms | 277 K | 98 |
| `Storage_WriteSegment_Int64SparseNulls` | 1.94 ms | 132 K | 53 |
| `Storage_WriteSegment_Float64Plain` | 2.30 ms | 115 K | 80 |
| `Storage_WriteSegment_Float64Decimal` | 2.11 ms | 97 K | 60 |
| `Storage_WriteSegment_TextLowCardinality` | 2.52 ms | 426 K | 206 |
| `Codec_Decode/for_int64` | 26 us | 16 K | 1 |
| `Codec_Decode/delta_int64` | 22 us | 33 K | 2 |
| `Codec_Decode/pcodec_int64` | 22 us | 16 K | 1 |
| `Codec_Decode/constant_int64` | 5.3 us | 0 | 0 |
| `Codec_Decode/sequence_int64` | 1.6 us | 0 | 0 |
| `Codec_Decode/plain_int64` | 160 ns | 0 | 0 |
| `Codec_Decode/dict_text_lowcard` | 9.7 us | 33 K | 5 |
| `Codec_Decode/plain_text` | 48 us | 55 K | 3 |

`Storage_ScanEqInt64Miss` is the page-prune fast path resolving in sub-microsecond via the Binary Fuse 8 `.bf` sidecar. Sidecar loaders all sit below 25us and run once per segment open.

## Iterating on a single bench

The full sweep above takes 8-12 minutes because `go test -bench` adapts iterations so every function runs ~`benchtime` seconds, then `-count=10` multiplies that. Wall time is `N_functions × benchtime × count + compile`. For perf iteration on the same bench, two patterns cut that.

**Precompile and reuse the test binary** when running the same bench repeatedly against unchanged code. The build cache already shortcuts most re-compiles after edits, so this pattern shines on repeat runs, not on the first invocation after an edit:

```
go test -c -o exec.test.exe ./internal/exec
.\exec.test.exe '-test.run=^$' '-test.bench=Filter_Int64Less' '-test.benchtime=1s' '-test.count=10' '-test.benchmem'
```

`-test.run=^$` skips normal tests so only the bench runs. PowerShell needs the dotted flags single-quoted. Drop the binary when done.

**Targeted filter for smoke checks** during perf iteration:

```
go test '-run=^$' '-bench=Benchmark(Filter_|Storage_ScanFull)|Codec_Decode/plain_int64' '-benchtime=500ms' '-count=3' ./internal/...
```

- Runs in ~10s instead of 10 min.
- Anchor the regex with `Benchmark(...)` because a bare `Filter` also matches things like `Storage_LoadIntFilter`.
- Reserve this form for "did I regress" iteration and use the full-sweep form above when capturing a fresh baseline.
