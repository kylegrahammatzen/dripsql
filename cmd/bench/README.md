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

## Snapshot (hot mode, median ms across timed runs)

| Query | Rows | Runs | Median ms | io | decode | exec |
| --- | --- | --- | --- | --- | --- | --- |
| `count` | 100k | 200 | ~0 | 0 | 0 | 0 |
| `id_lookup` | 100k | 200 | ~0 | 0.03 | 0.06 | 0 |
| `category_groupby` | 100k | 100 | 5.94 | 1.43 | 2.50 | 2.01 |
| `category_groupby` | 10M | 10 | 608.13 | 118.59 | 569.48 | ~0 |
| `top_age` | 100k | 200 | 2.27 | 0.49 | 1.36 | 0.42 |
| `top_age` | 1M | 50 | 26.46 | 5.69 | 16.12 | 4.64 |
| `top_age` | 10M | 10 | 249.17 | 52.91 | 155.24 | 41.02 |

- `count` and `id_lookup` short-circuit via the `.sm` numsum and Binary Fuse 8 sidecars.
- `count(*)` stays on the metadata-only path even after `DELETE` by popcounting the segment's deletion vector instead of scanning pages.
- `category_groupby` reads from the dict-histogram sidecar when no `WHERE` is present.
- TPC-H Q1 and Q6 run against a synthetic `lineitem` with dates as int64 days since 1992-01-01.

## Exec microbenchmarks

```
go test ./internal/exec '-bench=.' '-benchmem' '-run=^$' '-count=10'
```

| Bench | ns/op | B/op | allocs/op |
| --- | --- | --- | --- |
| `Filter_Int64Less` | 2494 | 256 | 1 |
| `Filter_Int64Between` | 3288 | 256 | 1 |
| `Filter_Int64AndCompound` | 5378 | 256 | 1 |
| `Filter_Int64Equal` | 2684 | 256 | 1 |
| `Sort_FullAsc_Int64_10k` | 1.49 ms | 252 K | 38 |
| `Sort_FullDesc_Int64_10k` | 1.59 ms | 252 K | 38 |
| `Sort_TopK_Int64_10k_K100` | 194 us | 7.7 K | 17 |
| `Sort_TopK_Int64_100k_K100` | 1.19 ms | 29.6 K | 64 |
| `Sort_TopK_Int64_100k_K100_Off50` | 1.23 ms | 31.0 K | 64 |
| `Sort_TopK_Int64_100k_K100_NullsEvery10` | 1.19 ms | 29.6 K | 64 |
| `Sort_FullAsc_Text_10k` | 3.74 ms | 253 K | 43 |

## Storage microbenchmarks

```
go test ./internal/storage '-bench=.' '-benchmem' '-run=^$' '-count=10'
go test ./internal/storage/codec '-bench=.' '-benchmem' '-run=^$' '-count=10'
```

| Bench | ns/op | B/op | allocs/op |
| --- | --- | --- | --- |
| `Storage_ScanFull` | 288 us | 467 K | 70 |
| `Storage_ScanEqInt64Hit` | 305 us | 467 K | 72 |
| `Storage_ScanEqInt64Miss` | 730 | 232 | 6 |
| `Storage_ScanEqBytesHit` | 323 us | 465 K | 71 |
| `Storage_WriteSegment_Int64Random` | 3.58 ms | 294 K | 91 |
| `Storage_WriteSegment_Int64Constant` | 2.71 ms | 150 K | 85 |
| `Storage_WriteSegment_Int64Monotonic` | 2.97 ms | 277 K | 98 |
| `Storage_WriteSegment_Int64SparseNulls` | 2.51 ms | 132 K | 53 |
| `Storage_WriteSegment_Float64Plain` | 4.43 ms | 263 K | 81 |
| `Storage_WriteSegment_Float64Decimal` | 3.53 ms | 287 K | 119 |
| `Storage_WriteSegment_TextLowCardinality` | 16.5 ms | 2.99 M | 307 K |
| `Codec_Decode/for_int64` | 30 us | 87 K | 16 K |
| `Codec_Decode/delta_int64` | 26 us | 31 K | 33 K |
| `Codec_Decode/pcodec_int64` | 27 us | 60 K | 16 K |
| `Codec_Decode/constant_int64` | 5.5 us | 1.5 K | 0 |
| `Codec_Decode/sequence_int64` | 1.6 us | 9.8 K | 0 |
| `Codec_Decode/plain_int64` | 190 | 87 K | 0 |
| `Codec_Decode/dict_text_lowcard` | 12 us | 181 K | 33 K |
| `Codec_Decode/plain_text` | 63 us | 503 K | 55 K |

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
- Reserve this form for "did I regress" iteration and use the full-sweep form above when capturing a `.bench/final.txt` baseline.
