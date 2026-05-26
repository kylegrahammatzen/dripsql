# Benchmarks

<div align="center">
  <a href="../../README.md">DripSQL</a>
  /
  <a href="../../examples/README.md">Examples</a>
  /
  <a href="../cli/README.md">CLI</a>
</div>

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
| `-mode` | `hot`, `cold-soft` (reopen DB between runs), or `cold-hard` (drop OS page cache) |
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

`count` and `id_lookup` short-circuit via the `.sm` numsum and Binary Fuse 8 sidecars; `category_groupby` reads from the dict-histogram sidecar when no `WHERE` is present. TPC-H Q1/Q6 run against a synthetic `lineitem` with dates as int64 days since 1992-01-01.

## Exec microbenchmarks

```
go test ./internal/exec -bench=. -benchmem -run=^$ -count=10
```

| Bench | ns/op | B/op | allocs/op |
| --- | --- | --- | --- |
| `Filter_Int64Less` | _pending_ | | |
| `Filter_Int64Between` | _pending_ | | |
| `Filter_Int64AndCompound` | _pending_ | | |
| `Filter_Int64Equal` | _pending_ | | |
| `Sort_FullAsc_Int64_10k` | _pending_ | | |
| `Sort_FullDesc_Int64_10k` | _pending_ | | |
| `Sort_TopK_Int64_10k_K100` | _pending_ | | |
| `Sort_TopK_Int64_100k_K100` | _pending_ | | |
| `Sort_TopK_Int64_100k_K100_Off50` | _pending_ | | |
| `Sort_TopK_Int64_100k_K100_NullsEvery10` | _pending_ | | |
| `Sort_FullAsc_Text_10k` | _pending_ | | |

## Storage microbenchmarks

```
go test ./internal/storage -bench=. -benchmem -run=^$ -count=10
go test ./internal/storage/codec -bench=. -benchmem -run=^$ -count=10
```

| Bench | ns/op | B/op | allocs/op |
| --- | --- | --- | --- |
| `Storage_ScanFull` | _pending_ | | |
| `Storage_ScanEqInt64Hit` | _pending_ | | |
| `Storage_ScanEqInt64Miss` | _pending_ | | |
| `Storage_ScanEqBytesHit` | _pending_ | | |
| `Storage_WriteSegment_Int64Random` | _pending_ | | |
| `Storage_WriteSegment_Int64Constant` | _pending_ | | |
| `Storage_WriteSegment_Int64Monotonic` | _pending_ | | |
| `Storage_WriteSegment_Int64SparseNulls` | _pending_ | | |
| `Storage_WriteSegment_Float64Plain` | _pending_ | | |
| `Storage_WriteSegment_Float64Decimal` | _pending_ | | |
| `Storage_WriteSegment_TextLowCardinality` | _pending_ | | |
| `Codec_Decode/for_int64` | _pending_ | | |
| `Codec_Decode/delta_int64` | _pending_ | | |
| `Codec_Decode/pcodec_int64` | _pending_ | | |
| `Codec_Decode/constant_int64` | _pending_ | | |
| `Codec_Decode/sequence_int64` | _pending_ | | |
| `Codec_Decode/plain_int64` | _pending_ | | |
| `Codec_Decode/dict_text_lowcard` | _pending_ | | |
| `Codec_Decode/plain_text` | _pending_ | | |
