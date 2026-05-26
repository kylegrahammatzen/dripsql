# DripSQL

Embeddable single-node SQL analytics engine for Go with columnar storage and vectorized execution.

- Embedded analytical queries in a single binary
- Time-travel reads against any committed snapshot
- Columnar storage with FOR, Delta, Dictionary, FSST, ALP, and Pcodec cascades
- ACID transactions with snapshot isolation and atomic multi-table commit
- Window functions, CTEs, hash joins, and correlated subqueries

## Examples

```
go run ./examples -db <path> <name>
```

| Name | Source | Demonstrates |
| --- | --- | --- |
| `embed` | `examples/embed.go` | `Open`, `Exec`, `QueryRow` with a parameterized argument |
| `read_only` | `examples/read_only.go` | `SetReadOnly` plus the `ErrReadOnly` sentinel via `errors.Is` |
| `transactions` | `examples/transactions.go` | `Update` closure that commits on nil and rolls back on error |

## Tests

```
go test -count=1 ./...
```

`-count=1` bypasses Go's test cache so every invocation re-runs.

## Benchmarks

All numbers below were captured on `AMD Ryzen 7 3700X` / `Windows amd64` /
`go1.26.0`, and shift on other hardware while the relative shape holds.

### Exec microbenchmarks

```
go test ./internal/exec -bench=. -benchmem -run=^$ -count=10
```

Snapshot (median of 10 runs):

| Bench | ns/op | B/op | allocs/op |
| --- | --- | --- | --- |
| `Filter_Int64Less` | 3176 | 256 | 1 |
| `Filter_Int64Between` | 3824 | 256 | 1 |
| `Filter_Int64AndCompound` | 4428 | 256 | 1 |
| `Filter_Int64Equal` | 2418 | 256 | 1 |
| `Sort_FullAsc_Int64_10k` | 1.43 ms | 252 K | 38 |
| `Sort_FullDesc_Int64_10k` | 1.41 ms | 252 K | 38 |
| `Sort_TopK_Int64_10k_K100` | 158 us | 7.7 K | 17 |
| `Sort_TopK_Int64_100k_K100` | 1.09 ms | 29.6 K | 64 |
| `Sort_TopK_Int64_100k_K100_Off50` | 1.11 ms | 31.0 K | 64 |
| `Sort_TopK_Int64_100k_K100_NullsEvery10` | 1.11 ms | 29.6 K | 64 |
| `Sort_FullAsc_Text_10k` | 3.39 ms | 253 K | 43 |

Streaming top-K stays bounded at `K+Offset` entries regardless of input size.
The text full-sort rides a typed `bytes.Compare` fast path, dropping allocs
from ~30k to under 50 per run.

### Storage microbenchmarks

```
go test ./internal/storage -bench=. -benchmem -run=^$ -count=10
go test ./internal/storage/codec -bench=. -benchmem -run=^$ -count=10
```

Snapshot (median of 10 runs):

| Bench | ns/op | B/op | allocs/op |
| --- | --- | --- | --- |
| `Storage_ScanFull` | 313 us | 467 K | 70 |
| `Storage_ScanEqInt64Hit` | 320 us | 467 K | 80 |
| `Storage_ScanEqInt64Miss` | 648 | 232 | 6 |
| `Storage_ScanEqBytesHit` | 325 us | 465 K | 79 |
| `Storage_WriteSegment_Int64Random` | 2.84 ms | 294 K | 91 |
| `Storage_WriteSegment_Int64Constant` | 2.37 ms | 150 K | 85 |
| `Storage_WriteSegment_Int64Monotonic` | 2.54 ms | 277 K | 98 |
| `Storage_WriteSegment_Int64SparseNulls` | 1.90 ms | 132 K | 53 |
| `Storage_WriteSegment_Float64Plain` | 2.99 ms | 263 K | 81 |
| `Storage_WriteSegment_Float64Decimal` | 3.20 ms | 287 K | 119 |
| `Storage_WriteSegment_TextLowCardinality` | 16.4 ms | 2.99 M | 307 K |
| `Storage_OpenCold` | 81 us | 3.4 K | 20 |
| `Storage_LoadDictHist` | 23 us | 42 K | 18 |
| `Storage_LoadIntFilter` | 24 us | 46 K | 14 |
| `Storage_LoadNumericSums` | 22 us | 42 K | 8 |
| `Codec_Decode/for_int64` | 30 us | 118 K | 16 K |
| `Codec_Decode/delta_int64` | 19 us | 26 K | 33 K |
| `Codec_Decode/pcodec_int64` | 26 us | 74 K | 16 K |
| `Codec_Decode/constant_int64` | 5.6 us | 1.5 K | 0 |
| `Codec_Decode/sequence_int64` | 1.7 us | 9.7 K | 0 |
| `Codec_Decode/plain_int64` | 185 | 104 K | 0 |
| `Codec_Decode/dict_text_lowcard` | 8.8 us | 227 K | 33 K |
| `Codec_Decode/plain_text` | 50 us | 593 K | 55 K |

`Storage_ScanEqInt64Miss` is the page-prune fast path resolving in
microseconds via the Binary Fuse 8 `.bf` sidecar. Sidecar loaders all sit
below 25us and run once per segment open.

### Workload benchmark

```
go run ./cmd/bench -query <name> -rows <N> -runs <R> -mode hot -json
```

`-query` picks one of the cataloged queries from `go run ./cmd/bench list`,
`-rows` sizes the synthetic `users` dataset, `-runs` is the timed-run count,
and `-mode` is one of `hot`, `cold-soft` (closes and reopens the DB between
runs), or `cold-hard` (also drops the OS page cache and needs root or admin).

Snapshot (hot mode, median ms across timed runs):

| Query | Rows | Runs | Median (ms) | io | decode | exec |
| --- | --- | --- | --- | --- | --- | --- |
| `count` | 100k | 200 | ~0 | 0 | 0 | 0 |
| `id_lookup` | 100k | 200 | ~0 | 0.03 | 0.06 | 0 |
| `category_groupby` | 100k | 100 | 5.94 | 1.43 | 2.50 | 2.01 |
| `category_groupby` | 10M | 10 | 608.13 | 118.59 | 569.48 | ~0 |
| `top_age` | 100k | 200 | 2.27 | 0.49 | 1.36 | 0.42 |
| `top_age` | 1M | 50 | 26.46 | 5.69 | 16.12 | 4.64 |
| `top_age` | 10M | 10 | 249.17 | 52.91 | 155.24 | 41.02 |

TPC-H-shaped queries (`tpch_q1`, `tpch_q6`) run against a synthetic `lineitem` with dates stored as int64 days since 1992-01-01 and `l_disc_rev` / `l_disc_price` precomputed because the aggregate-over-expression binder is not wired yet; Q3 (joined `customer` / `orders` / `lineitem`) is a follow-up.

`count` and `id_lookup` short-circuit through the `.sm` numsum sidecar and Binary Fuse 8 page-prune respectively so they resolve without scanning row data, and `category_groupby` reads from the dict-histogram sidecar when no `WHERE` is present.

The harness also prints min, p95, max, mean, and stddev as reference points rather than regression gates.

## CLI

```
go run ./cmd/cli -db <path> -exec "CREATE TABLE events (id INT64 NOT NULL)"
go run ./cmd/cli -db <path> -exec "INSERT INTO events (id) VALUES (1),(2),(42)"
go run ./cmd/cli -db <path> -exec "SELECT count(*) FROM events"
```

Each invocation runs a single SQL string against the required `-db`
directory, which is created automatically if it does not exist.

## License

MIT
