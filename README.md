# DripSQL

Experimental single-node SQL analytics engine for exploring columnar storage,
compression, and memory-efficient query execution. APIs and storage formats
will change.

## Layout

- `cmd/cli` is the single-shot SQL runner (`-db <path> -exec <sql>`).
- `cmd/bench` is the workload benchmark driver.
- `internal/engine` owns DB lifecycle, plan dispatch, INSERT/UPDATE/DELETE, the query runner, and the segment open cache.
- `internal/storage` owns immutable columnar segments, the manifest with atomic transaction records, deletion vectors, and predicate plus top-K page pruning.
- `internal/sql` owns the lexer, parser, binder, unified `Plan` + `Rel` IR, and plan cache.
- `internal/exec` owns the pull-based vector operators (scan, filter, project, aggregate, sort, limit, hash join).
- `internal/schema` owns table specs, column types, encoding enums, and name normalization.
- `internal/vector` owns typed vectors, batches, validity bitmaps, selection masks, and kernel routines.

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

## Feature coverage

What the engine currently supports versus what's still on the list:

| Area | Status | Notes |
| --- | --- | --- |
| Columnar segments, per-column + per-page min/max | yes | |
| Codecs (plain, dictionary, constant, sequence, FOR+BitPack, delta+BitPack, Flate, Zstd, ALP, ALP-RD, FSST) | yes | Cascade chooses by encoded size; ALP for decimal floats, ALP-RD for irrational/sci floats, FSST for long/repetitive varbytes |
| 15 types (numeric, text, bytes, UUID, date/time/timestamp, JSON, enum) | yes | |
| Validity bitmaps, null-aware filter/sort/aggregate | yes | |
| SQL parser, binder, plan + rel IR, plan cache | yes | |
| Pull-based vectorized operators (scan/filter/aggregate/project/sort/limit) | yes | |
| Hash join (inner/left/right/full) | yes | maphash-keyed probe |
| INSERT, UPDATE, DELETE, BulkInsert | yes | UPDATE/DELETE atomic via versioned DV + manifest transaction record |
| Multi-statement transactions (`BeginTx` / Exec / Query / Commit / Rollback) | yes | Single-writer; reads inside the txn see staged adds/DV overlays |
| Cross-table atomic commit | yes | One (TxnID, CommitTs) per `Tx.Commit`; recovery forward-rolls partial groups |
| MVCC snapshot reads | yes | `runQuery` pins `read_ts` at statement start; segment-level visibility via `ScanOpts.ReadTs` |
| Time travel (`DB.QueryAt`, SQL `... FROM t AS OF <commit_ts>`) | yes | Per-table-reference snapshot in joins |
| Retention vacuum | yes | `DB.VacuumRetention(cutoff)`; pin registry blocks retiring snapshots in use; honors max(seg.CommitTs, DV-out CommitTs) |
| WAL with append-only framing + truncate-after-commit | yes | CRC32 per record; orphan cleanup + forward-roll on `Open` |
| Top-K page pruning (single-column ORDER BY ... LIMIT) | yes | Sound: only prunes pages strictly dominated by others |
| Streaming top-K with bounded heap | yes | `K+Offset` items, no `O(N)` materialization |
| Segment handle cache | yes | DB-level LRU bounded at 256, keyed by `(path, dvPath)` |
| EXPLAIN / EXPLAIN ANALYZE | yes | Per-operator wall + rows + calls tree |
| Parallel per-segment scans | yes | `ScanOp.Parallelism = GOMAXPROCS` when >1 segment and no TopK pushdown |
| Bloom skip filters (int columns) | yes | In-house ~10 bits/key, k=8 in `.bf` sidecar |
| Cross-column SMA (sum/count from metadata alone) | yes | `tryMetadataAggregate` consults `.sm` numsum sidecar |
| SELECT DISTINCT (single or multi-column) | yes | Lowers to GROUP BY over the named columns; rides the composite-key aggregate path |
| Multi-column GROUP BY | yes | Composite-key path encodes (col1, col2, ...) per row, single-col stays on the typed fast paths |
| CASE WHEN ... THEN ... [ELSE ...] END | yes | Searched form. Branches must share a kind. |
| Configurable retention via DB.SetAutoRetention / DB.SetRetentionLag | yes | AutoRetention + RetentionLag makes DB.Vacuum also retire fully-DV'd-out segments below the cutoff |
| JSON path operators (`->`, `->>`) | yes | `->` returns JSON, `->>` returns Text; both wired through Project |
| CTEs (`WITH ... AS`) | yes | Plan substitution; CTE name resolves to inner Rel at plan time |
| Scalar / `IN` / `EXISTS` subqueries | yes | Uncorrelated subqueries materialized once at BuildOperator |
| Correlated subqueries | yes | Outer column refs bind to parent scope; inner plan rebuilt per outer row with values threaded through a shared carrier |
| Qualified column refs (`tbl.col`, `alias.col`) | yes | Resolve in single-table and joined scopes |
| Window functions | yes | `ROW_NUMBER/RANK/DENSE_RANK` and aggregate `SUM/COUNT/MIN/MAX/AVG OVER (PARTITION BY ... ORDER BY ...)`; `ROWS` and `RANGE BETWEEN` frame clauses |
| Predicate-on-encoded execution | yes | FOR-bitpacked int64 equality + LT/GT/BETWEEN, delta-bitpack int64 equality, dictionary-text equality + LT/GT/LE/GE all run on encoded bytes via a per-page accept mask |
| Lazy footer / sidecar / validation decode | yes | PageStats, sidecars, and `validateColumnDirectory` all defer to first access |
| Page-level varbytes pruning | yes | Per-page bloom filter sidecar (`.tbf`), FNV-1a-64 keyed; consulted by `boundEqBytes.PrunePage` after the segment-level dict-hist check |
| Compaction / vacuum (for DV path) | yes | `DB.Compact` rewrites half-or-more-deleted segments; `DB.Vacuum` drops unreferenced DV files |
| User-declared codecs in DDL | yes | `CREATE TABLE ... col WITH (codec = 'dictionary' | ... | 'fsst')` overrides cascade |
| Bench harness (TPC-H Q1/Q6, ClickBench Q1/Q4/Q5/Q7/Q9, SQLsmith fuzz, CI) | yes | Synthetic loaders for `lineitem` and `hits` plus a deterministic SELECT fuzzer; GitHub Actions runs build/vet/test + smoke bench |
| UNION / UNION ALL | yes | Parser + planner lower to `RelUnion`; UNION distinct wraps in GROUP BY over all output columns for dedup |
| Selective Late Materialization | yes | `Scan` decodes predicate-needed columns first, evaluates the filter, then decodes projection-only columns only when the page survives |
| Pcodec (chunked FOR + bitpack) | yes | Splits FOR-packable pages into 1024-row sub-chunks each with its own base + bit-width; cascade picks it when multimodal distributions beat single-base FOR |
| Operator fusion pass | yes | `fusePlan` collapses stacked `RelFilter` nodes into one `AND`-merged filter post-bind |

## License

MIT
