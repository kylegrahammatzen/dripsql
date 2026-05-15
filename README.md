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
- `internal/types` owns table specs, typed batches, vectors, validity bitmaps, and selection masks.

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
go test ./internal/exec -bench=. -benchmem -run=^$ -count=1
```

Snapshot:

| Bench | ns/op | B/op | allocs/op |
| --- | --- | --- | --- |
| `Filter_Int64Less` | 2841 | 256 | 1 |
| `Filter_Int64Between` | 3465 | 256 | 1 |
| `Filter_Int64AndCompound` | 5247 | 512 | 2 |
| `Filter_Int64Equal` | 2650 | 256 | 1 |
| `Sort_FullAsc_Int64_10k` | 1.54 ms | 252 K | 38 |
| `Sort_FullDesc_Int64_10k` | 1.43 ms | 252 K | 38 |
| `Sort_TopK_Int64_10k_K100` | 149 us | 7.7 K | 17 |
| `Sort_TopK_Int64_100k_K100` | 989 us | 29.6 K | 64 |
| `Sort_TopK_Int64_100k_K100_Off50` | 1.03 ms | 31.0 K | 64 |
| `Sort_TopK_Int64_100k_K100_NullsEvery10` | 1.02 ms | 29.6 K | 64 |
| `Sort_FullAsc_Text_10k` | 6.72 ms | 2.48 MB | 30 059 |

Streaming top-K stays bounded at `K+Offset` regardless of N. The text full-sort
is the known slow path. It goes through a boxed `any` comparator. No production
workload exercises it today.

### Workload benchmark

```
go run ./cmd/bench -query <name> -rows <N> -runs <R> -mode hot -json
```

Flags: `-query` picks one of the cataloged queries (`go run ./cmd/bench list`),
`-rows` sizes the synthetic `users` dataset, `-runs` is the timed-run count,
`-mode` is one of `hot`, `cold-soft` (close+reopen DB between runs), or
`cold-hard` (also drops the OS page cache, needs root/admin).

Snapshot (hot mode, median ms across timed runs):

| Query | Rows | Runs | Median (ms) | io | decode | exec |
| --- | --- | --- | --- | --- | --- | --- |
| `count` | 100k | 200 | 5.96 | 1.37 | 3.32 | 1.28 |
| `id_lookup` | 100k | 200 | 5.02 | 1.09 | 2.89 | 1.03 |
| `category_groupby` | 100k | 100 | 11.77 | 1.05 | 1.73 | 9.00 |
| `category_groupby` | 10M | 10 | 1129 | 93.95 | 149.15 | 886.02 |
| `top_age` | 100k | 200 | 6.37 | 1.20 | 3.52 | 1.64 |
| `top_age` | 1M | 50 | 50.07 | 9.76 | 29.48 | 10.84 |
| `top_age` | 10M | 10 | 514.60 | 97.58 | 285.38 | 131.64 |

The harness also prints min / p95 / max / mean / stddev. These are reference
points, not regression gates.

## CLI

```
go run ./cmd/cli -db <path> -exec "CREATE TABLE events (id INT64 NOT NULL)"
go run ./cmd/cli -db <path> -exec "INSERT INTO events (id) VALUES (1),(2),(42)"
go run ./cmd/cli -db <path> -exec "SELECT count(*) FROM events"
```

One SQL string per invocation. `-db` is required. The directory is created
if missing.

## Feature coverage

What the engine currently supports versus what's still on the list:

| Area | Status | Notes |
| --- | --- | --- |
| Columnar segments, per-column + per-page min/max | yes | |
| Codecs (plain, dictionary, constant, FOR+BitPack, delta+BitPack, Flate, Zstd) | yes | Cascade chooses by encoded size |
| 15 types (numeric, text, bytes, UUID, date/time/timestamp, JSON, enum) | yes | |
| Validity bitmaps, null-aware filter/sort/aggregate | yes | |
| SQL parser, binder, plan + rel IR, plan cache | yes | |
| Pull-based vectorized operators (scan/filter/aggregate/project/sort/limit) | yes | |
| Hash join (inner/left/right/full) | yes | maphash-keyed probe |
| INSERT, UPDATE, DELETE | yes | UPDATE/DELETE atomic via versioned DV + manifest transaction record |
| Top-K page pruning (single-column ORDER BY ... LIMIT) | yes | Sound: only prunes pages strictly dominated by others |
| Streaming top-K with bounded heap | yes | `K+Offset` items, no `O(N)` materialization |
| Segment handle cache | yes | DB-level, keyed by `(path, dvPath)` |
| EXPLAIN / EXPLAIN ANALYZE | no | Parser accepts it but engine rejects with `Query supports SELECT only` |
| JSON path operators (`->`, `->>`) | partial | Parsed and bound but exec not wired |
| Window functions, subqueries, CTEs | no | Not in grammar |
| Bloom / Xor / Ribbon skip filters | no | |
| Cross-column SMA (count/sum from metadata alone) | no | |
| Parallel per-segment scans | no | |
| Predicate-on-encoded execution | no | Predicates evaluate post-decode |
| Lazy column-metadata decode | no | Full footer parsed at segment open |
| Compaction / vacuum (for DV path) | no | UPDATE/DELETE already atomic, but DV files accumulate |
| WAL, recovery, MVCC | no | Manifest lines carry a 32-bit checksum but no row-level durability story exists |
| User-declared codecs in DDL | no | |

## License

MIT
