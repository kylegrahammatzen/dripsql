# DripSQL

Experimental single-node SQL analytics engine for exploring columnar storage,
compression, and memory-efficient query execution. APIs and storage formats
will change.

## Layout

- `cmd/cli` — admin shell (`version`, `exec`, `query`).
- `cmd/bench` — workload benchmark driver.
- `internal/engine` — DB lifecycle, query/exec, EXPLAIN.
- `internal/explain` — report types and renderers.
- `internal/storage` — immutable columnar segments, predicate pushdown.
- `internal/sql` — parser, binder, logical plan.
- `internal/types` — table specs, typed batches, and vectors.

## Tests

```
go test -count=1 ./...
```

`-count=1` bypasses Go's test cache so every invocation re-runs.

## Benchmarks

All benchmark commands assume `AMD Ryzen 7 3700X` / `Windows amd64`. Numbers shift on other hardware; the relative shape (and the comparisons in tables below) holds.

Use longer `-benchtime` and higher `-count` for noisy paths.

### Codec benchmarks (single page, 2048 rows)

```
go test ./internal/storage/codec -run ^$ -bench . -benchmem -benchtime=1s -count=1
```

Snapshot (2026-05-11):

| Bench | ns/op | allocs |
| --- | --- | --- |
| `PlainDecodeInt64/DecodeInto_reuse` | 165 | 0 |
| `PlainEncodeInt64/EncodeInto_reuse` | 309 | 0 |
| `PlainDecodeText` | 913 | 0 |
| `PlainEncodeText` | 1,461 | 0 |
| `DictionaryDecodeText/DecodeInto_reuse` | 48 | 0 |
| `DictionaryEncodeText` (`EncodeInto` only) | 78 | 0 |
| `FORBitPackDecodeInt64` (widths 10-31) | 1,300-2,600 | 0 |
| `FORBitPackEncodeInt64` (widths 10-31) | ~9,000 | 0 |
| `ConstantDecodeInt64` | 964 | 0 |
| `FlatePrepareText` (klauspost) | 395,000 | 24 |
| `FlateDecodeText` (klauspost) | 258,000 | 71 |
| `ZstdPrepareText` | 224,000 | 4 |
| `ZstdDecodeText` | 81,000 | 1 |

### Segment scan benchmarks (131,072 rows, full segment)

```
go test ./internal/storage -run ^$ -bench BenchmarkStorageReadSegmentDecode -benchtime=3s -count=1
```

Snapshot (2026-05-11):

| Bench | ns/op | rows/s | allocs/op |
| --- | --- | --- | --- |
| `all_columns` | 21.7 ms | 6.0 M | 796 |
| `flat_text` (high-cardinality, compressed) | 20.2 ms | 6.5 M | 798 |
| `encoded_numeric` (FOR/Constant) | 1.9 ms | 70 M | 128 |
| `dictionary_text` | 1.0 ms | 129 M | 128 |

### Workload benchmark

End-to-end query workload via the bench driver:

```
go run ./cmd/bench -rows 10000000 -segment-rows 2097152 -profile structured -runs 50
```

Snapshot (2026-05-12), `structured` profile, 10M rows, 2M-row segments, `sort_by = 'tenant_id'`:

| Query | best | strategy |
| --- | --- | --- |
| `SELECT count(*) WHERE event_type = 'checkout'` | 45 µs | text summary + metadata count |
| `SELECT event_type, count(*) GROUP BY event_type` | 5 µs | metadata group |
| `SELECT status, count(*) GROUP BY status` | 4 µs | metadata group |
| `SELECT count(*) WHERE path = '/checkout/confirm'` | 27 µs | text summary prune |
| `SELECT count(*), sum(amount), min(amount), max(amount)` | 6 µs | metadata aggregates |
| `SELECT country, count(*), sum(amount) GROUP BY country` | 4 µs | per-segment SMA |
| `SELECT count(*) WHERE tenant_id = 42 AND event_type = 'checkout'` | 168 µs | min/max + value prune (sort_by) |
| `SELECT count(*) WHERE tenant_id = 999999` | 3 µs | min/max + value prune (sort_by) |
| `SELECT sum(amount) WHERE tenant_id = 42 AND event_type = 'checkout'` | 168 µs | min/max + value prune (sort_by) |
| `SELECT country, count(*) WHERE event_type = 'checkout' GROUP BY country` | 6 µs | cross-count SMA |
| `SELECT count(*) WHERE user_id = 778` | 170 µs | min/max + value prune (int bloom) |
| `SELECT count(*) WHERE created_at = ...` | 168 µs | min/max + value prune (int bloom) |
| `SELECT count(*) WHERE event_uuid = '...'` | 174 µs | uuid summary prune |
| `SELECT count(*) WHERE url = '...'` | 253 µs | text summary prune |

Every query is sub-millisecond on this profile.

Pass `-mode cold-ish` to close and reopen the database between every sample, so each measurement starts with an empty in-process state. On this profile cold-ish for `user id lookup` lands at **150 ms best / 168 ms avg** — within the open path, not the query path. The cost decomposes into reading and parsing the manifest, then loading per-segment footers in parallel; the predicate-evaluation work after that stays in the microsecond range observed warm. OS file cache is not dropped (no portable way without admin), so this still measures "first query after process restart" rather than truly cold disk.

Full usage in [`cmd/bench/README.md`](cmd/bench/README.md). The driver exercises segment build, predicate pushdown, and aggregate execution end-to-end.

These are reference points, not regression gates. Re-baseline with `-count=5` or higher for any comparison work.

## CLI

```
go run ./cmd/cli version
go run ./cmd/cli exec <db> "CREATE TABLE events (id INT64 NOT NULL)"
go run ./cmd/cli query <db> "SELECT count(*) FROM events"
go run ./cmd/cli query <db> "EXPLAIN ANALYZE SELECT count(*) FROM events WHERE id = 42"
```

## License

MIT
