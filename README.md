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
go run ./cmd/bench -runs 3 -profile structured
```

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
