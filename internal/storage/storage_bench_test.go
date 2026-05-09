package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

var benchCtx = context.Background()

func benchErr(b *testing.B, bytes int64, fn func() error) {
	b.Helper()

	if bytes > 0 {
		b.SetBytes(bytes)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := fn(); err != nil {
			b.Fatal(err)
		}
	}
}

func benchCount(
	b *testing.B,
	rows int,
	bytes int64,
	metric string,
	fn func() (uint64, error),
	check func(uint64),
) {
	b.Helper()

	if _, err := fn(); err != nil {
		b.Fatal(err)
	}

	if bytes > 0 {
		b.SetBytes(bytes)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		count, err := fn()
		if err != nil {
			b.Fatal(err)
		}
		if check != nil {
			check(count)
		}
	}

	if metric != "" && rows > 0 {
		b.ReportMetric(float64(rows)*float64(b.N)/b.Elapsed().Seconds(), metric)
	}
}

func benchGroupStringCounts(
	b *testing.B,
	store *Store,
	table catalog.TableDef,
	colID catalog.ColumnID,
	rows int,
	metric string,
	check func(map[string]uint64),
) {
	b.Helper()

	if _, err := store.GroupStringCounts(benchCtx, table, colID, Predicate{}, nil); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		counts, err := store.GroupStringCounts(benchCtx, table, colID, Predicate{}, nil)
		if err != nil {
			b.Fatal(err)
		}
		if check != nil {
			check(counts)
		}
	}

	if metric != "" && rows > 0 {
		b.ReportMetric(float64(rows)*float64(b.N)/b.Elapsed().Seconds(), metric)
	}
}

func benchScratchCount(
	b *testing.B,
	store *Store,
	table catalog.TableDef,
	pred Predicate,
	rows int,
	bytesPerRow int,
	metric string,
	check func(uint64),
) {
	b.Helper()

	var scratch QueryScratch
	benchCount(b, rows, int64(rows*bytesPerRow), metric, func() (uint64, error) {
		return store.Count(benchCtx, table, pred, &scratch)
	}, check)
}

func benchStoreCount(
	b *testing.B,
	store *Store,
	table catalog.TableDef,
	pred Predicate,
	rows int,
	metric string,
	check func(uint64),
) {
	b.Helper()

	benchCount(b, rows, 0, metric, func() (uint64, error) {
		return store.Count(benchCtx, table, pred, nil)
	}, check)
}

func benchEncodePage(b *testing.B, col vector.Column, rows int) {
	b.Helper()

	payload, _, err := encodeColumnPage(col, 0, rows)
	if err != nil {
		b.Fatal(err)
	}

	benchErr(b, int64(len(payload)), func() error {
		_, _, err := encodeColumnPage(col, 0, rows)
		return err
	})
}

func wantNonZero(b *testing.B) func(uint64) {
	b.Helper()

	return func(count uint64) {
		b.Helper()
		if count == 0 {
			b.Fatal("count = 0")
		}
	}
}

func wantCount(b *testing.B, want uint64) func(uint64) {
	b.Helper()

	return func(count uint64) {
		b.Helper()
		if count != want {
			b.Fatalf("count = %d, want %d", count, want)
		}
	}
}

func wantGroupCount(b *testing.B, want uint64) func(map[string]uint64) {
	b.Helper()

	return func(counts map[string]uint64) {
		b.Helper()
		if counts["event"] != want {
			b.Fatalf("count for event = %d, want %d", counts["event"], want)
		}
	}
}

func BenchmarkAppendInt64TextSegment(b *testing.B) {
	store, table := benchStore(b)
	batch := benchBatch(b, vector.StandardBatchRows)

	benchErr(b, 0, func() error {
		_, err := store.AppendBatch(benchCtx, table, batch)
		return err
	})
}

func BenchmarkAppendInt64TextSegment64Pages(b *testing.B) {
	store, table := benchStore(b)
	batches := benchBatches(b, 64)

	benchErr(b, 0, func() error {
		_, err := store.AppendBatches(benchCtx, table, batches)
		return err
	})
}

func BenchmarkEncodePages(b *testing.B) {
	nulls := make(map[int]bool)
	for row := 0; row < vector.StandardBatchRows; row += 17 {
		nulls[row] = true
	}

	cases := []struct {
		name  string
		batch vector.Batch
		col   int
	}{
		{"int64_all_valid", benchBatch(b, vector.StandardBatchRows), 0},
		{"int64_with_nulls", benchBatchWithNulls(b, vector.StandardBatchRows, nulls), 0},
		{"text_small_strings", benchBatch(b, vector.StandardBatchRows), 1},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			benchEncodePage(b, tc.batch.Columns[tc.col], tc.batch.Len)
		})
	}
}

func BenchmarkScanCountAllMetadata(b *testing.B) {
	store, table := benchStore(b)
	benchAppendNoPruneSegments(b, store, table, 8)

	benchStoreCount(b, store, table, Predicate{}, 0, "", nil)
}

func BenchmarkScanCountNoPrune(b *testing.B) {
	cases := []struct {
		name        string
		makeStore   func(testing.TB) (*Store, catalog.TableDef)
		load        func(*testing.B, *Store, catalog.TableDef, int)
		segments    int
		bytesPerRow int
		pred        func(catalog.TableDef) Predicate
		check       func(*testing.B) func(uint64)
	}{
		{
			name:        "int64_eq",
			makeStore:   benchStore,
			load:        benchAppendNoPruneSegments,
			segments:    8,
			bytesPerRow: 8,
			pred: func(table catalog.TableDef) Predicate {
				return Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int64: 7}
			},
		},
		{
			name:        "int64_between",
			makeStore:   benchStore,
			load:        benchAppendNoPruneSegments,
			segments:    8,
			bytesPerRow: 8,
			pred: func(table catalog.TableDef) Predicate {
				return Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpBetween, Lo: 4, Hi: 12}
			},
		},
		{
			name:        "int64_not_eq",
			makeStore:   benchStore,
			load:        benchAppendNoPruneSegments,
			segments:    8,
			bytesPerRow: 8,
			pred: func(table catalog.TableDef) Predicate {
				return Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpNotEq, Int64: 7}
			},
		},
		{
			name:        "int64_in",
			makeStore:   benchStore,
			load:        benchAppendNoPruneSegments,
			segments:    8,
			bytesPerRow: 8,
			pred: func(table catalog.TableDef) Predicate {
				return Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpIn, Int64s: []int64{3, 7, 11}}
			},
		},
		{
			name:        "int64_not_in",
			makeStore:   benchStore,
			load:        benchAppendNoPruneSegments,
			segments:    8,
			bytesPerRow: 8,
			pred: func(table catalog.TableDef) Predicate {
				return Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpNotIn, Int64s: []int64{3, 7, 11}}
			},
		},
		{
			name:        "int64_less",
			makeStore:   benchStore,
			load:        benchAppendNoPruneSegments,
			segments:    8,
			bytesPerRow: 8,
			pred: func(table catalog.TableDef) Predicate {
				return Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpLess, Int64: 8}
			},
		},
		{
			name:        "int64_greater_equal",
			makeStore:   benchStore,
			load:        benchAppendNoPruneSegments,
			segments:    8,
			bytesPerRow: 8,
			pred: func(table catalog.TableDef) Predicate {
				return Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpGreaterEqual, Int64: 8}
			},
		},
		{
			name:        "int32_eq",
			makeStore:   benchStoreInt32,
			load:        benchAppendNoPruneSegmentsInt32,
			segments:    8,
			bytesPerRow: 4,
			pred: func(table catalog.TableDef) Predicate {
				return Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int32: 7}
			},
		},
		{
			name:        "int32_between",
			makeStore:   benchStoreInt32,
			load:        benchAppendNoPruneSegmentsInt32,
			segments:    8,
			bytesPerRow: 4,
			pred: func(table catalog.TableDef) Predicate {
				return Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpBetween, Lo32: 4, Hi32: 12}
			},
		},
		{
			name:        "text_eq_high_card",
			makeStore:   benchStore,
			load:        benchAppendNoPruneHighCardTextSegments,
			segments:    8,
			bytesPerRow: len("event-0000"),
			pred: func(table catalog.TableDef) Predicate {
				return Predicate{ColumnID: table.Columns[1].ID, Op: PredicateOpEq, Text: "event-7"}
			},
		},
		{
			name:      "text_eq_low_card_metadata",
			makeStore: benchStore,
			load:      benchAppendNoPruneSegments,
			segments:  64,
			pred: func(table catalog.TableDef) Predicate {
				return Predicate{ColumnID: table.Columns[1].ID, Op: PredicateOpEq, Text: "event"}
			},
			check: wantNonZero,
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			store, table := tc.makeStore(b)
			tc.load(b, store, table, tc.segments)

			var check func(uint64)
			if tc.check != nil {
				check = tc.check(b)
			}

			benchScratchCount(
				b,
				store,
				table,
				tc.pred(table),
				tc.segments*vector.StandardBatchRows,
				tc.bytesPerRow,
				"rows/sec",
				check,
			)
		})
	}
}

func BenchmarkGroupStringCountsWherePredicates(b *testing.B) {
	cases := []struct {
		name string
		pred func(catalog.TableDef) Predicate
	}{
		{
			name: "int64_eq",
			pred: func(table catalog.TableDef) Predicate {
				return Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int64: 7}
			},
		},
		{
			name: "int64_not_eq",
			pred: func(table catalog.TableDef) Predicate {
				return Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpNotEq, Int64: 7}
			},
		},
		{
			name: "int64_in",
			pred: func(table catalog.TableDef) Predicate {
				return Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpIn, Int64s: []int64{3, 7, 11}}
			},
		},
		{
			name: "int64_not_in",
			pred: func(table catalog.TableDef) Predicate {
				return Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpNotIn, Int64s: []int64{3, 7, 11}}
			},
		},
		{
			name: "int64_greater_equal",
			pred: func(table catalog.TableDef) Predicate {
				return Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpGreaterEqual, Int64: 8}
			},
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			store, table := benchStore(b)
			benchAppendNoPruneSegments(b, store, table, 8)
			pred := tc.pred(table)
			var scratch QueryScratch
			if _, err := store.GroupStringCounts(benchCtx, table, table.Columns[1].ID, pred, &scratch); err != nil {
				b.Fatal(err)
			}

			rows := 8 * vector.StandardBatchRows
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				counts, err := store.GroupStringCounts(benchCtx, table, table.Columns[1].ID, pred, &scratch)
				if err != nil {
					b.Fatal(err)
				}
				if len(counts) == 0 {
					b.Fatal("counts empty")
				}
			}
			b.ReportMetric(float64(rows)*float64(b.N)/b.Elapsed().Seconds(), "rows/sec")
		})
	}
}

func BenchmarkScanCountAllMetadataBySegments(b *testing.B) {
	for _, segments := range []int{1, 64, 256} {
		b.Run(fmt.Sprintf("segments=%d", segments), func(b *testing.B) {
			store, table := benchStore(b)
			benchAppendNoPruneSegments(b, store, table, segments)

			benchStoreCount(b, store, table, Predicate{}, segments*vector.StandardBatchRows, "rows/sec", nil)
		})
	}
}

func BenchmarkScanCountInt64EqNoPruneBySegments(b *testing.B) {
	for _, segments := range []int{1, 8, 64, 256} {
		b.Run(fmt.Sprintf("segments=%d", segments), func(b *testing.B) {
			store, table := benchStore(b)
			benchAppendNoPruneSegments(b, store, table, segments)
			pred := Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int64: 7}

			benchScratchCount(b, store, table, pred, segments*vector.StandardBatchRows, 8, "rows/sec", nil)
		})
	}
}

func BenchmarkScanCountInt64EqPagePruneRatios(b *testing.B) {
	const pages = 64
	for _, matchedPages := range []int{1, 8, 32, 64} {
		b.Run(fmt.Sprintf("matched_pages=%d", matchedPages), func(b *testing.B) {
			store, table := benchStore(b)
			if _, err := store.AppendBatches(benchCtx, table, benchPagePruneBatches(b, pages, matchedPages)); err != nil {
				b.Fatal(err)
			}
			pred := Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int64: 7}
			b.ReportMetric(float64(pages-matchedPages)/float64(pages), "prune_ratio")

			benchScratchCount(b, store, table, pred, matchedPages*vector.StandardBatchRows, 8, "matched-rows/sec", wantNonZero(b))
		})
	}
}

func BenchmarkScanCountInt64EqWithSegmentPrune(b *testing.B) {
	store, table := benchStore(b)
	benchAppendPrunedSegments(b, store, table, 8)
	pred := Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int64: 7}

	benchScratchCount(b, store, table, pred, vector.StandardBatchRows, 8, "matched-segment-rows/sec", nil)
}

func BenchmarkScanProjectTwoColumnsNoPredicate(b *testing.B) {
	store, table := benchStore(b)
	batches := benchBatches(b, 64)
	if _, err := store.AppendBatches(benchCtx, table, batches); err != nil {
		b.Fatal(err)
	}
	req := ScanRequest{Table: table, Columns: []catalog.ColumnID{table.Columns[0].ID, table.Columns[1].ID}}
	rowsPerScan := 64 * vector.StandardBatchRows
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scanner, err := store.Scan(benchCtx, req)
		if err != nil {
			b.Fatal(err)
		}
		rows := 0
		var batch vector.Batch
		for scanner.Next(&batch) {
			rows += batch.VisibleLen()
		}
		if err := scanner.Err(); err != nil {
			b.Fatal(err)
		}
		if err := scanner.Close(); err != nil {
			b.Fatal(err)
		}
		if rows != rowsPerScan {
			b.Fatalf("rows = %d, want %d", rows, rowsPerScan)
		}
	}
	b.ReportMetric(float64(rowsPerScan*b.N)/b.Elapsed().Seconds(), "rows/sec")
}

func BenchmarkScanProjectTwoColumnsInt64EqPredicate(b *testing.B) {
	store, table := benchStore(b)
	batches := benchBatches(b, 64)
	if _, err := store.AppendBatches(benchCtx, table, batches); err != nil {
		b.Fatal(err)
	}
	pred := Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int64: 7}
	expected, err := store.Count(benchCtx, table, pred, nil)
	if err != nil {
		b.Fatal(err)
	}
	req := ScanRequest{Table: table, Columns: []catalog.ColumnID{table.Columns[0].ID, table.Columns[1].ID}, Predicate: pred}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scanner, err := store.Scan(benchCtx, req)
		if err != nil {
			b.Fatal(err)
		}
		rows := 0
		var batch vector.Batch
		for scanner.Next(&batch) {
			rows += batch.VisibleLen()
		}
		if err := scanner.Err(); err != nil {
			b.Fatal(err)
		}
		if err := scanner.Close(); err != nil {
			b.Fatal(err)
		}
		if uint64(rows) != expected {
			b.Fatalf("rows = %d, want %d", rows, expected)
		}
	}
	b.ReportMetric((float64(expected)*float64(b.N))/b.Elapsed().Seconds(), "matched-rows/sec")
}

func BenchmarkReadDecodeInt64PageOpenClose(b *testing.B) {
	store, table := benchStore(b)
	meta, err := store.AppendBatch(benchCtx, table, benchBatch(b, vector.StandardBatchRows))
	if err != nil {
		b.Fatal(err)
	}
	path := filepath.Join(tableDir(store.root, table), meta.Path)
	col := meta.Columns[0]
	page := col.Pages[0]
	benchErr(b, int64(page.Length), func() error {
		_, err := readColumnPage(path, col, page)
		return err
	})
}

func BenchmarkCountInt64EqDirectPage(b *testing.B) {
	store, table := benchStore(b)
	meta, err := store.AppendBatch(benchCtx, table, benchBatch(b, vector.StandardBatchRows))
	if err != nil {
		b.Fatal(err)
	}
	path := filepath.Join(tableDir(store.root, table), meta.Path)
	file, err := os.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	col := meta.Columns[0]
	page := col.Pages[0]
	pred := Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int64: 7}
	var scratch pageScratch
	benchCount(b, int(page.Rows), int64(page.Length), "rows/sec", func() (uint64, error) {
		count, err := countInt64PredicatePage(file, col, page, pred, &scratch)
		return uint64(count), err
	}, wantNonZero(b))
}

func BenchmarkCountTextEqDirectPage(b *testing.B) {
	store, table := benchStore(b)
	meta, err := store.AppendBatch(benchCtx, table, benchBatch(b, vector.StandardBatchRows))
	if err != nil {
		b.Fatal(err)
	}
	path := filepath.Join(tableDir(store.root, table), meta.Path)
	file, err := os.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	col := meta.Columns[1]
	page := col.Pages[0]
	var scratch pageScratch
	benchCount(b, int(page.Rows), int64(page.Length), "rows/sec", func() (uint64, error) {
		count, err := countTextEqPage(file, col, page, "event", &scratch)
		return uint64(count), err
	}, wantNonZero(b))
}

func BenchmarkCountInt64EqPayloadOnlyAllValid(b *testing.B) {
	store, table := benchStore(b)
	meta, err := store.AppendBatch(benchCtx, table, benchBatch(b, vector.StandardBatchRows))
	if err != nil {
		b.Fatal(err)
	}
	path := filepath.Join(tableDir(store.root, table), meta.Path)
	file, err := os.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	col := meta.Columns[0]
	page := col.Pages[0]
	var scratch pageScratch
	payload := scratch.pagePayload(int(page.Length))
	if _, err := file.ReadAt(payload, int64(page.Offset)); err != nil {
		b.Fatal(err)
	}
	rows := int(page.Rows)
	pred := Predicate{Op: PredicateOpEq, Int64: 7}
	benchCount(b, rows, int64(len(payload)), "rows/sec", func() (uint64, error) {
		count, err := countInt64PredicatePayload(payload, nil, rows, pred)
		return uint64(count), err
	}, wantNonZero(b))
}

func BenchmarkCountInt64EqPayloadOnlyWithNulls(b *testing.B) {
	store, table := benchStore(b)
	nulls := make(map[int]bool)
	for row := 0; row < vector.StandardBatchRows; row += 17 {
		nulls[row] = true
	}
	meta, err := store.AppendBatch(benchCtx, table, benchBatchWithNulls(b, vector.StandardBatchRows, nulls))
	if err != nil {
		b.Fatal(err)
	}
	path := filepath.Join(tableDir(store.root, table), meta.Path)
	file, err := os.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	page := meta.Columns[0].Pages[0]
	var scratch pageScratch
	payload := scratch.pagePayload(int(page.Length))
	if _, err := file.ReadAt(payload, int64(page.Offset)); err != nil {
		b.Fatal(err)
	}
	rows := int(page.Rows)
	validLen := vector.ValidityWords(rows) * 8
	valid := payload[:validLen]
	values := payload[validLen:]
	pred := Predicate{Op: PredicateOpEq, Int64: 7}
	benchCount(b, rows, int64(len(values)), "rows/sec", func() (uint64, error) {
		count, err := countInt64PredicatePayload(values, valid, rows, pred)
		return uint64(count), err
	}, wantNonZero(b))
}

func BenchmarkCountInt32EqPayloadOnly(b *testing.B) {
	store, table := benchStoreInt32(b)
	meta, err := store.AppendBatch(benchCtx, table, benchInt32Batch(b, vector.StandardBatchRows))
	if err != nil {
		b.Fatal(err)
	}
	path := filepath.Join(tableDir(store.root, table), meta.Path)
	file, err := os.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	page := meta.Columns[0].Pages[0]
	var scratch pageScratch
	payload := scratch.pagePayload(int(page.Length))
	if _, err := file.ReadAt(payload, int64(page.Offset)); err != nil {
		b.Fatal(err)
	}
	rows := int(page.Rows)
	pred := Predicate{Op: PredicateOpEq, Int32: 7}
	benchCount(b, rows, int64(len(payload)), "rows/sec", func() (uint64, error) {
		count, err := countInt32PredicatePayload(payload, nil, rows, pred)
		return uint64(count), err
	}, wantNonZero(b))
}

func BenchmarkCountInt64BetweenPayloadOnly(b *testing.B) {
	store, table := benchStore(b)
	meta, err := store.AppendBatch(benchCtx, table, benchBatch(b, vector.StandardBatchRows))
	if err != nil {
		b.Fatal(err)
	}
	path := filepath.Join(tableDir(store.root, table), meta.Path)
	file, err := os.Open(path)
	if err != nil {
		b.Fatal(err)
	}
	defer file.Close()
	page := meta.Columns[0].Pages[0]
	var scratch pageScratch
	payload := scratch.pagePayload(int(page.Length))
	if _, err := file.ReadAt(payload, int64(page.Offset)); err != nil {
		b.Fatal(err)
	}
	rows := int(page.Rows)
	pred := Predicate{Op: PredicateOpBetween, Lo: 4, Hi: 12}
	benchCount(b, rows, int64(len(payload)), "rows/sec", func() (uint64, error) {
		count, err := countInt64PredicatePayload(payload, nil, rows, pred)
		return uint64(count), err
	}, wantNonZero(b))
}

func BenchmarkReadDecodeTextPageOpenClose(b *testing.B) {
	store, table := benchStore(b)
	meta, err := store.AppendBatch(benchCtx, table, benchBatch(b, vector.StandardBatchRows))
	if err != nil {
		b.Fatal(err)
	}
	path := filepath.Join(tableDir(store.root, table), meta.Path)
	col := meta.Columns[1]
	page := col.Pages[0]
	benchErr(b, int64(page.Length), func() error {
		_, err := readColumnPage(path, col, page)
		return err
	})
}

func BenchmarkAppendStageEncode64Pages(b *testing.B) {
	_, table := benchStore(b)
	batches := benchBatches(b, 64)
	encodedBytes, err := encodeBatchesForBenchmark(table, batches)
	if err != nil {
		b.Fatal(err)
	}
	benchErr(b, int64(encodedBytes), func() error {
		_, err := encodeBatchesForBenchmark(table, batches)
		return err
	})
}

func BenchmarkAppendStageWriteSegment64Pages(b *testing.B) {
	store, table := benchStore(b)
	if err := ensureTableDirs(store.root, table); err != nil {
		b.Fatal(err)
	}
	batches := benchBatches(b, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := writeSegmentBatches(benchCtx, store.root, table, batches, SegmentID(i+1)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendStageManifestOnly(b *testing.B) {
	store, table := benchStore(b)
	if err := ensureTableDirs(store.root, table); err != nil {
		b.Fatal(err)
	}
	dir := tableDir(store.root, table)
	meta := SegmentMeta{TableID: table.ID, SchemaVersion: table.Version, Path: "segments/0000000000000001.dseg", Rows: DefaultSegmentRows, PageRows: DefaultPageRows}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		meta.ID = SegmentID(i + 1)
		if err := appendManifest(dir, meta); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendStageAccumulateBatchRefs64Pages(b *testing.B) {
	source := benchBatches(b, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batches := make([]vector.Batch, 0, len(source))
		batches = append(batches, source...)
		if len(batches) != len(source) {
			b.Fatal(len(batches))
		}
	}
}

func BenchmarkAppendStageAccumulateOwnedColumns64Pages(b *testing.B) {
	source := benchBatches(b, 64)
	b.SetBytes(int64(64 * vector.StandardBatchRows * (8 + len("event"))))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batches := cloneBatchesForBuffer(source)
		if len(batches) != len(source) {
			b.Fatal(len(batches))
		}
	}
}

func BenchmarkIngestBufferAppendOwned64PagesNoFlush(b *testing.B) {
	store, table := benchStore(b)
	source := benchBatches(b, 64)
	benchErr(b, int64(64*vector.StandardBatchRows*(8+len("event"))), func() error {
		buffer, err := store.NewIngestBuffer(table, IngestBufferOptions{TargetRows: 2 * DefaultSegmentRows})
		if err != nil {
			return err
		}
		for _, batch := range source {
			published, err := buffer.Append(benchCtx, batch)
			if err != nil {
				return err
			}
			if len(published) != 0 {
				return fmt.Errorf("published %d segments before target", len(published))
			}
		}
		if buffer.BufferedRows() != DefaultSegmentRows {
			return fmt.Errorf("buffered rows = %d, want %d", buffer.BufferedRows(), DefaultSegmentRows)
		}
		return nil
	})
}

func BenchmarkIngestBufferAppendFlush64Pages(b *testing.B) {
	store, table := benchStore(b)
	source := benchBatches(b, 64)
	benchErr(b, 0, func() error {
		buffer, err := store.NewIngestBuffer(table, IngestBufferOptions{})
		if err != nil {
			return err
		}
		publishedCount := 0
		for _, batch := range source {
			published, err := buffer.Append(benchCtx, batch)
			if err != nil {
				return err
			}
			publishedCount += len(published)
		}
		if publishedCount != 1 || buffer.BufferedRows() != 0 {
			return fmt.Errorf("published=%d buffered=%d", publishedCount, buffer.BufferedRows())
		}
		return nil
	})
}

func BenchmarkAppendBufferedInt64TextSegmentNoPublish(b *testing.B) {
	store, table := benchStore(b)
	benchInstallLargeBufferedTarget(b, store, table)
	batch := benchBatch(b, vector.StandardBatchRows)
	benchErr(b, int64(vector.StandardBatchRows*(8+len("event"))), func() error {
		return store.AppendBuffered(benchCtx, table, batch)
	})
	b.StopTimer()
	benchDiscardBufferedRows(b, store, table)
	b.ReportMetric(float64(vector.StandardBatchRows*b.N)/b.Elapsed().Seconds(), "rows/sec")
}

func BenchmarkCountHotBufferAllMetadata(b *testing.B) {
	store, table := benchStore(b)
	benchInstallLargeBufferedTarget(b, store, table)
	for _, batch := range benchBatches(b, 8) {
		if err := store.AppendBuffered(benchCtx, table, batch); err != nil {
			b.Fatal(err)
		}
	}
	benchStoreCount(b, store, table, Predicate{}, 8*vector.StandardBatchRows, "rows/sec", wantCount(b, 8*uint64(vector.StandardBatchRows)))
	b.StopTimer()
	benchDiscardBufferedRows(b, store, table)
}

func BenchmarkCountHotBufferInt64Eq(b *testing.B) {
	store, table := benchStore(b)
	benchInstallLargeBufferedTarget(b, store, table)
	for _, batch := range benchBatches(b, 8) {
		if err := store.AppendBuffered(benchCtx, table, batch); err != nil {
			b.Fatal(err)
		}
	}
	pred := Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int64: 7}
	benchStoreCount(b, store, table, pred, 8*vector.StandardBatchRows, "rows/sec", wantNonZero(b))
	b.StopTimer()
	benchDiscardBufferedRows(b, store, table)
}

func BenchmarkCountHotBufferInt64EqWithNulls(b *testing.B) {
	store, table := benchStore(b)
	benchInstallLargeBufferedTarget(b, store, table)
	nulls := make(map[int]bool)
	for row := 0; row < vector.StandardBatchRows; row += 17 {
		nulls[row] = true
	}
	for i := 0; i < 8; i++ {
		batch := benchBatchWithNulls(b, vector.StandardBatchRows, nulls)
		if err := store.AppendBuffered(benchCtx, table, batch); err != nil {
			b.Fatal(err)
		}
	}
	pred := Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int64: 7}
	benchStoreCount(b, store, table, pred, 8*vector.StandardBatchRows, "rows/sec", wantNonZero(b))
	b.StopTimer()
	benchDiscardBufferedRows(b, store, table)
}

func BenchmarkSumInt64AllValid(b *testing.B) {
	store, table := benchStore(b)
	benchAppendNoPruneSegments(b, store, table, 8)
	colID := table.Columns[0].ID
	var scratch QueryScratch
	if _, err := store.SumInt(benchCtx, table, colID, Predicate{}, &scratch); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(8 * vector.StandardBatchRows * 8))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.SumInt(benchCtx, table, colID, Predicate{}, &scratch); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(8*vector.StandardBatchRows)*float64(b.N)/b.Elapsed().Seconds(), "rows/sec")
}

func BenchmarkSumInt64Filtered(b *testing.B) {
	store, table := benchStore(b)
	benchAppendNoPruneSegments(b, store, table, 8)
	colID := table.Columns[0].ID
	pred := Predicate{ColumnID: colID, Op: PredicateOpBetween, Lo: 4, Hi: 12}
	var scratch QueryScratch
	if _, err := store.SumInt(benchCtx, table, colID, pred, &scratch); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(8 * vector.StandardBatchRows * 8))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.SumInt(benchCtx, table, colID, pred, &scratch); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(8*vector.StandardBatchRows)*float64(b.N)/b.Elapsed().Seconds(), "rows/sec")
}

func BenchmarkSumInt64FilteredDifferentColumn(b *testing.B) {
	store, table := benchStore(b)
	benchAppendNoPruneSegments(b, store, table, 8)
	colID := table.Columns[0].ID
	pred := Predicate{ColumnID: table.Columns[1].ID, Op: PredicateOpEq, Text: "event"}
	var scratch QueryScratch
	if _, err := store.SumInt(benchCtx, table, colID, pred, &scratch); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(8 * vector.StandardBatchRows * 8))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := store.SumInt(benchCtx, table, colID, pred, &scratch); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(8*vector.StandardBatchRows)*float64(b.N)/b.Elapsed().Seconds(), "rows/sec")
}

func BenchmarkGroupStringCountsHotSegments(b *testing.B) {
	store, table := benchStore(b)
	for _, batch := range benchBatches(b, 8) {
		if _, err := store.AppendBatch(benchCtx, table, batch); err != nil {
			b.Fatal(err)
		}
	}
	benchGroupStringCounts(b, store, table, table.Columns[1].ID, 8*vector.StandardBatchRows, "rows/sec", wantGroupCount(b, 8*uint64(vector.StandardBatchRows)))
}

func BenchmarkGroupStringCountsHotBufferOnly(b *testing.B) {
	store, table := benchStore(b)
	benchInstallLargeBufferedTarget(b, store, table)
	for _, batch := range benchBatches(b, 8) {
		if err := store.AppendBuffered(benchCtx, table, batch); err != nil {
			b.Fatal(err)
		}
	}
	benchGroupStringCounts(b, store, table, table.Columns[1].ID, 8*vector.StandardBatchRows, "rows/sec", wantGroupCount(b, 8*uint64(vector.StandardBatchRows)))
	b.StopTimer()
	benchDiscardBufferedRows(b, store, table)
}

func BenchmarkGroupStringCountsPersistedPlusBuffer(b *testing.B) {
	store, table := benchStore(b)
	benchAppendNoPruneSegments(b, store, table, 4)
	benchInstallLargeBufferedTarget(b, store, table)
	for _, batch := range benchBatches(b, 4) {
		if err := store.AppendBuffered(benchCtx, table, batch); err != nil {
			b.Fatal(err)
		}
	}
	benchGroupStringCounts(
		b,
		store,
		table,
		table.Columns[1].ID,
		8*vector.StandardBatchRows,
		"rows/sec",
		wantGroupCount(b, 8*uint64(vector.StandardBatchRows)),
	)
	b.StopTimer()
	benchDiscardBufferedRows(b, store, table)
}

func benchStore(tb testing.TB) (*Store, catalog.TableDef) {
	tb.Helper()
	store, table := newTestStore(tb)
	return store, table
}

func benchInstallLargeBufferedTarget(tb testing.TB, store *Store, table catalog.TableDef) {
	tb.Helper()
	state, err := store.appendTableState(table)
	if err != nil {
		tb.Fatal(err)
	}
	buffer, err := store.NewIngestBuffer(table, IngestBufferOptions{TargetRows: 1 << 30})
	if err != nil {
		tb.Fatal(err)
	}
	state.bufferMu.Lock()
	state.buffer = buffer
	state.bufferMu.Unlock()
}

func benchDiscardBufferedRows(tb testing.TB, store *Store, table catalog.TableDef) {
	tb.Helper()
	state, err := store.ensureTableState(table)
	if err != nil {
		tb.Fatal(err)
	}
	state.bufferMu.Lock()
	state.buffer = nil
	state.pendingRuns = nil
	state.bufferMu.Unlock()
}

func benchStoreInt32(tb testing.TB) (*Store, catalog.TableDef) {
	tb.Helper()
	store, table := newTestStoreInt32(tb)
	return store, table
}

func benchBatch(tb testing.TB, rows int) vector.Batch {
	tb.Helper()
	values := make([]int64, rows)
	texts := make([]string, rows)
	for row := range values {
		values[row] = int64(row % 17)
		texts[row] = "event"
	}
	return makeIntTextBatch(tb, values, texts, nil)
}

func benchHighCardTextBatch(tb testing.TB, rows int) vector.Batch {
	tb.Helper()
	values := make([]int64, rows)
	texts := make([]string, rows)
	for row := range values {
		values[row] = int64(row % 17)
		texts[row] = fmt.Sprintf("event-%d", row)
	}
	return makeIntTextBatch(tb, values, texts, nil)
}

func benchBatchWithNulls(tb testing.TB, rows int, nulls map[int]bool) vector.Batch {
	tb.Helper()
	values := make([]int64, rows)
	texts := make([]string, rows)
	for row := range values {
		values[row] = int64(row % 17)
		texts[row] = "event"
	}
	return makeIntTextBatch(tb, values, texts, nulls)
}

func benchInt32Batch(tb testing.TB, rows int) vector.Batch {
	tb.Helper()
	values := make([]int32, rows)
	texts := make([]string, rows)
	for row := range values {
		values[row] = int32(row % 17)
		texts[row] = "event"
	}
	return makeInt32TextBatch(tb, values, texts, nil)
}

func benchBatches(tb testing.TB, count int) []vector.Batch {
	tb.Helper()
	batches := make([]vector.Batch, 0, count)
	for i := 0; i < count; i++ {
		batches = append(batches, benchBatch(tb, vector.StandardBatchRows))
	}
	return batches
}

func benchPagePruneBatches(tb testing.TB, pages int, matchedPages int) []vector.Batch {
	tb.Helper()
	batches := make([]vector.Batch, 0, pages)
	for page := 0; page < pages; page++ {
		values := make([]int64, vector.StandardBatchRows)
		texts := make([]string, vector.StandardBatchRows)
		for row := range values {
			if page < matchedPages {
				values[row] = int64(row % 17)
			} else {
				values[row] = int64(10_000 + page*vector.StandardBatchRows + row)
			}
			texts[row] = "event"
		}
		batches = append(batches, makeIntTextBatch(tb, values, texts, nil))
	}
	return batches
}

func benchAppendNoPruneSegments(b *testing.B, store *Store, table catalog.TableDef, segments int) {
	b.Helper()
	batch := benchBatch(b, vector.StandardBatchRows)
	for i := 0; i < segments; i++ {
		if _, err := store.AppendBatch(benchCtx, table, batch); err != nil {
			b.Fatal(err)
		}
	}
}

func benchAppendNoPruneHighCardTextSegments(b *testing.B, store *Store, table catalog.TableDef, segments int) {
	b.Helper()
	batch := benchHighCardTextBatch(b, vector.StandardBatchRows)
	for i := 0; i < segments; i++ {
		if _, err := store.AppendBatch(benchCtx, table, batch); err != nil {
			b.Fatal(err)
		}
	}
}

func benchAppendPrunedSegments(b *testing.B, store *Store, table catalog.TableDef, segments int) {
	b.Helper()
	for i := 0; i < segments; i++ {
		values := make([]int64, vector.StandardBatchRows)
		texts := make([]string, vector.StandardBatchRows)
		for row := range values {
			values[row] = int64(i*10_000 + row)
			texts[row] = "event"
		}
		_, err := store.AppendBatch(benchCtx, table, makeIntTextBatch(b, values, texts, nil))
		if err != nil {
			b.Fatal(err)
		}
	}
}

func benchAppendNoPruneSegmentsInt32(b *testing.B, store *Store, table catalog.TableDef, segments int) {
	b.Helper()
	batch := benchInt32Batch(b, vector.StandardBatchRows)
	for i := 0; i < segments; i++ {
		if _, err := store.AppendBatch(benchCtx, table, batch); err != nil {
			b.Fatal(err)
		}
	}
}

func encodeBatchesForBenchmark(table catalog.TableDef, batches []vector.Batch) (int, error) {
	encodedBytes := 0
	for colIndex := range table.Columns {
		for _, batch := range batches {
			batchCol := batch.Columns[colIndex]
			for start := 0; start < batch.Len; start += DefaultPageRows {
				rows := min(DefaultPageRows, batch.Len-start)
				payload, _, err := encodeColumnPage(batchCol, start, rows)
				if err != nil {
					return 0, err
				}
				encodedBytes += len(payload)
			}
		}
	}
	return encodedBytes, nil
}
