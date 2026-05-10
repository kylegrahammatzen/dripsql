package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

var scanBenchSink int

func BenchmarkSegmentScanIteratorMemoryPredicate(b *testing.B) {
	for _, tt := range []struct {
		name  string
		batch types.Batch
		pred  Predicate
	}{
		{name: "flat_text_eq", batch: benchmarkTextBatch(b, types.EncodingFlat), pred: Predicate{Column: "event_type", Op: PredicateOpEq, Text: "checkout"}},
		{name: "dict_text_eq", batch: benchmarkTextBatch(b, types.EncodingDictionary), pred: Predicate{Column: "event_type", Op: PredicateOpEq, Text: "checkout"}},
		{name: "compound_and", batch: benchmarkCompoundBatch(b), pred: Predicate{Op: PredicateAnd, Children: []Predicate{
			{Column: "tenant_id", Op: PredicateOpEq, Int64: 42},
			{Column: "event_type", Op: PredicateOpEq, Text: "checkout"},
		}}},
	} {
		b.Run(tt.name, func(b *testing.B) {
			pages := scanBenchmarkPages(tt.batch, 4)
			it := SegmentScanIterator{Segments: []ScanSegment{{Pages: pages}}, Predicate: NewPredicateEvaluator(tt.pred)}
			var sink int
			visit := func(_ types.Batch, sel types.SelectionMask) error {
				sink += sel.PopCount()
				return nil
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := it.ForEach(visit); err != nil {
					b.Fatal(err)
				}
			}
			scanBenchSink = sink
		})
	}
}

func BenchmarkSegmentScanIteratorPrunedMiss(b *testing.B) {
	path := filepath.Join(b.TempDir(), "segment.dsv3")
	meta, err := WriteSegment(path, 41, []types.Batch{
		benchmarkCompoundBatch(b),
		benchmarkCompoundBatch(b),
	})
	if err != nil {
		b.Fatalf("WriteSegment: %v", err)
	}
	infos, err := buildSegmentPageInfos(meta, nil)
	if err != nil {
		b.Fatalf("buildSegmentPageInfos: %v", err)
	}
	size := benchmarkSegmentSize(b, path)
	pred := Predicate{Column: "tenant_id", Op: PredicateOpEq, Int64: 999999}
	it := SegmentScanIterator{Segments: []ScanSegment{{Path: path, Size: size, Meta: meta, PageInfos: infos}}, Predicate: NewPredicateEvaluator(pred), Prune: pred}
	var sink int
	visit := func(_ types.Batch, sel types.SelectionMask) error {
		sink += sel.PopCount()
		return nil
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := it.ForEach(visit); err != nil {
			b.Fatal(err)
		}
	}
	scanBenchSink = sink
}

func BenchmarkSegmentScanIteratorPersistedPredicate(b *testing.B) {
	path := filepath.Join(b.TempDir(), "segment.dsv3")
	pred := Predicate{Column: "event_type", Op: PredicateOpEq, Text: "checkout"}
	meta, err := WriteSegment(path, 42, []types.Batch{
		benchmarkCompoundBatch(b),
		benchmarkCompoundBatch(b),
	})
	if err != nil {
		b.Fatalf("WriteSegment: %v", err)
	}
	infos, err := buildSegmentPageInfos(meta, nil)
	if err != nil {
		b.Fatalf("buildSegmentPageInfos: %v", err)
	}
	size := benchmarkSegmentSize(b, path)
	for _, tt := range []struct {
		name    string
		columns []string
	}{
		{name: "all_columns"},
		{name: "predicate_column", columns: []string{"event_type"}},
	} {
		b.Run(tt.name, func(b *testing.B) {
			cache := newSegmentFileCache()
			defer func() {
				if err := cache.Close(); err != nil {
					b.Fatalf("cache.Close: %v", err)
				}
			}()
			it := SegmentScanIterator{Segments: []ScanSegment{{Path: path, Size: size, Meta: meta, PageInfos: infos}}, Predicate: NewPredicateEvaluator(pred), Prune: pred, OutputColumns: tt.columns, fileCache: cache}
			var sink int
			visit := func(_ types.Batch, sel types.SelectionMask) error {
				sink += sel.PopCount()
				return nil
			}
			if err := it.ForEach(visit); err != nil {
				b.Fatalf("warm ForEach: %v", err)
			}
			sink = 0
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := it.ForEach(visit); err != nil {
					b.Fatal(err)
				}
			}
			scanBenchSink = sink
		})
	}
}

func scanBenchmarkPages(batch types.Batch, count int) []ScanPage {
	pages := make([]ScanPage, count)
	for i := range pages {
		pages[i] = ScanPage{Batch: batch, PayloadBytes: 4096}
	}
	return pages
}

func benchmarkSegmentSize(tb testing.TB, path string) int64 {
	tb.Helper()
	info, err := os.Stat(path)
	if err != nil {
		tb.Fatalf("Stat: %v", err)
	}
	return info.Size()
}
