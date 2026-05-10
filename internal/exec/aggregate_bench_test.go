package exec

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

var aggregateBenchSink any

func BenchmarkSumInt64AllRows(b *testing.B) {
	batch := aggregateInt64BenchBatch(b)
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	sink := &SumInt64Sink{Column: "amount"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sink.Consume(batch, sel); err != nil {
			b.Fatal(err)
		}
	}
	aggregateBenchSink = sink.Sum
}

func BenchmarkSumInt64Sparse(b *testing.B) {
	batch := aggregateInt64BenchBatch(b)
	sel := types.NewSelectionMask(batch.Len)
	for row := 0; row < batch.Len; row += 64 {
		sel.Set(row)
	}
	sink := &SumInt64Sink{Column: "amount"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sink.Consume(batch, sel); err != nil {
			b.Fatal(err)
		}
	}
	aggregateBenchSink = sink.Sum
}

func BenchmarkGroupStringFlat(b *testing.B) {
	batch := aggregateFlatTextBenchBatch(b)
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	sink := &GroupStringCountSink{Column: "event_type"}
	if err := sink.Consume(batch, sel); err != nil {
		b.Fatalf("prewarm Consume: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sink.Consume(batch, sel); err != nil {
			b.Fatal(err)
		}
	}
	aggregateBenchSink = sink.Counts
}

func BenchmarkGroupStringDict(b *testing.B) {
	batch := aggregateDictTextBenchBatch(b)
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	sink := &GroupStringCountSink{Column: "event_type"}
	if err := sink.Consume(batch, sel); err != nil {
		b.Fatalf("prewarm Consume: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sink.Consume(batch, sel); err != nil {
			b.Fatal(err)
		}
	}
	aggregateBenchSink = sink.Counts
}

func aggregateInt64BenchBatch(tb testing.TB) types.Batch {
	tb.Helper()
	values := make([]int64, types.StandardBatchRows)
	for row := range values {
		values[row] = int64(row & 3)
	}
	return execInt64Batch(tb, values, nil)
}

func aggregateFlatTextBenchBatch(tb testing.TB) types.Batch {
	tb.Helper()
	values := make([]string, types.StandardBatchRows)
	dict := []string{"checkout", "login", "signup", "logout"}
	for row := range values {
		values[row] = dict[row&3]
	}
	return execTextBatch(tb, values, nil)
}

func aggregateDictTextBenchBatch(tb testing.TB) types.Batch {
	tb.Helper()
	dict := types.NewVarBytes(4, 32)
	for row, value := range []string{"checkout", "login", "signup", "logout"} {
		dict.AppendString(row, value)
	}
	ids := make([]uint8, types.StandardBatchRows)
	for row := range ids {
		ids[row] = uint8(row & 3)
	}
	batch, err := types.NewBatch([]types.Column{{Name: "event_type", Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingDictionary, Len: len(ids), DictIDs: ids, DictValues: dict}}})
	if err != nil {
		tb.Fatalf("NewBatch: %v", err)
	}
	return batch
}
