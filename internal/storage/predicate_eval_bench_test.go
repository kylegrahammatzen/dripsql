package storage

import (
	"fmt"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

var predicateBenchSink int

func BenchmarkDictTextEqVsPlain(b *testing.B) {
	flat := benchmarkTextBatch(b, types.EncodingFlat)
	dict := benchmarkTextBatch(b, types.EncodingDictionary)
	pred := Predicate{Column: "event_type", Op: PredicateOpEq, PredicateValue: PredicateValue{Text: "checkout"}}
	flatEval := BindPredicate(pred)
	dictEval := BindPredicate(pred)
	b.Run("flat", func(b *testing.B) {
		var sink int
		sel := types.NewSelectionMask(flat.Len)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			matched, err := flatEval.Eval(flat, &sel)
			if err != nil {
				b.Fatal(err)
			}
			sink += matched
		}
		predicateBenchSink = sink
	})
	b.Run("dict", func(b *testing.B) {
		var sink int
		sel := types.NewSelectionMask(dict.Len)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			matched, err := dictEval.Eval(dict, &sel)
			if err != nil {
				b.Fatal(err)
			}
			sink += matched
		}
		predicateBenchSink = sink
	})
}

func BenchmarkCompoundPredicateReusedMask(b *testing.B) {
	batch := benchmarkCompoundBatch(b)
	eval := BindPredicate(Predicate{Op: PredicateAnd, Children: []Predicate{
		{Column: "tenant_id", Op: PredicateOpEq, PredicateValue: PredicateValue{Int64: 42}},
		{Column: "event_type", Op: PredicateOpEq, PredicateValue: PredicateValue{Text: "checkout"}},
	}})
	sel := types.NewSelectionMask(batch.Len)
	var sink int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matched, err := eval.Eval(batch, &sel)
		if err != nil {
			b.Fatal(err)
		}
		sink += matched
	}
	predicateBenchSink = sink
}

func BenchmarkPredicateEvalSelectedSparse(b *testing.B) {
	batch := benchmarkCompoundBatch(b)
	eval := BindPredicate(Predicate{Op: PredicateAnd, Children: []Predicate{
		{Column: "tenant_id", Op: PredicateOpEq, PredicateValue: PredicateValue{Int64: 42}},
		{Column: "event_type", Op: PredicateOpEq, PredicateValue: PredicateValue{Text: "checkout"}},
	}})
	input := types.NewSelectionMask(batch.Len)
	for row := 0; row < batch.Len; row += 64 {
		input.Set(row)
	}
	b.Run("full_then_intersect", func(b *testing.B) {
		sel := types.NewSelectionMask(batch.Len)
		var sink int
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			matched, err := eval.Eval(batch, &sel)
			if err != nil {
				b.Fatal(err)
			}
			if matched != 0 {
				matched = sel.AndCount(input)
			}
			sink += matched
		}
		predicateBenchSink = sink
	})
	b.Run("selected", func(b *testing.B) {
		sel := types.NewSelectionMask(batch.Len)
		var sink int
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			matched, err := eval.EvalSelected(batch, input, &sel)
			if err != nil {
				b.Fatal(err)
			}
			sink += matched
		}
		predicateBenchSink = sink
	})
}

func BenchmarkPredicateInt64EqFull(b *testing.B) {
	batch := benchmarkInt64Batch(b)
	eval := BindPredicate(Predicate{Column: "tenant_id", Op: PredicateOpEq, PredicateValue: PredicateValue{Int64: 42}})
	sel := types.NewSelectionMask(batch.Len)
	if _, err := eval.Eval(batch, &sel); err != nil {
		b.Fatalf("prewarm Eval: %v", err)
	}
	var sink int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matched, err := eval.Eval(batch, &sel)
		if err != nil {
			b.Fatal(err)
		}
		sink += matched
	}
	predicateBenchSink = sink
}

func BenchmarkPredicateInt64EqSelectedSparse(b *testing.B) {
	batch := benchmarkInt64Batch(b)
	eval := BindPredicate(Predicate{Column: "tenant_id", Op: PredicateOpEq, PredicateValue: PredicateValue{Int64: 42}})
	input := types.NewSelectionMask(batch.Len)
	for row := 0; row < batch.Len; row += 64 {
		input.Set(row)
	}
	sel := types.NewSelectionMask(batch.Len)
	if _, err := eval.EvalSelected(batch, input, &sel); err != nil {
		b.Fatalf("prewarm EvalSelected: %v", err)
	}
	var sink int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matched, err := eval.EvalSelected(batch, input, &sel)
		if err != nil {
			b.Fatal(err)
		}
		sink += matched
	}
	predicateBenchSink = sink
}

func BenchmarkPredicateFORBitPackEqFull(b *testing.B) {
	batch := benchmarkFORBitPackBatch(b)
	eval := BindPredicate(Predicate{Column: "tenant_id", Op: PredicateOpEq, PredicateValue: PredicateValue{Int64: 42}})
	sel := types.NewSelectionMask(batch.Len)
	if _, err := eval.Eval(batch, &sel); err != nil {
		b.Fatalf("prewarm Eval: %v", err)
	}
	var sink int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matched, err := eval.Eval(batch, &sel)
		if err != nil {
			b.Fatal(err)
		}
		sink += matched
	}
	predicateBenchSink = sink
}

func BenchmarkPredicateInt64InSmall(b *testing.B) {
	batch := benchmarkInt64Batch(b)
	eval := BindPredicate(Predicate{Column: "tenant_id", Op: PredicateOpIn, PredicateValue: PredicateValue{Int64s: []int64{7, 42, 99, 1234}}})
	sel := types.NewSelectionMask(batch.Len)
	if _, err := eval.Eval(batch, &sel); err != nil {
		b.Fatalf("prewarm Eval: %v", err)
	}
	var sink int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matched, err := eval.Eval(batch, &sel)
		if err != nil {
			b.Fatal(err)
		}
		sink += matched
	}
	predicateBenchSink = sink
}

func BenchmarkPredicateInt64InLarge(b *testing.B) {
	batch := benchmarkInt64Batch(b)
	values := make([]int64, 128)
	for i := range values {
		values[i] = int64(i * 3)
	}
	eval := BindPredicate(Predicate{Column: "tenant_id", Op: PredicateOpIn, PredicateValue: PredicateValue{Int64s: values}})
	sel := types.NewSelectionMask(batch.Len)
	if _, err := eval.Eval(batch, &sel); err != nil {
		b.Fatalf("prewarm Eval: %v", err)
	}
	var sink int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matched, err := eval.Eval(batch, &sel)
		if err != nil {
			b.Fatal(err)
		}
		sink += matched
	}
	predicateBenchSink = sink
}

func BenchmarkPredicateTextInFlatLarge(b *testing.B) {
	batch := benchmarkTextDistinctBatch(b, 128)
	values := make([]string, 64)
	for i := range values {
		values[i] = fmt.Sprintf("event_%03d", i*2)
	}
	eval := BindPredicate(Predicate{Column: "event_type", Op: PredicateOpIn, PredicateValue: PredicateValue{Texts: values}})
	sel := types.NewSelectionMask(batch.Len)
	if _, err := eval.Eval(batch, &sel); err != nil {
		b.Fatalf("prewarm Eval: %v", err)
	}
	var sink int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		matched, err := eval.Eval(batch, &sel)
		if err != nil {
			b.Fatal(err)
		}
		sink += matched
	}
	predicateBenchSink = sink
}

func BenchmarkPredicatePruneBound(b *testing.B) {
	meta := benchmarkPruneMeta(16, 256)
	pred := Predicate{Column: "col_15", Op: PredicateOpBetween, PredicateValue: PredicateValue{Lo: 1000, Hi: 1400}}
	plan := BindPrunePredicate(pred, meta)
	var sink int
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for page := range meta.Columns[0].Pages {
			if plan.PageCandidate(page) {
				sink++
			}
		}
	}
	predicateBenchSink = sink
}

func benchmarkTextBatch(tb testing.TB, enc types.Encoding) types.Batch {
	tb.Helper()
	const rows = types.StandardBatchRows
	values := []string{"checkout", "login", "signup", "logout"}
	var vec types.Vec
	switch enc {
	case types.EncodingFlat:
		varText := types.NewVarBytes(rows, rows*8)
		for row := range rows {
			varText.AppendString(row, values[row&3])
		}
		vec = types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: rows, Var: varText}
	case types.EncodingDictionary:
		dict := types.NewVarBytes(len(values), 32)
		for i, value := range values {
			dict.AppendString(i, value)
		}
		ids := make([]uint8, rows)
		for row := range rows {
			ids[row] = uint8(row & 3)
		}
		vec = types.Vec{Kind: types.VecText, Encoding: types.EncodingDictionary, Len: rows, Encoded: &types.EncodedState{DictIDs: ids, DictValues: dict}}
	default:
		tb.Fatalf("unsupported encoding %s", enc)
	}
	batch, err := types.NewBatch([]types.Column{{Name: "event_type", Type: types.Text, V: vec}})
	if err != nil {
		tb.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func benchmarkTextDistinctBatch(tb testing.TB, distinct int) types.Batch {
	tb.Helper()
	const rows = types.StandardBatchRows
	values := make([]string, distinct)
	for i := range values {
		values[i] = fmt.Sprintf("event_%03d", i)
	}
	varText := types.NewVarBytes(rows, rows*9)
	for row := range rows {
		varText.AppendString(row, values[row%len(values)])
	}
	batch, err := types.NewBatch([]types.Column{{Name: "event_type", Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: rows, Var: varText}}})
	if err != nil {
		tb.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func benchmarkInt64Batch(tb testing.TB) types.Batch {
	tb.Helper()
	const rows = types.StandardBatchRows
	values := make([]int64, rows)
	for row := range values {
		values[row] = int64(row & 255)
	}
	batch, err := types.NewBatch([]types.Column{{Name: "tenant_id", Type: types.Int64, V: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: rows, I64: values}}})
	if err != nil {
		tb.Fatalf("NewBatch: %v", err)
	}
	return batch
}

// benchmarkFORBitPackBatch builds an int64 column encoded as FORBitPack so
// the predicate hot path exercises the bit-extraction fast path. The values
// fit in width 9 (matches structured-profile tenant_id range).
func benchmarkFORBitPackBatch(tb testing.TB) types.Batch {
	tb.Helper()
	const rows = types.StandardBatchRows
	const base int64 = 0
	const width = 9
	values := make([]int64, rows)
	for row := range values {
		values[row] = int64(row & 511)
	}
	bytes := (rows*width + 7) / 8
	data := make([]byte, bytes)
	for row, value := range values {
		offset := uint64(value - base)
		for bit := range width {
			if offset&(uint64(1)<<uint(bit)) == 0 {
				continue
			}
			absoluteBit := row*width + bit
			data[absoluteBit>>3] |= byte(1) << uint(absoluteBit&7)
		}
	}
	vec := types.Vec{
		Kind:     types.VecInt64,
		Encoding: types.EncodingFORBitPack,
		Len:      rows,
		Encoded:  &types.EncodedState{FORBase: base, FORWidth: width, FORData: data},
	}
	batch, err := types.NewBatch([]types.Column{{Name: "tenant_id", Type: types.Int64, V: vec}})
	if err != nil {
		tb.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func benchmarkPruneMeta(columns int, pages int) SegmentMeta {
	meta := SegmentMeta{Rows: uint32(pages * types.StandardBatchRows), PageRows: types.StandardBatchRows, Columns: make([]ColumnMeta, columns)}
	for col := range meta.Columns {
		colMeta := ColumnMeta{Column: types.Column{Name: fmt.Sprintf("col_%02d", col), Type: types.Int64}, Rows: meta.Rows, AllValid: true, Pages: make([]PageMeta, pages)}
		for page := range colMeta.Pages {
			stats := &Int64Stats{Min: int64(page * 10), Max: int64(page*10 + 9)}
			colMeta.Pages[page] = PageMeta{RowStart: uint32(page * types.StandardBatchRows), Rows: types.StandardBatchRows, Kind: types.VecInt64, Encoding: types.EncodingFlat, AllValid: true, Int64: stats}
		}
		meta.Columns[col] = colMeta
	}
	return meta
}

func benchmarkCompoundBatch(tb testing.TB) types.Batch {
	tb.Helper()
	const rows = types.StandardBatchRows
	values := []string{"checkout", "login", "signup", "logout"}
	tenantIDs := make([]int64, rows)
	varText := types.NewVarBytes(rows, rows*8)
	for row := range rows {
		if row&1 == 0 {
			tenantIDs[row] = 42
		} else {
			tenantIDs[row] = int64(row)
		}
		varText.AppendString(row, values[row&3])
	}
	batch, err := types.NewBatch([]types.Column{
		{Name: "tenant_id", Type: types.Int64, V: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: rows, I64: tenantIDs}},
		{Name: "event_type", Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: rows, Var: varText}},
	})
	if err != nil {
		tb.Fatalf("NewBatch: %v", err)
	}
	return batch
}
