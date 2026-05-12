package codec

import (
	"fmt"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

var decodeBenchSink types.Vec

func BenchmarkPlainDecodeInt64(b *testing.B) {
	page := plainInt64BenchPage(b)
	b.Run("Decode_allocating_baseline", func(b *testing.B) {
		var sink types.Vec
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			v, err := (Plain{}).Decode(page)
			if err != nil {
				b.Fatal(err)
			}
			sink = v
		}
		decodeBenchSink = sink
	})
	b.Run("DecodeInto_reuse", func(b *testing.B) {
		var sink types.Vec
		if err := (Plain{}).DecodeInto(page, &sink); err != nil {
			b.Fatalf("prewarm DecodeInto: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := (Plain{}).DecodeInto(page, &sink); err != nil {
				b.Fatal(err)
			}
		}
		decodeBenchSink = sink
	})
}

func BenchmarkPlainEncodeInt64(b *testing.B) {
	values := make([]int64, types.StandardBatchRows)
	for row := range values {
		values[row] = int64(row)
	}
	v := types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: len(values), I64: values}
	prepared, ok := (Plain{}).Prepare(v)
	if !ok {
		b.Fatal("Plain.Prepare returned !ok")
	}
	scratch := make([]byte, prepared.Size())
	b.Run("EncodeInto_reuse", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := prepared.EncodeInto(scratch); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkDictionaryDecodeText(b *testing.B) {
	page := dictTextBenchPage(b)
	b.Run("Decode_allocating_baseline", func(b *testing.B) {
		var sink types.Vec
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			v, err := (Dictionary{}).Decode(page)
			if err != nil {
				b.Fatal(err)
			}
			sink = v
		}
		decodeBenchSink = sink
	})
	b.Run("DecodeInto_reuse", func(b *testing.B) {
		var sink types.Vec
		if err := (Dictionary{}).DecodeInto(page, &sink); err != nil {
			b.Fatalf("prewarm DecodeInto: %v", err)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := (Dictionary{}).DecodeInto(page, &sink); err != nil {
				b.Fatal(err)
			}
		}
		decodeBenchSink = sink
	})
}

func BenchmarkFORBitPackDecodeInt64(b *testing.B) {
	for _, width := range []int{10, 12, 24, 31} {
		page := forBitPackInt64BenchPage(b, width)
		b.Run(fmt.Sprintf("width_%d", width), func(b *testing.B) {
			var sink types.Vec
			if err := (FORBitPack{}).DecodeInto(page, &sink); err != nil {
				b.Fatalf("prewarm DecodeInto: %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := (FORBitPack{}).DecodeInto(page, &sink); err != nil {
					b.Fatal(err)
				}
			}
			decodeBenchSink = sink
		})
	}
}

func BenchmarkFORBitPackEncodeInt64(b *testing.B) {
	for _, width := range []int{10, 12, 24, 31} {
		v := forBitPackInt64BenchVec(b, width)
		prepared, ok := (FORBitPack{}).Prepare(v)
		if !ok {
			b.Fatalf("FORBitPack.Prepare returned !ok for width %d", width)
		}
		scratch := make([]byte, prepared.Size())
		b.Run(fmt.Sprintf("width_%d", width), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := prepared.EncodeInto(scratch); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkPlainDecodeText(b *testing.B) {
	page := plainTextBenchPage(b)
	var sink types.Vec
	if err := (Plain{}).DecodeInto(page, &sink); err != nil {
		b.Fatalf("prewarm DecodeInto: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := (Plain{}).DecodeInto(page, &sink); err != nil {
			b.Fatal(err)
		}
	}
	decodeBenchSink = sink
}

func BenchmarkPlainEncodeText(b *testing.B) {
	v := plainTextBenchVec(b)
	prepared, ok := (Plain{}).Prepare(v)
	if !ok {
		b.Fatal("Plain.Prepare returned !ok")
	}
	scratch := make([]byte, prepared.Size())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := prepared.EncodeInto(scratch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDictionaryEncodeText(b *testing.B) {
	v := dictTextBenchVec(b)
	prepared, ok := (Dictionary{}).Prepare(v)
	if !ok {
		b.Fatal("Dictionary.Prepare returned !ok")
	}
	scratch := make([]byte, prepared.Size())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := prepared.EncodeInto(scratch); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFlatePrepareText measures the cost of Flate.Prepare since that's
// where the compression actually runs (Flate.EncodeInto just returns the
// pre-compressed payload captured during Prepare). Same shape as the Zstd
// bench below for direct comparison.
func BenchmarkFlatePrepareText(b *testing.B) {
	v := plainTextBenchVec(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prepared, ok := (Flate{}).Prepare(v)
		if !ok {
			b.Fatal("Flate.Prepare returned !ok")
		}
		_ = prepared
	}
}

func BenchmarkZstdPrepareText(b *testing.B) {
	v := plainTextBenchVec(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prepared, ok := (Zstd{}).Prepare(v)
		if !ok {
			b.Fatal("Zstd.Prepare returned !ok")
		}
		_ = prepared
	}
}

func BenchmarkFlateDecodeText(b *testing.B) {
	v := plainTextBenchVec(b)
	page, err := (Flate{}).Encode(v)
	if err != nil {
		b.Fatalf("Encode: %v", err)
	}
	var sink types.Vec
	if err := (Flate{}).DecodeInto(page, &sink); err != nil {
		b.Fatalf("prewarm DecodeInto: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := (Flate{}).DecodeInto(page, &sink); err != nil {
			b.Fatal(err)
		}
	}
	decodeBenchSink = sink
}

func BenchmarkZstdDecodeText(b *testing.B) {
	v := plainTextBenchVec(b)
	page, err := (Zstd{}).Encode(v)
	if err != nil {
		b.Fatalf("Encode: %v", err)
	}
	var sink types.Vec
	if err := (Zstd{}).DecodeInto(page, &sink); err != nil {
		b.Fatalf("prewarm DecodeInto: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := (Zstd{}).DecodeInto(page, &sink); err != nil {
			b.Fatal(err)
		}
	}
	decodeBenchSink = sink
}

// BenchmarkPlainDecodeSelectedInt64 measures Plain.DecodeSelected at several
// selection densities. The current implementation uses sel.IterSet per row;
// dense selections (>50%) might benefit from bulk-decode-then-mask, but only
// if the per-row callback overhead actually dominates here.
func BenchmarkPlainDecodeSelectedInt64(b *testing.B) {
	page := plainInt64BenchPage(b)
	for _, density := range []int{5, 25, 50, 90} {
		sel := densitySelectionMask(b, page.Rows, density)
		b.Run(fmt.Sprintf("density_%dpct", density), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				v, err := (Plain{}).DecodeSelected(page, sel)
				if err != nil {
					b.Fatal(err)
				}
				decodeBenchSink = v
			}
		})
	}
}

func BenchmarkFORBitPackDecodeSelectedInt64(b *testing.B) {
	page := forBitPackInt64BenchPage(b, 12)
	for _, density := range []int{5, 25, 50, 90} {
		sel := densitySelectionMask(b, page.Rows, density)
		b.Run(fmt.Sprintf("density_%dpct", density), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				v, err := (FORBitPack{}).DecodeSelected(page, sel)
				if err != nil {
					b.Fatal(err)
				}
				decodeBenchSink = v
			}
		})
	}
}

// densitySelectionMask returns a SelectionMask of length rows where
// approximately percent% of rows are set, deterministically distributed.
func densitySelectionMask(tb testing.TB, rows int, percent int) types.SelectionMask {
	tb.Helper()
	sel := types.NewSelectionMask(rows)
	step := 100
	for row := 0; row < rows; row++ {
		// Bresenham-style: pick row when its share crosses a percent boundary.
		if ((row+1)*percent)/step != (row*percent)/step {
			sel.Set(row)
		}
	}
	return sel
}

func BenchmarkConstantDecodeInt64(b *testing.B) {
	v := types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: types.StandardBatchRows, I64: make([]int64, types.StandardBatchRows)}
	for i := range v.I64 {
		v.I64[i] = 42
	}
	page, err := (Constant{}).Encode(v)
	if err != nil {
		b.Fatalf("Encode: %v", err)
	}
	var sink types.Vec
	if err := (Constant{}).DecodeInto(page, &sink); err != nil {
		b.Fatalf("prewarm DecodeInto: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := (Constant{}).DecodeInto(page, &sink); err != nil {
			b.Fatal(err)
		}
	}
	decodeBenchSink = sink
}

func plainInt64BenchPage(tb testing.TB) Page {
	tb.Helper()
	values := make([]int64, types.StandardBatchRows)
	for row := range values {
		values[row] = int64(row)
	}
	page, err := (Plain{}).Encode(types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: len(values), I64: values})
	if err != nil {
		tb.Fatalf("Encode: %v", err)
	}
	return page
}

func dictTextBenchVec(tb testing.TB) types.Vec {
	tb.Helper()
	varText := types.NewVarBytes(types.StandardBatchRows, types.StandardBatchRows*8)
	values := []string{"checkout", "login", "signup", "logout"}
	for row := 0; row < types.StandardBatchRows; row++ {
		varText.AppendString(row, values[row&3])
	}
	return types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: types.StandardBatchRows, Var: varText}
}

func dictTextBenchPage(tb testing.TB) Page {
	tb.Helper()
	page, err := (Dictionary{}).Encode(dictTextBenchVec(tb))
	if err != nil {
		tb.Fatalf("Encode: %v", err)
	}
	return page
}

func forBitPackInt64BenchPage(tb testing.TB, width int) Page {
	tb.Helper()
	page, err := (FORBitPack{}).Encode(forBitPackInt64BenchVec(tb, width))
	if err != nil {
		tb.Fatalf("Encode: %v", err)
	}
	if got := int(page.Payload[8]); got != width {
		tb.Fatalf("encoded width = %d, want %d", got, width)
	}
	return page
}

func forBitPackInt64BenchVec(tb testing.TB, width int) types.Vec {
	tb.Helper()
	values := make([]int64, types.StandardBatchRows)
	mask := (uint64(1) << uint(width)) - 1
	base := int64(1000)
	values[0] = base
	values[1] = base + int64(mask)
	for row := 2; row < len(values); row++ {
		values[row] = base + int64((uint64(row)*1315423911)&mask)
	}
	return types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: len(values), I64: values}
}

func plainTextBenchVec(tb testing.TB) types.Vec {
	tb.Helper()
	varText := types.NewVarBytes(types.StandardBatchRows, types.StandardBatchRows*16)
	for row := 0; row < types.StandardBatchRows; row++ {
		varText.AppendString(row, fmt.Sprintf("user-%05d@example.com", row))
	}
	return types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: types.StandardBatchRows, Var: varText}
}

func plainTextBenchPage(tb testing.TB) Page {
	tb.Helper()
	page, err := (Plain{}).Encode(plainTextBenchVec(tb))
	if err != nil {
		tb.Fatalf("Encode: %v", err)
	}
	return page
}
