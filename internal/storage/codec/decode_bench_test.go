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

func dictTextBenchPage(tb testing.TB) Page {
	tb.Helper()
	varText := types.NewVarBytes(types.StandardBatchRows, types.StandardBatchRows*8)
	values := []string{"checkout", "login", "signup", "logout"}
	for row := 0; row < types.StandardBatchRows; row++ {
		varText.AppendString(row, values[row&3])
	}
	page, err := (Dictionary{}).Encode(types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: types.StandardBatchRows, Var: varText})
	if err != nil {
		tb.Fatalf("Encode: %v", err)
	}
	return page
}

func forBitPackInt64BenchPage(tb testing.TB, width int) Page {
	tb.Helper()
	values := make([]int64, types.StandardBatchRows)
	mask := (uint64(1) << uint(width)) - 1
	base := int64(1000)
	values[0] = base
	values[1] = base + int64(mask)
	for row := 2; row < len(values); row++ {
		values[row] = base + int64((uint64(row)*1315423911)&mask)
	}
	page, err := (FORBitPack{}).Encode(types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: len(values), I64: values})
	if err != nil {
		tb.Fatalf("Encode: %v", err)
	}
	if got := int(page.Payload[8]); got != width {
		tb.Fatalf("encoded width = %d, want %d", got, width)
	}
	return page
}
