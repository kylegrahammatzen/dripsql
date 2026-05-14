package types

import "testing"

func BenchmarkEqInt64AllValidNoAlloc(b *testing.B) {
	x := make([]int64, StandardBatchRows)
	for i := range x {
		x[i] = int64(i & 7)
	}
	out := make(Sel, 0, len(x))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = EqInt64(x, nil, nil, out[:0], 3)
	}
}

func BenchmarkSumInt64AllValidNoAlloc(b *testing.B) {
	x := make([]int64, StandardBatchRows)
	for i := range x {
		x[i] = int64(i & 7)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _ = SumInt64(x, nil, nil)
	}
}

func BenchmarkCountTrueNoAlloc(b *testing.B) {
	bits := make([]uint64, ValidityWords(StandardBatchRows))
	for i := range bits {
		bits[i] = ^uint64(0)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = CountTrue(bits, StandardBatchRows, nil, nil)
	}
}
