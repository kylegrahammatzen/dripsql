package kernel

import (
	"math"
	"slices"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

var benchSel vector.Sel
var benchMin int64
var benchMax int64
var benchOK bool
var benchCount int

func TestEqInt64(t *testing.T) {
	values := []int64{1, 2, 3, 2, 4}
	out := EqInt64(values, nil, nil, make(vector.Sel, 0, len(values)), 2)
	if !slices.Equal(out, vector.Sel{1, 3}) {
		t.Fatalf("EqInt64 no validity = %#v", out)
	}

	valid := vector.NewValidity(len(values))
	vector.SetInvalid(valid, 3)
	out = EqInt64(values, valid, nil, out, 2)
	if !slices.Equal(out, vector.Sel{1}) {
		t.Fatalf("EqInt64 with validity = %#v", out)
	}

	out = EqInt64(values, nil, vector.Sel{4, 3, 1}, out, 2)
	if !slices.Equal(out, vector.Sel{3, 1}) {
		t.Fatalf("EqInt64 with selection = %#v", out)
	}

	out = EqInt64(values, valid, vector.Sel{3, 1, 0}, out, 2)
	if !slices.Equal(out, vector.Sel{1}) {
		t.Fatalf("EqInt64 with validity and selection = %#v", out)
	}
}

func TestBetweenInt64(t *testing.T) {
	values := []int64{9, 1, 4, 7, 2}
	valid := vector.NewValidity(len(values))
	vector.SetInvalid(valid, 0)
	sel := BetweenInt64(values, valid, vector.Sel{0, 1, 2, 3, 4}, make(vector.Sel, 0, len(values)), 2, 7)
	if !slices.Equal(sel, vector.Sel{2, 3, 4}) {
		t.Fatalf("BetweenInt64 = %#v", sel)
	}

	sel = BetweenInt64(values, valid, vector.Sel{0, 1, 2}, sel, 1, 9)
	if !slices.Equal(sel, vector.Sel{1, 2}) {
		t.Fatalf("BetweenInt64 with validity and selection = %#v", sel)
	}
}

func TestMinMaxInt64(t *testing.T) {
	values := []int64{9, 1, 4, 7, 2}
	min, max, ok := MinMaxInt64(values, nil, nil)
	if !ok || min != 1 || max != 9 {
		t.Fatalf("MinMaxInt64 no selection = %d/%d/%v", min, max, ok)
	}

	min, max, ok = MinMaxInt64(values, nil, vector.Sel{0, 3, 4})
	if !ok || min != 2 || max != 9 {
		t.Fatalf("MinMaxInt64 selection only = %d/%d/%v", min, max, ok)
	}

	valid := vector.NewValidity(len(values))
	vector.SetInvalid(valid, 0)

	min, max, ok = MinMaxInt64(values, valid, nil)
	if !ok || min != 1 || max != 7 {
		t.Fatalf("MinMaxInt64 with validity = %d/%d/%v", min, max, ok)
	}

	sel := vector.Sel{2, 3, 4}
	min, max, ok = MinMaxInt64(values, valid, sel)
	if !ok || min != 2 || max != 7 {
		t.Fatalf("MinMaxInt64 with selection = %d/%d/%v", min, max, ok)
	}
}

func TestFloat64KernelsHandleNaNAndInvalidRanges(t *testing.T) {
	values := []float64{1, math.NaN(), 3, 4}
	valid := vector.NewValidity(len(values))
	vector.SetInvalid(valid, 1)

	out := EqFloat64(values, nil, nil, make(vector.Sel, 0, len(values)), math.NaN())
	if len(out) != 0 {
		t.Fatalf("EqFloat64 NaN = %#v, want empty", out)
	}

	out = EqFloat64(values, valid, vector.Sel{1, 2, 3}, out, 3)
	if !slices.Equal(out, vector.Sel{2}) {
		t.Fatalf("EqFloat64 with validity and selection = %#v", out)
	}

	out = BetweenFloat64(values, valid, nil, out, math.NaN(), 4)
	if len(out) != 0 {
		t.Fatalf("BetweenFloat64 NaN bound = %#v, want empty", out)
	}

	out = BetweenFloat64(values, nil, nil, out, 5, 4)
	if len(out) != 0 {
		t.Fatalf("BetweenFloat64 inverted range = %#v, want empty", out)
	}

	out = BetweenFloat64(values, valid, vector.Sel{1, 2, 3}, out, 2, 4)
	if !slices.Equal(out, vector.Sel{2, 3}) {
		t.Fatalf("BetweenFloat64 with validity and selection = %#v", out)
	}
}

func TestVarBytesKernels(t *testing.T) {
	v := vector.VarBytes{Offsets: []uint32{0, 3, 6, 9, 12}, Data: []byte("foobarbazfoo")}
	valid := vector.NewValidity(4)
	vector.SetInvalid(valid, 3)
	out := EqBytes(v, valid, nil, make(vector.Sel, 0, 4), []byte("foo"))
	if !slices.Equal(out, vector.Sel{0}) {
		t.Fatalf("EqBytes = %#v", out)
	}
	out = PrefixBytes(v, nil, vector.Sel{1, 2, 3}, out, []byte("ba"))
	if !slices.Equal(out, vector.Sel{1, 2}) {
		t.Fatalf("PrefixBytes = %#v", out)
	}
	out = PrefixBytes(v, valid, nil, out, nil)
	if !slices.Equal(out, vector.Sel{0, 1, 2}) {
		t.Fatalf("PrefixBytes empty prefix = %#v", out)
	}
}

func TestBoolKernelsMaskTrailingBits(t *testing.T) {
	bits := []uint64{^uint64(0)}
	out := IsTrue(bits, 3, nil, nil, make(vector.Sel, 0, 3))
	if !slices.Equal(out, vector.Sel{0, 1, 2}) {
		t.Fatalf("IsTrue = %#v", out)
	}
	if got := CountTrue(bits, 3, nil, nil); got != 3 {
		t.Fatalf("CountTrue = %d, want 3", got)
	}
}

func TestBoolKernelsSelectionAndValidity(t *testing.T) {
	bits := []uint64{0b101101}
	valid := vector.NewValidity(6)
	vector.SetInvalid(valid, 2)
	out := IsTrue(bits, 6, valid, nil, make(vector.Sel, 0, 6))
	if !slices.Equal(out, vector.Sel{0, 3, 5}) {
		t.Fatalf("IsTrue with validity = %#v", out)
	}
	if got := CountTrue(bits, 6, valid, nil); got != 3 {
		t.Fatalf("CountTrue with validity = %d, want 3", got)
	}

	sel := vector.Sel{5, 4, 3, 2, 1, 0}
	out = IsTrue(bits, 6, valid, sel, out)
	if !slices.Equal(out, vector.Sel{5, 3, 0}) {
		t.Fatalf("IsTrue with validity and selection = %#v", out)
	}
	if got := CountTrue(bits, 6, valid, sel); got != 3 {
		t.Fatalf("CountTrue with validity and selection = %d, want 3", got)
	}

	out = IsTrue(bits, 6, nil, sel, out)
	if !slices.Equal(out, vector.Sel{5, 3, 2, 0}) {
		t.Fatalf("IsTrue with selection = %#v", out)
	}
	if got := CountTrue(bits, 6, nil, sel); got != 4 {
		t.Fatalf("CountTrue with selection = %d, want 4", got)
	}
}

func TestAndOrBoolMaskTrailingBits(t *testing.T) {
	and := AndBool(nil, []uint64{^uint64(0)}, []uint64{0b101 | 1<<63}, 3)
	if len(and) != 1 || and[0] != 0b101 {
		t.Fatalf("AndBool = %#v, want 0b101", and)
	}
	or := OrBool(nil, []uint64{0b001}, []uint64{0b010 | 1<<63}, 3)
	if len(or) != 1 || or[0] != 0b011 {
		t.Fatalf("OrBool = %#v, want 0b011", or)
	}
}

func TestTake(t *testing.T) {
	values := []int64{10, 20, 30}
	got := TakeInt64(nil, values, vector.Sel{2, 0})
	if !slices.Equal(got, []int64{30, 10}) {
		t.Fatalf("TakeInt64 = %#v", got)
	}
	floats := []float64{1.5, 2.5, 3.5}
	gotFloats := TakeFloat64(nil, floats, vector.Sel{2, 0})
	if !slices.Equal(gotFloats, []float64{3.5, 1.5}) {
		t.Fatalf("TakeFloat64 = %#v", gotFloats)
	}

	v := vector.VarBytes{Offsets: []uint32{0, 3, 6, 9}, Data: []byte("foobarbaz")}
	taken := TakeVarBytes(vector.VarBytes{}, v, vector.Sel{2, 0})
	if string(taken.Bytes(0)) != "baz" || string(taken.Bytes(1)) != "foo" {
		t.Fatalf("TakeVarBytes = %#v %q", taken.Offsets, taken.Data)
	}
}

func BenchmarkEqInt64NoNullNoSel(b *testing.B) {
	values := make([]int64, vector.StandardBatchRows)
	for i := range values {
		values[i] = int64(i % 17)
	}
	out := make(vector.Sel, 0, len(values))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = EqInt64(values, nil, nil, out, 7)
	}
	benchSel = out
}

func BenchmarkEqInt64NoMatches(b *testing.B) {
	values := make([]int64, vector.StandardBatchRows)
	for i := range values {
		values[i] = int64(i)
	}
	out := make(vector.Sel, 0, len(values))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = EqInt64(values, nil, nil, out, -1)
	}
	benchSel = out
}

func BenchmarkEqInt64AllMatches(b *testing.B) {
	values := make([]int64, vector.StandardBatchRows)
	for i := range values {
		values[i] = 7
	}
	out := make(vector.Sel, 0, len(values))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = EqInt64(values, nil, nil, out, 7)
	}
	benchSel = out
}

func BenchmarkEqInt64OnePercentMatches(b *testing.B) {
	values := make([]int64, vector.StandardBatchRows)
	for i := range values {
		if i%100 == 0 {
			values[i] = 7
		} else {
			values[i] = int64(i + 1000)
		}
	}
	out := make(vector.Sel, 0, len(values))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = EqInt64(values, nil, nil, out, 7)
	}
	benchSel = out
}

func BenchmarkEqInt64FiftyPercentMatches(b *testing.B) {
	values := make([]int64, vector.StandardBatchRows)
	for i := range values {
		if i%2 == 0 {
			values[i] = 7
		} else {
			values[i] = int64(i + 1000)
		}
	}
	out := make(vector.Sel, 0, len(values))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = EqInt64(values, nil, nil, out, 7)
	}
	benchSel = out
}

func BenchmarkEqInt64WithNulls(b *testing.B) {
	values := make([]int64, vector.StandardBatchRows)
	valid := vector.NewValidity(len(values))
	for i := range values {
		values[i] = int64(i % 17)
		if i%11 == 0 {
			vector.SetInvalid(valid, i)
		}
	}
	out := make(vector.Sel, 0, len(values))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = EqInt64(values, valid, nil, out, 7)
	}
	benchSel = out
}

func BenchmarkEqInt64WithNullsDense(b *testing.B) {
	values := make([]int64, vector.StandardBatchRows)
	valid := vector.NewValidity(len(values))
	for i := range values {
		values[i] = int64(i % 17)
		if i%2 == 0 {
			vector.SetInvalid(valid, i)
		}
	}
	out := make(vector.Sel, 0, len(values))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = EqInt64(values, valid, nil, out, 7)
	}
	benchSel = out
}

func BenchmarkEqInt64WithNullsSparse(b *testing.B) {
	values := make([]int64, vector.StandardBatchRows)
	valid := vector.NewValidity(len(values))
	for i := range values {
		values[i] = int64(i % 17)
		if i%257 == 0 {
			vector.SetInvalid(valid, i)
		}
	}
	out := make(vector.Sel, 0, len(values))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = EqInt64(values, valid, nil, out, 7)
	}
	benchSel = out
}

func BenchmarkEqInt64WithSel(b *testing.B) {
	values := make([]int64, vector.StandardBatchRows)
	sel := make(vector.Sel, 0, len(values)/2)
	for i := range values {
		values[i] = int64(i % 17)
		if i%2 == 0 {
			sel = append(sel, vector.Row(i))
		}
	}
	out := make(vector.Sel, 0, len(sel))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = EqInt64(values, nil, sel, out, 7)
	}
	benchSel = out
}

func BenchmarkEqInt64WithTinySel(b *testing.B) {
	values := make([]int64, vector.StandardBatchRows)
	sel := make(vector.Sel, 0, 32)
	for i := range values {
		values[i] = int64(i % 17)
		if i < 32 {
			sel = append(sel, vector.Row(i))
		}
	}
	out := make(vector.Sel, 0, len(sel))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = EqInt64(values, nil, sel, out, 7)
	}
	benchSel = out
}

func BenchmarkEqInt64WithDenseSel(b *testing.B) {
	values := make([]int64, vector.StandardBatchRows)
	sel := make(vector.Sel, 0, len(values))
	for i := range values {
		values[i] = int64(i % 17)
		if i%10 != 0 {
			sel = append(sel, vector.Row(i))
		}
	}
	out := make(vector.Sel, 0, len(sel))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = EqInt64(values, nil, sel, out, 7)
	}
	benchSel = out
}

func BenchmarkMinMaxInt64(b *testing.B) {
	values := make([]int64, vector.StandardBatchRows)
	for i := range values {
		values[i] = int64((i * 37) % 1000)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchMin, benchMax, benchOK = MinMaxInt64(values, nil, nil)
	}
}

func BenchmarkCountTrueFullWords(b *testing.B) {
	n := vector.StandardBatchRows
	bits := make([]uint64, vector.ValidityWords(n))
	for i := range bits {
		bits[i] = ^uint64(0)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchCount = CountTrue(bits, n, nil, nil)
	}
}

func BenchmarkCountTrueTrailingBits(b *testing.B) {
	n := vector.StandardBatchRows - 3
	bits := make([]uint64, vector.ValidityWords(n))
	for i := range bits {
		bits[i] = ^uint64(0)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchCount = CountTrue(bits, n, nil, nil)
	}
}

func BenchmarkEqBytesShort(b *testing.B) {
	v := makeBenchmarkVarBytes(vector.StandardBatchRows, 8)
	out := make(vector.Sel, 0, vector.StandardBatchRows)
	rhs := []byte("row-0007")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = EqBytes(v, nil, nil, out, rhs)
	}
	benchSel = out
}

func BenchmarkEqBytesLong(b *testing.B) {
	v := makeBenchmarkVarBytes(vector.StandardBatchRows, 96)
	out := make(vector.Sel, 0, vector.StandardBatchRows)
	rhs := v.Bytes(7)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out = EqBytes(v, nil, nil, out, rhs)
	}
	benchSel = out
}

func makeBenchmarkVarBytes(rows int, width int) vector.VarBytes {
	v := vector.NewVarBytes(rows, rows*width)
	for row := 0; row < rows; row++ {
		start := len(v.Data)
		for len(v.Data)-start < width {
			v.Data = append(v.Data, byte('a'+row%26))
		}
		copy(v.Data[start:], []byte("row-0000"))
		v.Data[start+4] = byte('0' + row/1000%10)
		v.Data[start+5] = byte('0' + row/100%10)
		v.Data[start+6] = byte('0' + row/10%10)
		v.Data[start+7] = byte('0' + row%10)
		v.Offsets[row+1] = uint32(len(v.Data))
	}
	return v
}
