// End-to-end storage benches: write, cold-open, scan, predicate scan, and lazy sidecar
// load. Page shape mirrors the cmd/bench users dataset (id int64, name text, age int64,
// category text) so the numbers stack against the workload bench.
package storage

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}

const benchPageRows = 2048

func BenchmarkStorage_WriteSegment(b *testing.B) {
	page := makeBenchSegmentBatch(benchPageRows)
	pages := []types.Batch{page, page, page, page}
	b.ReportAllocs()
	for b.Loop() {
		tmp := b.TempDir()
		path := filepath.Join(tmp, "seg.dsv4")
		if _, err := WriteSegment(path, pages, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStorage_WriteSegment_Int64Random(b *testing.B) {
	runWriteShapeBench(b, makeInt64RandomBatch(benchPageRows))
}

func BenchmarkStorage_WriteSegment_Int64Monotonic(b *testing.B) {
	runWriteShapeBench(b, makeInt64MonotonicBatch(benchPageRows))
}

func BenchmarkStorage_WriteSegment_Int64Constant(b *testing.B) {
	runWriteShapeBench(b, makeInt64ConstantBatch(benchPageRows))
}

func BenchmarkStorage_WriteSegment_Int64SparseNulls(b *testing.B) {
	runWriteShapeBench(b, makeInt64SparseNullsBatch(benchPageRows))
}

func BenchmarkStorage_WriteSegment_TextLowCardinality(b *testing.B) {
	runWriteShapeBench(b, makeTextLowCardBatch(benchPageRows))
}

func BenchmarkStorage_WriteSegment_TextHighCardinality(b *testing.B) {
	runWriteShapeBench(b, makeTextHighCardBatch(benchPageRows))
}

func BenchmarkStorage_WriteSegment_TextLongValues(b *testing.B) {
	runWriteShapeBench(b, makeTextLongBatch(benchPageRows))
}

func BenchmarkStorage_WriteSegment_Float64Decimal(b *testing.B) {
	runWriteShapeBench(b, makeFloat64DecimalBatch(benchPageRows))
}

func BenchmarkStorage_WriteSegment_Float64Plain(b *testing.B) {
	runWriteShapeBench(b, makeFloat64PlainBatch(benchPageRows))
}

// TestFloat64_ALPVsPlain_FileSize is a one-shot check that ALP actually shrinks
// segment files for decimal-shaped float data. Asserts the ALP-default file is
// strictly smaller than the same data forced to flat encoding.
func TestFloat64_ALPVsPlain_FileSize(t *testing.T) {
	page := makeFloat64DecimalBatch(benchPageRows)
	pages := []types.Batch{page, page, page, page}
	tmp := t.TempDir()

	defaultPath := filepath.Join(tmp, "alp.dsv4")
	if _, err := WriteSegment(defaultPath, pages, nil); err != nil {
		t.Fatal(err)
	}
	forcedPath := filepath.Join(tmp, "plain.dsv4")
	overrides := map[string]types.Encoding{"price": types.EncodingFlat}
	if _, err := WriteSegment(forcedPath, pages, overrides); err != nil {
		t.Fatal(err)
	}
	alpSize := fileSize(t, defaultPath)
	plainSize := fileSize(t, forcedPath)
	t.Logf("alp=%d plain=%d ratio=%.2f", alpSize, plainSize, float64(alpSize)/float64(plainSize))
	if alpSize >= plainSize {
		t.Fatalf("ALP did not shrink decimal float column: alp=%d plain=%d", alpSize, plainSize)
	}
}

func BenchmarkStorage_OpenCold(b *testing.B) {
	tmp := b.TempDir()
	path := filepath.Join(tmp, "seg.dsv4")
	page := makeBenchSegmentBatch(benchPageRows)
	if _, err := WriteSegment(path, []types.Batch{page, page, page, page}, nil); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		seg, err := OpenSegment(path)
		if err != nil {
			b.Fatal(err)
		}
		seg.Close()
	}
}

func BenchmarkStorage_ScanFull(b *testing.B) {
	seg := openBenchSegment(b, 4)
	defer seg.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		opts := ScanOpts{Segments: []*Segment{seg}}
		err := Scan(opts, func(batch types.Batch, sel *types.SelectionMask) error { return nil })
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStorage_ScanEqInt64Hit(b *testing.B) {
	seg := openBenchSegment(b, 4)
	defer seg.Close()
	pred := Pred{Op: OpEq, Col: "age", Kind: types.VecInt64, I64: 42}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		opts := ScanOpts{Segments: []*Segment{seg}, Pred: &pred}
		err := Scan(opts, func(batch types.Batch, sel *types.SelectionMask) error { return nil })
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStorage_ScanEqInt64Miss(b *testing.B) {
	seg := openBenchSegment(b, 4)
	defer seg.Close()
	// age is i % 100 so 999 falls inside [0, 99] only as a Bloom-rescuable miss.
	pred := Pred{Op: OpEq, Col: "age", Kind: types.VecInt64, I64: 999}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		opts := ScanOpts{Segments: []*Segment{seg}, Pred: &pred}
		err := Scan(opts, func(batch types.Batch, sel *types.SelectionMask) error { return nil })
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStorage_ScanEqBytesHit(b *testing.B) {
	seg := openBenchSegment(b, 4)
	defer seg.Close()
	pred := Pred{Op: OpEq, Col: "category", Kind: types.VecText, Bytes: []byte("alpha")}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		opts := ScanOpts{Segments: []*Segment{seg}, Pred: &pred}
		err := Scan(opts, func(batch types.Batch, sel *types.SelectionMask) error { return nil })
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStorage_LoadDictHist(b *testing.B) {
	seg := openBenchSegment(b, 4)
	defer seg.Close()
	b.ReportAllocs()
	for b.Loop() {
		seg.containerOnce = sync.Once{}
		seg.dictHistsOnce = sync.Once{}
		if _, err := seg.DictHistograms(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStorage_LoadIntFilter(b *testing.B) {
	seg := openBenchSegment(b, 4)
	defer seg.Close()
	b.ReportAllocs()
	for b.Loop() {
		seg.containerOnce = sync.Once{}
		seg.intFiltersOnce = sync.Once{}
		if _, err := seg.IntFilterSet(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStorage_LoadNumericSums(b *testing.B) {
	seg := openBenchSegment(b, 4)
	defer seg.Close()
	b.ReportAllocs()
	for b.Loop() {
		seg.containerOnce = sync.Once{}
		seg.numSumsOnce = sync.Once{}
		if _, err := seg.NumericSums(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWireBuffer_RoundTrip(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		w := newWireBuffer(256)
		w.U8(1)
		w.U16(2)
		w.U32(3)
		w.U64(4)
		w.LenPrefixedString("category")
		w.LenPrefixedString("age")
		w.Raw([]byte{1, 2, 3, 4, 5, 6, 7, 8})
		r := newWireReader(w.Bytes())
		_ = r.U8()
		_ = r.U16()
		_ = r.U32()
		_ = r.U64()
		_ = r.LenPrefixedString()
		_ = r.LenPrefixedString()
		_ = r.Raw(8)
	}
}

func openBenchSegment(b *testing.B, pages int) *Segment {
	b.Helper()
	tmp := b.TempDir()
	path := filepath.Join(tmp, "seg.dsv4")
	batch := makeBenchSegmentBatch(benchPageRows)
	all := make([]types.Batch, pages)
	for i := range pages {
		all[i] = batch
	}
	if _, err := WriteSegment(path, all, nil); err != nil {
		b.Fatal(err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		b.Fatal(err)
	}
	return seg
}

func runWriteShapeBench(b *testing.B, page types.Batch) {
	b.Helper()
	pages := []types.Batch{page, page, page, page}
	b.ReportAllocs()
	for b.Loop() {
		tmp := b.TempDir()
		path := filepath.Join(tmp, "seg.dsv4")
		if _, err := WriteSegment(path, pages, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func makeInt64Batch(name string, rows int, fill func(i int) int64) types.Batch {
	v := types.NewVec(types.VecInt64, rows)
	xs := v.I64()
	for i := range rows {
		xs[i] = fill(i)
	}
	batch, err := types.NewBatch([]types.Column{{Name: name, Type: types.Int64, V: v}})
	if err != nil {
		panic(err)
	}
	return batch
}

func makeInt64RandomBatch(rows int) types.Batch {
	// LCG so the test stays deterministic without importing math/rand.
	state := uint64(0x9E3779B97F4A7C15)
	return makeInt64Batch("x", rows, func(int) int64 {
		state = state*6364136223846793005 + 1442695040888963407
		return int64(state)
	})
}

func makeInt64MonotonicBatch(rows int) types.Batch {
	return makeInt64Batch("x", rows, func(i int) int64 { return int64(i) })
}

func makeInt64ConstantBatch(rows int) types.Batch {
	return makeInt64Batch("x", rows, func(int) int64 { return 42 })
}

func makeInt64SparseNullsBatch(rows int) types.Batch {
	v := types.NewVec(types.VecInt64, rows)
	v.Valid = types.NewValidity(rows)
	xs := v.I64()
	for i := range rows {
		xs[i] = int64(i % 256)
		if i%10 == 0 {
			v.Valid.SetInvalid(i)
		}
	}
	batch, err := types.NewBatch([]types.Column{{Name: "x", Type: types.Int64, V: v}})
	if err != nil {
		panic(err)
	}
	return batch
}

func makeTextBatch(rows int, fill func(i int) string) types.Batch {
	v := types.NewVarVec(types.VecText, rows, 0)
	vb := v.Var()
	for i := range rows {
		vb.AppendString(i, fill(i))
	}
	batch, err := types.NewBatch([]types.Column{{Name: "s", Type: types.Text, V: v}})
	if err != nil {
		panic(err)
	}
	return batch
}

func makeTextLowCardBatch(rows int) types.Batch {
	labels := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	return makeTextBatch(rows, func(i int) string { return labels[i%len(labels)] })
}

func makeTextHighCardBatch(rows int) types.Batch {
	// 8192 distinct strings cycled across rows. Exceeds DictMaxValues (256)
	// so Dictionary codec rejects and falls through to plain varbytes.
	const distinct = 8192
	cache := make([]string, distinct)
	for i := range distinct {
		cache[i] = fmt.Sprintf("v%07d", i)
	}
	return makeTextBatch(rows, func(i int) string { return cache[i%distinct] })
}

// makeFloat64DecimalBatch produces ALP-friendly prices like 12.34 across [0, 1000).
// Two decimal places fit cleanly at e=2 with a small mantissa range, exercising ALP.
func makeFloat64DecimalBatch(rows int) types.Batch {
	v := types.NewVec(types.VecFloat64, rows)
	xs := v.F64()
	for i := range rows {
		cents := int64((i * 7919) % 100000)
		xs[i] = float64(cents) / 100.0
	}
	batch, err := types.NewBatch([]types.Column{{Name: "price", Type: types.Float64, V: v}})
	if err != nil {
		panic(err)
	}
	return batch
}

// makeFloat64PlainBatch produces noisy float64 from a uint64 LCG bit-cast. ALP
// must reject these (no exponent makes them round-trip) and fall through to plain.
func makeFloat64PlainBatch(rows int) types.Batch {
	state := uint64(0x9E3779B97F4A7C15)
	v := types.NewVec(types.VecFloat64, rows)
	xs := v.F64()
	for i := range rows {
		state = state*6364136223846793005 + 1442695040888963407
		bits := (state & 0x000FFFFFFFFFFFFF) | 0x4000000000000000
		xs[i] = math.Float64frombits(bits)
	}
	batch, err := types.NewBatch([]types.Column{{Name: "x", Type: types.Float64, V: v}})
	if err != nil {
		panic(err)
	}
	return batch
}

func makeTextLongBatch(rows int) types.Batch {
	// Each value > StringViewInlineMax (12 bytes) so SetView takes the long path.
	const padding = "long_string_payload_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	return makeTextBatch(rows, func(i int) string {
		return fmt.Sprintf("%s_%d", padding, i)
	})
}

func makeBenchSegmentBatch(rows int) types.Batch {
	idVec := types.NewVec(types.VecInt64, rows)
	for i := range idVec.I64() {
		idVec.I64()[i] = int64(i)
	}
	ageVec := types.NewVec(types.VecInt64, rows)
	for i := range ageVec.I64() {
		ageVec.I64()[i] = int64(i % 100)
	}
	nameVec := types.NewVarVec(types.VecText, rows, 0)
	for i := range rows {
		nameVec.Var().AppendString(i, "row_name")
	}
	catVec := types.NewVarVec(types.VecText, rows, 0)
	labels := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for i := range rows {
		catVec.Var().AppendString(i, labels[i%len(labels)])
	}
	batch, err := types.NewBatch([]types.Column{
		{Name: "id", Type: types.Int64, V: idVec},
		{Name: "name", Type: types.Text, V: nameVec},
		{Name: "age", Type: types.Int64, V: ageVec},
		{Name: "category", Type: types.Text, V: catVec},
	})
	if err != nil {
		panic(err)
	}
	return batch
}
