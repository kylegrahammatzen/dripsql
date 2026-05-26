// End-to-end storage benches cover write, cold open, scan, predicate scan, and lazy sidecar load.
// Page shape mirrors the cmd/bench users dataset with id int64, name text, age int64, and category text.
package storage

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
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

var benchVecSink vector.Vec

func BenchmarkStorage_Write(b *testing.B) {
	for _, tc := range []struct {
		name string
		page vector.Batch
	}{
		{name: "All", page: makeBenchSegmentBatch(benchPageRows)},
		{name: "Int64/Random", page: makeInt64RandomBatch(benchPageRows)},
		{name: "Int64/Sequence", page: makeInt64MonotonicBatch(benchPageRows)},
		{name: "Int64/Constant", page: makeInt64ConstantBatch(benchPageRows)},
		{name: "Int64/SparseNulls", page: makeInt64SparseNullsBatch(benchPageRows)},
		{name: "Float64/Plain", page: makeFloat64PlainBatch(benchPageRows)},
		{name: "Float64/Decimal", page: makeFloat64DecimalBatch(benchPageRows)},
		{name: "Text/Dict", page: makeTextLowCardBatch(benchPageRows)},
		{name: "Text/Plain", page: makeTextHighCardBatch(benchPageRows)},
		{name: "Text/Long", page: makeTextLongBatch(benchPageRows)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			runWriteShapeBench(b, tc.page)
		})
	}
}

// TestFloat64_ALPVsPlain_FileSize is a one-shot check that ALP actually shrinks
// segment files for decimal-shaped float data. Asserts the ALP-default file is
// strictly smaller than the same data forced to flat encoding.
func TestFloat64_ALPVsPlain_FileSize(t *testing.T) {
	page := makeFloat64DecimalBatch(benchPageRows)
	pages := []vector.Batch{page, page, page, page}
	tmp := t.TempDir()

	defaultPath := filepath.Join(tmp, "alp.dsv4")
	if _, err := WriteSegment(defaultPath, pages, nil); err != nil {
		t.Fatal(err)
	}
	forcedPath := filepath.Join(tmp, "plain.dsv4")
	overrides := map[string]schema.Encoding{"price": schema.EncPlain}
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
	if _, err := WriteSegment(path, []vector.Batch{page, page, page, page}, nil); err != nil {
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

func BenchmarkStorage_ReadPage(b *testing.B) {
	for _, tc := range []struct {
		name   string
		colIdx int
	}{
		{name: "Int64", colIdx: 0},
		{name: "Text", colIdx: 3},
	} {
		b.Run(tc.name, func(b *testing.B) {
			runReadPageBench(b, tc.colIdx)
		})
	}
}

func runReadPageBench(b *testing.B, colIdx int) {
	b.Helper()
	seg := openBenchSegment(b, 4)
	defer seg.Close()
	if err := seg.ValidateColumns(); err != nil {
		b.Fatal(err)
	}
	var scratch []byte
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		v, nextScratch, err := seg.ReadPageInto(colIdx, 0, scratch)
		if err != nil {
			b.Fatal(err)
		}
		scratch = nextScratch
		benchVecSink = v
	}
}

func BenchmarkStorage_Scan(b *testing.B) {
	for _, tc := range []struct {
		name    string
		columns []string
		pred    *Pred
	}{
		{name: "One/Int64", columns: []string{"id"}},
		{name: "One/Text", columns: []string{"category"}},
		{name: "All"},
		{name: "Eq/Int64/Hit", pred: &Pred{Op: OpEq, Col: "age", Kind: vector.VecInt64, I64: 42}},
		{name: "Eq/Int64/Miss", pred: &Pred{Op: OpEq, Col: "age", Kind: vector.VecInt64, I64: 999}},
		{name: "Eq/Text/Hit", pred: &Pred{Op: OpEq, Col: "category", Kind: vector.VecText, Bytes: []byte("alpha")}},
		{name: "Lt/Int64/AllByMeta", columns: []string{"name"}, pred: &Pred{Op: OpLt, Col: "id", Kind: vector.VecInt64, I64: benchPageRows}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			runScanBench(b, tc.columns, tc.pred)
		})
	}
}

func runScanBench(b *testing.B, columns []string, pred *Pred) {
	b.Helper()
	seg := openBenchSegment(b, 4)
	defer seg.Close()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		opts := ScanOpts{Segments: []*Segment{seg}, Columns: columns, Pred: pred}
		err := Scan(opts, func(batch vector.Batch, sel *vector.SelectionMask) error { return nil })
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
	all := make([]vector.Batch, pages)
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

func runWriteShapeBench(b *testing.B, page vector.Batch) {
	b.Helper()
	pages := []vector.Batch{page, page, page, page}
	b.ReportAllocs()
	for b.Loop() {
		tmp := b.TempDir()
		path := filepath.Join(tmp, "seg.dsv4")
		if _, err := WriteSegment(path, pages, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func makeInt64Batch(name string, rows int, fill func(i int) int64) vector.Batch {
	v := vector.NewVec(vector.VecInt64, rows)
	xs := v.I64()
	for i := range rows {
		xs[i] = fill(i)
	}
	batch, err := vector.NewBatch([]vector.Column{{Name: name, Type: schema.Int64, V: v}})
	if err != nil {
		panic(err)
	}
	return batch
}

func makeInt64RandomBatch(rows int) vector.Batch {
	// LCG so the test stays deterministic without importing math/rand.
	state := uint64(0x9E3779B97F4A7C15)
	return makeInt64Batch("x", rows, func(int) int64 {
		state = state*6364136223846793005 + 1442695040888963407
		return int64(state)
	})
}

func makeInt64MonotonicBatch(rows int) vector.Batch {
	return makeInt64Batch("x", rows, func(i int) int64 { return int64(i) })
}

func makeInt64ConstantBatch(rows int) vector.Batch {
	return makeInt64Batch("x", rows, func(int) int64 { return 42 })
}

func makeInt64SparseNullsBatch(rows int) vector.Batch {
	v := vector.NewVec(vector.VecInt64, rows)
	v.Valid = vector.NewValidity(rows)
	xs := v.I64()
	for i := range rows {
		xs[i] = int64(i % 256)
		if i%10 == 0 {
			v.Valid.SetInvalid(i)
		}
	}
	batch, err := vector.NewBatch([]vector.Column{{Name: "x", Type: schema.Int64, V: v}})
	if err != nil {
		panic(err)
	}
	return batch
}

func makeTextBatch(rows int, fill func(i int) string) vector.Batch {
	v := vector.NewVarVec(vector.VecText, rows, 0)
	vb := v.Var()
	for i := range rows {
		vb.AppendString(i, fill(i))
	}
	batch, err := vector.NewBatch([]vector.Column{{Name: "s", Type: schema.Text, V: v}})
	if err != nil {
		panic(err)
	}
	return batch
}

func makeTextLowCardBatch(rows int) vector.Batch {
	labels := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	return makeTextBatch(rows, func(i int) string { return labels[i%len(labels)] })
}

func makeTextHighCardBatch(rows int) vector.Batch {
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
func makeFloat64DecimalBatch(rows int) vector.Batch {
	v := vector.NewVec(vector.VecFloat64, rows)
	xs := v.F64()
	for i := range rows {
		cents := int64((i * 7919) % 100000)
		xs[i] = float64(cents) / 100.0
	}
	batch, err := vector.NewBatch([]vector.Column{{Name: "price", Type: schema.Float64, V: v}})
	if err != nil {
		panic(err)
	}
	return batch
}

// makeFloat64PlainBatch produces noisy float64 from a uint64 LCG bit-cast. ALP
// must reject these (no exponent makes them round-trip) and fall through to plain.
func makeFloat64PlainBatch(rows int) vector.Batch {
	state := uint64(0x9E3779B97F4A7C15)
	v := vector.NewVec(vector.VecFloat64, rows)
	xs := v.F64()
	for i := range rows {
		state = state*6364136223846793005 + 1442695040888963407
		bits := (state & 0x000FFFFFFFFFFFFF) | 0x4000000000000000
		xs[i] = math.Float64frombits(bits)
	}
	batch, err := vector.NewBatch([]vector.Column{{Name: "x", Type: schema.Float64, V: v}})
	if err != nil {
		panic(err)
	}
	return batch
}

func makeTextLongBatch(rows int) vector.Batch {
	// Each value > StringViewInlineMax (12 bytes) so SetView takes the long path.
	const padding = "long_string_payload_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	return makeTextBatch(rows, func(i int) string {
		return fmt.Sprintf("%s_%d", padding, i)
	})
}

func makeBenchSegmentBatch(rows int) vector.Batch {
	idVec := vector.NewVec(vector.VecInt64, rows)
	for i := range idVec.I64() {
		idVec.I64()[i] = int64(i)
	}
	ageVec := vector.NewVec(vector.VecInt64, rows)
	for i := range ageVec.I64() {
		ageVec.I64()[i] = int64(i % 100)
	}
	nameVec := vector.NewVarVec(vector.VecText, rows, 0)
	for i := range rows {
		nameVec.Var().AppendString(i, "row_name")
	}
	catVec := vector.NewVarVec(vector.VecText, rows, 0)
	labels := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for i := range rows {
		catVec.Var().AppendString(i, labels[i%len(labels)])
	}
	batch, err := vector.NewBatch([]vector.Column{
		{Name: "id", Type: schema.Int64, V: idVec},
		{Name: "name", Type: schema.Text, V: nameVec},
		{Name: "age", Type: schema.Int64, V: ageVec},
		{Name: "category", Type: schema.Text, V: catVec},
	})
	if err != nil {
		panic(err)
	}
	return batch
}
