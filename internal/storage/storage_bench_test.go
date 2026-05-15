// End-to-end storage benches: write, cold-open, scan, predicate scan, and lazy sidecar
// load. Page shape mirrors the cmd/bench users dataset (id int64, name text, age int64,
// category text) so the numbers stack against the workload bench in drip_bench.md.
package storage

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

const benchPageRows = 2048

func BenchmarkStorage_WriteSegment(b *testing.B) {
	page := makeBenchSegmentBatch(benchPageRows)
	pages := []types.Batch{page, page, page, page}
	b.ReportAllocs()
	for b.Loop() {
		tmp := b.TempDir()
		path := filepath.Join(tmp, "seg.dsv4")
		if err := WriteSegment(path, pages); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStorage_OpenCold(b *testing.B) {
	tmp := b.TempDir()
	path := filepath.Join(tmp, "seg.dsv4")
	page := makeBenchSegmentBatch(benchPageRows)
	if err := WriteSegment(path, []types.Batch{page, page, page, page}); err != nil {
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
	pred := EqInt64{Column: "age", Value: 42}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		opts := ScanOpts{Segments: []*Segment{seg}, Predicate: pred}
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
	pred := EqInt64{Column: "age", Value: 999}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		opts := ScanOpts{Segments: []*Segment{seg}, Predicate: pred}
		err := Scan(opts, func(batch types.Batch, sel *types.SelectionMask) error { return nil })
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStorage_ScanEqBytesHit(b *testing.B) {
	seg := openBenchSegment(b, 4)
	defer seg.Close()
	pred := EqBytes{Column: "category", Value: []byte("alpha")}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		opts := ScanOpts{Segments: []*Segment{seg}, Predicate: pred}
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
	if err := WriteSegment(path, all); err != nil {
		b.Fatal(err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		b.Fatal(err)
	}
	return seg
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
