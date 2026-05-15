// Storage benches: segment cold open / footer parse, and wire buffer round-trip.
// Run with: go test ./internal/storage -bench=. -benchmem -run=^$
package storage

import (
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func BenchmarkSegmentOpen_Cold(b *testing.B) {
	tmp := b.TempDir()
	path := filepath.Join(tmp, "seg.dsv4")
	page := makeBenchSegmentBatch(2048)
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
