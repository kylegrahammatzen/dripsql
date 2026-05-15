// Binary Fuse filter correctness + prune path: filter reports presence for every inserted
// key, rejects keys absent from the segment, and boundEqInt64.PruneSegment uses it when
// the value lies inside column min/max but is not in the segment.
package storage

import (
	"path/filepath"
	"testing"

	"github.com/FastFilter/xorfilter"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestIntFilter_NoFalseNegatives(t *testing.T) {
	keys := make([]uint64, 0, 1000)
	for i := range 1000 {
		keys = append(keys, uint64(i)*7)
	}
	fuse, err := xorfilter.PopulateBinaryFuse8(keys)
	if err != nil {
		t.Fatalf("populate: %v", err)
	}
	f := &IntFilter{fuse: fuse}
	for _, k := range keys {
		if !f.Contains(int64(k)) {
			t.Fatalf("Binary Fuse missed inserted key %d", k)
		}
	}
}

func TestBoundEqInt64_PruneSegment_UsesIntFilterWhenInRange(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "seg.dsv4")
	v := types.NewVec(types.VecInt64, 100)
	s := v.I64()
	for i := range s {
		s[i] = int64(i * 100)
	}
	col := types.Column{Name: "id", Type: types.Int64, V: v}
	batch, err := types.NewBatch([]types.Column{col})
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteSegment(path, []types.Batch{batch}); err != nil {
		t.Fatal(err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()
	filters, err := seg.IntFilterSet()
	if err != nil {
		t.Fatal(err)
	}
	if filters == nil {
		t.Fatal("expected int filter sidecar to be loaded")
	}

	pred, err := BindPredicate(EqInt64{Column: "id", Value: 50}, SegmentSchema(seg))
	if err != nil {
		t.Fatal(err)
	}
	if !pred.PruneSegment(seg) {
		t.Fatalf("PruneSegment(id=50) should have pruned via Binary Fuse (50 in [0, 9900] but not in segment)")
	}
	pred, err = BindPredicate(EqInt64{Column: "id", Value: 100}, SegmentSchema(seg))
	if err != nil {
		t.Fatal(err)
	}
	if pred.PruneSegment(seg) {
		t.Fatalf("PruneSegment(id=100) must not prune a key present in segment")
	}
}
