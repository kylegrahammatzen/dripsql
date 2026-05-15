// Bloom filter correctness + prune path: filter accepts every inserted key, rejects
// keys absent from the segment, and boundEqInt64.PruneSegment uses the filter when the
// value lies within column min/max but is not in the segment.
package storage

import (
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestIntBloom_NoFalseNegatives(t *testing.T) {
	b := newIntBloom(1000)
	for i := int64(0); i < 1000; i++ {
		b.Add(i * 7)
	}
	for i := int64(0); i < 1000; i++ {
		if !b.Contains(i * 7) {
			t.Fatalf("Bloom missed inserted key %d", i*7)
		}
	}
}

func TestBoundEqInt64_PruneSegment_UsesBloomWhenInRange(t *testing.T) {
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
	if seg.IntBlooms == nil {
		t.Fatal("expected bloom sidecar to be loaded")
	}

	pred, err := BindPredicate(EqInt64{Column: "id", Value: 50}, SegmentSchema(seg))
	if err != nil {
		t.Fatal(err)
	}
	if !pred.PruneSegment(seg) {
		t.Fatalf("PruneSegment(id=50) should have pruned via Bloom (50 in [0, 9900] but not in segment)")
	}
	pred, err = BindPredicate(EqInt64{Column: "id", Value: 100}, SegmentSchema(seg))
	if err != nil {
		t.Fatal(err)
	}
	if pred.PruneSegment(seg) {
		t.Fatalf("PruneSegment(id=100) must not prune a key present in segment")
	}
}
