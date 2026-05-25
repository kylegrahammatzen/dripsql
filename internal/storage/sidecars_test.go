// Bloom filter correctness and the boundEqInt64.PruneSegment fast path.
// Filter reports presence for every inserted key and rejects absent keys.
package storage

import (
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestIntFilter_NoFalseNegatives(t *testing.T) {
	keys := make([]uint64, 0, 1000)
	for i := range 1000 {
		keys = append(keys, uint64(i)*7)
	}
	f := &IntFilter{bloom: newBloomFilter(keys)}
	for _, k := range keys {
		if !f.Contains(int64(k)) {
			t.Fatalf("bloom filter missed inserted key %d", k)
		}
	}
}

func TestBoundEqInt64_PruneSegment_UsesIntFilterWhenInRange(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "seg.dsv4")
	v := vector.NewVec(vector.VecInt64, 100)
	s := v.I64()
	for i := range s {
		s[i] = int64(i * 100)
	}
	col := vector.Column{Name: "id", Type: schema.Int64, V: v}
	batch, err := vector.NewBatch([]vector.Column{col})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WriteSegment(path, []vector.Batch{batch}, nil); err != nil {
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

	p := Pred{Op: OpEq, Col: "id", Kind: vector.VecInt64, I64: 50}
	if !p.Skips(seg) {
		t.Fatalf("Skips(id=50) should have pruned via Binary Fuse (50 in [0, 9900] but not in segment)")
	}
	p = Pred{Op: OpEq, Col: "id", Kind: vector.VecInt64, I64: 100}
	if p.Skips(seg) {
		t.Fatalf("Skips(id=100) must not prune a key present in segment")
	}
}
