// Sidecar tests covering bloom-filter correctness, the boundEqInt64 PruneSegment fast path, and per-page varbytes bloom pruning.
package storage

import (
	"os"
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
	cp, err := CompilePred(p)
	if err != nil {
		t.Fatal(err)
	}
	if !cp.Skips(seg) {
		t.Fatalf("Skips(id=50) should have pruned via Binary Fuse (50 in [0, 9900] but not in segment)")
	}
	p = Pred{Op: OpEq, Col: "id", Kind: vector.VecInt64, I64: 100}
	cp, err = CompilePred(p)
	if err != nil {
		t.Fatal(err)
	}
	if cp.Skips(seg) {
		t.Fatalf("Skips(id=100) must not prune a key present in segment")
	}
}

func TestVarBloom_PrunesAbsentPerPage(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.dat")

	makePage := func(values []string) vector.Batch {
		v := vector.NewVarVec(vector.VecText, len(values), 0)
		vb := v.Var()
		for i, s := range values {
			vb.AppendString(i, s)
		}
		b, err := vector.NewBatch([]vector.Column{{Name: "k", Type: schema.Text, V: v}})
		if err != nil {
			t.Fatalf("NewBatch: %v", err)
		}
		return b
	}
	pages := []vector.Batch{
		makePage([]string{"alpha", "alpha", "alpha"}),
		makePage([]string{"beta", "beta"}),
		makePage([]string{"gamma", "gamma", "gamma", "gamma"}),
	}
	if _, err := WriteSegment(path, pages, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	defer os.Remove(path)

	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()

	blooms, err := seg.VarBlooms()
	if err != nil {
		t.Fatalf("VarBlooms: %v", err)
	}
	vb := blooms[schema.NormalizeName("k")]
	if vb == nil {
		t.Fatal("missing varbloom for k")
	}
	if len(vb.Pages) != 3 {
		t.Fatalf("want 3 page blooms got %d", len(vb.Pages))
	}

	bp := boundEqBytes{column: "k", value: []byte("beta")}
	if !bp.PrunePage(seg, 0) {
		t.Errorf("page 0 should prune 'beta'")
	}
	if bp.PrunePage(seg, 1) {
		t.Errorf("page 1 must NOT prune 'beta' (value present)")
	}
	if !bp.PrunePage(seg, 2) {
		t.Errorf("page 2 should prune 'beta'")
	}
}
