// Per-page varbytes bloom sidecar round-trip and prune test.
// Builds a 3-page segment of disjoint values and asserts PrunePage rejects per-page absentees.
package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

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
