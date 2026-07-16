// SelectionMask invariant tests checking that the allSet cache stays coherent, tail bits are ignored,
// and zero-row masks are vacuously all-set across construction, resize, and clear.
package vector

import "testing"

func TestSelectionMask_AllSetAfterAnd(t *testing.T) {
	a := NewSelectionMask(100)
	a.FillAll()
	b := NewSelectionMask(100)
	b.FillAll()
	a.AndCount(b)
	if !a.IsAllSet() {
		t.Fatal("AND of two all-set masks must remain all-set")
	}
	c := NewSelectionMask(100)
	c.FillAll()
	c.Set(50)
	d := NewSelectionMask(100)
	d.FillAll()
	if n := d.AndCount(c); n != 100 || !d.IsAllSet() {
		t.Fatalf("AND with cleared-then-reset still equals all-set: n=%d allSet=%v", n, d.IsAllSet())
	}
}

func TestSelectionMask_TailBitsIgnored(t *testing.T) {
	s := NewSelectionMask(10)
	s.FillAll()
	if s.PopCount() != 10 {
		t.Fatalf("PopCount=%d want 10 (tail bits must be ignored)", s.PopCount())
	}
	s.NotCount()
	if s.PopCount() != 0 {
		t.Fatalf("PopCount after NOT=%d want 0", s.PopCount())
	}
}

func TestSelectionMask_ZeroRowsAllSet(t *testing.T) {
	s := NewSelectionMask(0)
	if !s.IsAllSet() {
		t.Fatal("zero-row mask must be vacuously all-set")
	}
	var r SelectionMask
	r.Resize(0)
	if !r.IsAllSet() {
		t.Fatal("Resize(0) must leave mask vacuously all-set")
	}
	s.Clear()
	if !s.IsAllSet() {
		t.Fatal("Clear() on zero-row mask must keep vacuously all-set")
	}
	nonzero := NewSelectionMask(8)
	nonzero.FillAll()
	nonzero.Clear()
	if nonzero.IsAllSet() {
		t.Fatal("Clear() on non-zero mask must drop allSet")
	}
}

func TestSelectionMask_IterSetOrderedAsc(t *testing.T) {
	s := NewSelectionMask(200)
	want := []int{3, 7, 63, 64, 65, 127, 128, 199}
	for _, r := range want {
		s.Set(r)
	}
	var got []int
	s.IterSet(func(row int) { got = append(got, row) })
	if len(got) != len(want) {
		t.Fatalf("IterSet emitted %d rows, want %d", len(got), len(want))
	}
	for i, r := range got {
		if r != want[i] {
			t.Fatalf("IterSet row %d = %d want %d", i, r, want[i])
		}
		if i > 0 && got[i] <= got[i-1] {
			t.Fatalf("IterSet must be strictly ascending; got %v", got)
		}
	}
}

func TestSelectionMask_PopCountMatchesIterSet(t *testing.T) {
	s := NewSelectionMask(300)
	for _, r := range []int{0, 1, 13, 64, 100, 255, 299} {
		s.Set(r)
	}
	visited := 0
	s.IterSet(func(int) { visited++ })
	if pc := s.PopCount(); pc != visited {
		t.Fatalf("PopCount=%d IterSet count=%d", pc, visited)
	}
}
