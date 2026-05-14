package types

import (
	"sort"
	"testing"
)

func TestSelectionMaskZeroIsAllUnset(t *testing.T) {
	m := NewSelectionMask(64)
	for i := range 64 {
		if m.IsSet(i) {
			t.Errorf("row %d set in fresh mask", i)
		}
	}
}

func TestSelectionMaskSetGet(t *testing.T) {
	m := NewSelectionMask(128)
	m.Set(5)
	m.Set(63)
	m.Set(64)
	m.Set(127)
	for _, row := range []int{5, 63, 64, 127} {
		if !m.IsSet(row) {
			t.Errorf("row %d not set after Set", row)
		}
	}
	if m.IsSet(0) || m.IsSet(126) {
		t.Fatal("unexpected row reported set")
	}
}

func TestSelectionMaskSetPanicsOutOfRange(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	m := NewSelectionMask(8)
	m.Set(63)
}

func TestSelectionMaskSetPanicsNegative(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	m := NewSelectionMask(8)
	m.Set(-1)
}

func TestSelectionMaskUnsetPanicsOutOfRange(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	m := NewSelectionMask(8)
	m.Unset(8)
}

func TestSelectionMaskUnset(t *testing.T) {
	m := NewSelectionMask(64)
	m.FillAll()
	m.Unset(10)
	if m.IsSet(10) {
		t.Fatal("row 10 still set after Unset")
	}
	if !m.IsSet(11) {
		t.Fatal("Unset affected adjacent row")
	}
}

func TestSelectionMaskPopCountFreshIsZero(t *testing.T) {
	m := NewSelectionMask(64)
	if m.PopCount() != 0 {
		t.Fatalf("PopCount fresh = %d, want 0", m.PopCount())
	}
}

func TestSelectionMaskPopCountMasksTail(t *testing.T) {
	m := NewSelectionMask(10)
	m.Words[0] = ^uint64(0)
	if got := m.PopCount(); got != 10 {
		t.Fatalf("PopCount = %d, want 10", got)
	}
}

func TestSelectionMaskFillAllPopCount(t *testing.T) {
	for _, n := range []int{0, 1, 63, 64, 65, 127, 128, 200} {
		m := NewSelectionMask(n)
		m.FillAll()
		if got := m.PopCount(); got != n {
			t.Errorf("FillAll(%d).PopCount = %d, want %d", n, got, n)
		}
	}
}

func TestSelectionMaskReset(t *testing.T) {
	m := NewSelectionMask(64)
	m.FillAll()
	m.Reset()
	if m.PopCount() != 0 {
		t.Fatalf("PopCount after Reset = %d, want 0", m.PopCount())
	}
}

func TestSelectionMaskIterSet(t *testing.T) {
	m := NewSelectionMask(128)
	want := []int{0, 5, 63, 64, 127}
	for _, row := range want {
		m.Set(row)
	}
	got := []int{}
	m.IterSet(func(row int) { got = append(got, row) })
	if !sort.IntsAreSorted(got) {
		t.Fatal("IterSet must yield ascending row indices")
	}
	if len(got) != len(want) {
		t.Fatalf("IterSet yielded %v, want %v", got, want)
	}
	for i, r := range got {
		if r != want[i] {
			t.Errorf("got[%d] = %d, want %d", i, r, want[i])
		}
	}
}

func TestSelectionMaskIterSetMasksTail(t *testing.T) {
	m := NewSelectionMask(10)
	m.Words[0] = ^uint64(0)
	got := []int{}
	m.IterSet(func(row int) { got = append(got, row) })
	if len(got) != 10 {
		t.Fatalf("IterSet yielded %d rows, want 10: %v", len(got), got)
	}
	for i, row := range got {
		if row != i {
			t.Fatalf("got[%d] = %d, want %d", i, row, i)
		}
	}
}

func TestSelectionMaskIterEmpty(t *testing.T) {
	m := NewSelectionMask(64)
	count := 0
	m.IterSet(func(int) { count++ })
	if count != 0 {
		t.Fatalf("IterSet on empty yielded %d rows", count)
	}
}

func TestSelectionMaskAnd(t *testing.T) {
	a := NewSelectionMask(8)
	b := NewSelectionMask(8)
	a.Set(0)
	a.Set(1)
	a.Set(3)
	b.Set(1)
	b.Set(2)
	b.Set(3)
	a.And(b)
	if a.PopCount() != 2 {
		t.Fatalf("And popcount = %d, want 2", a.PopCount())
	}
	if !a.IsSet(1) || !a.IsSet(3) {
		t.Fatal("And must keep rows in both inputs")
	}
	if a.IsSet(0) || a.IsSet(2) {
		t.Fatal("And must drop rows not in both inputs")
	}
}

func TestSelectionMaskAndCount(t *testing.T) {
	a := NewSelectionMask(65)
	b := NewSelectionMask(65)
	a.Set(1)
	a.Set(64)
	b.Set(64)
	if got := a.AndCount(b); got != 1 {
		t.Fatalf("AndCount = %d, want 1", got)
	}
	if !a.IsSet(64) || a.IsSet(1) {
		t.Fatal("AndCount produced wrong mask")
	}
}

func TestSelectionMaskOr(t *testing.T) {
	a := NewSelectionMask(8)
	b := NewSelectionMask(8)
	a.Set(0)
	b.Set(2)
	a.Or(b)
	if a.PopCount() != 2 {
		t.Fatalf("Or popcount = %d, want 2", a.PopCount())
	}
	if !a.IsSet(0) || !a.IsSet(2) {
		t.Fatal("Or must include rows from either input")
	}
}

func TestSelectionMaskOrCount(t *testing.T) {
	a := NewSelectionMask(65)
	b := NewSelectionMask(65)
	a.Set(1)
	b.Set(64)
	if got := a.OrCount(b); got != 2 {
		t.Fatalf("OrCount = %d, want 2", got)
	}
	if !a.IsSet(1) || !a.IsSet(64) {
		t.Fatal("OrCount produced wrong mask")
	}
}

func TestSelectionMaskNotCount(t *testing.T) {
	m := NewSelectionMask(10)
	m.Set(1)
	m.Set(3)
	if got := m.NotCount(); got != 8 {
		t.Fatalf("NotCount = %d, want 8", got)
	}
	if m.IsSet(1) || m.IsSet(3) || !m.IsSet(0) || !m.IsSet(9) || m.IsSet(10) {
		t.Fatal("NotCount produced wrong mask")
	}
}

func TestSelectionMaskResizeClearsOldBits(t *testing.T) {
	m := NewSelectionMask(128)
	m.FillAll()
	m.Resize(8)
	if got := m.PopCount(); got != 0 {
		t.Fatalf("Resize should clear mask, got popcount %d", got)
	}
}

func TestSelectionMaskAndPanicsOnRowMismatch(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on row-count mismatch")
		}
	}()
	a := NewSelectionMask(64)
	b := NewSelectionMask(128)
	a.And(b)
}

func TestSelectionMaskToSel(t *testing.T) {
	m := NewSelectionMask(64)
	m.Set(3)
	m.Set(8)
	m.Set(40)
	sel := m.AppendToSel(make(Sel, 0, m.PopCount()))
	if len(sel) != 3 {
		t.Fatalf("AppendToSel returned %d rows, want 3", len(sel))
	}
	want := []Row{3, 8, 40}
	for i, r := range sel {
		if r != want[i] {
			t.Errorf("sel[%d] = %d, want %d", i, r, want[i])
		}
	}
}
