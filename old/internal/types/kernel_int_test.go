package types

import (
	"math"
	"testing"
)

func TestEqInt64AllValid(t *testing.T) {
	tests := []struct {
		name string
		x    []int64
		rhs  int64
		want Sel
	}{
		{name: "some", x: []int64{1, 2, 3, 2, 1}, rhs: 2, want: Sel{1, 3}},
		{name: "none", x: []int64{1, 2, 3}, rhs: 99, want: nil},
		{name: "all", x: []int64{7, 7, 7}, rhs: 7, want: Sel{0, 1, 2}},
		{name: "alternating", x: []int64{4, 5, 4, 5, 4}, rhs: 4, want: Sel{0, 2, 4}},
		{name: "negative", x: []int64{-1, 1, -1}, rhs: -1, want: Sel{0, 2}},
		{name: "first", x: []int64{8, 1, 2}, rhs: 8, want: Sel{0}},
		{name: "last", x: []int64{1, 2, 8}, rhs: 8, want: Sel{2}},
		{name: "empty", x: nil, rhs: 1, want: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := EqInt64(tt.x, nil, nil, nil, tt.rhs)
			assertSel(t, got, tt.want)
		})
	}
}

func TestEqInt64NoMatch(t *testing.T) {
	x := []int64{1, 2, 3}
	got := EqInt64(x, nil, nil, nil, 99)
	if len(got) != 0 {
		t.Fatalf("EqInt64 no-match selected %d rows", len(got))
	}
}

func TestEqInt64AllMatch(t *testing.T) {
	x := []int64{7, 7, 7}
	got := EqInt64(x, nil, nil, nil, 7)
	if len(got) != 3 {
		t.Fatalf("EqInt64 all-match selected %d rows, want 3", len(got))
	}
}

func TestEqInt64WithNulls(t *testing.T) {
	tests := []struct {
		name    string
		invalid []int
		want    Sel
	}{
		{name: "none invalid", invalid: nil, want: Sel{1, 3}},
		{name: "skip first match", invalid: []int{1}, want: Sel{3}},
		{name: "skip second match", invalid: []int{3}, want: Sel{1}},
		{name: "skip all matches", invalid: []int{1, 3}, want: nil},
		{name: "skip nonmatch", invalid: []int{0, 4}, want: Sel{1, 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			x := []int64{1, 2, 3, 2, 1}
			v := NewValidity(len(x))
			for _, row := range tt.invalid {
				SetInvalid(v, row)
			}
			got := EqInt64(x, v, nil, nil, 2)
			assertSel(t, got, tt.want)
		})
	}
}

func TestEqInt64WithSelection(t *testing.T) {
	x := []int64{1, 2, 3, 2, 1}
	sel := Sel{1, 3, 4}
	got := EqInt64(x, nil, sel, nil, 2)
	if len(got) != 2 {
		t.Fatalf("EqInt64 sel selected %d, want 2", len(got))
	}
}

func TestLtInt64AllValid(t *testing.T) {
	x := []int64{1, 5, 3, 7}
	got := LtInt64(x, nil, nil, nil, 4)
	if len(got) != 2 {
		t.Fatalf("LtInt64 selected %d, want 2", len(got))
	}
}

func TestGteInt64AllValid(t *testing.T) {
	x := []int64{1, 5, 3, 7}
	got := GteInt64(x, nil, nil, nil, 5)
	if len(got) != 2 {
		t.Fatalf("GteInt64 selected %d, want 2", len(got))
	}
}

func TestBetweenInt64Inclusive(t *testing.T) {
	x := []int64{1, 5, 3, 7, 9}
	got := BetweenInt64(x, nil, nil, nil, 3, 7)
	if len(got) != 3 {
		t.Fatalf("BetweenInt64 selected %d, want 3 (rows with values 5,3,7)", len(got))
	}
}

func TestBetweenInt64WithNulls(t *testing.T) {
	x := []int64{1, 5, 3, 7}
	v := NewValidity(4)
	SetInvalid(v, 1)
	got := BetweenInt64(x, v, nil, nil, 3, 7)
	if len(got) != 2 {
		t.Fatalf("BetweenInt64 with nulls selected %d, want 2 (skip invalid row 1)", len(got))
	}
}

func TestBetweenInt64RejectsInvalidRange(t *testing.T) {
	got := BetweenInt64([]int64{1, 2, 3}, nil, nil, nil, 10, 1)
	if len(got) != 0 {
		t.Fatalf("BetweenInt64 invalid range selected %d rows, want 0", len(got))
	}
}

func TestMinMaxInt64AllValid(t *testing.T) {
	x := []int64{4, -1, 99, 5}
	min, max, ok := MinMaxInt64(x, nil, nil)
	if !ok {
		t.Fatal("MinMaxInt64 returned ok=false")
	}
	if min != -1 || max != 99 {
		t.Fatalf("MinMaxInt64 = (%d, %d), want (-1, 99)", min, max)
	}
}

func TestMinMaxInt64Empty(t *testing.T) {
	_, _, ok := MinMaxInt64(nil, nil, nil)
	if ok {
		t.Fatal("MinMaxInt64 on empty should return ok=false")
	}
}

func TestMinMaxInt64WithNulls(t *testing.T) {
	x := []int64{1, 999, 3, 4}
	v := NewValidity(4)
	SetInvalid(v, 1)
	min, max, ok := MinMaxInt64(x, v, nil)
	if !ok {
		t.Fatal("MinMaxInt64 returned ok=false")
	}
	if max == 999 {
		t.Fatal("MinMaxInt64 should ignore invalid row")
	}
	if min != 1 || max != 4 {
		t.Fatalf("MinMaxInt64 = (%d, %d), want (1, 4)", min, max)
	}
}

func TestSumInt64AllValid(t *testing.T) {
	sum, count, overflow := SumInt64([]int64{1, 2, -3, 10}, nil, nil)
	if overflow || sum != 10 || count != 4 {
		t.Fatalf("SumInt64 = (%d, %d, %v), want (10, 4, false)", sum, count, overflow)
	}
}

func TestSumInt64WithNulls(t *testing.T) {
	x := []int64{1, 100, 3}
	v := NewValidity(len(x))
	SetInvalid(v, 1)
	sum, count, overflow := SumInt64(x, v, nil)
	if overflow || sum != 4 || count != 2 {
		t.Fatalf("SumInt64 = (%d, %d, %v), want (4, 2, false)", sum, count, overflow)
	}
}

func TestSumInt64Overflow(t *testing.T) {
	sum, _, overflow := SumInt64([]int64{math.MaxInt64, 1}, nil, nil)
	if !overflow || sum != 0 {
		t.Fatalf("SumInt64 overflow = (%d, %v), want (0, true)", sum, overflow)
	}
}

func TestSumInt64WithSelection(t *testing.T) {
	sum, count, overflow := SumInt64([]int64{1, 2, 3, 4}, nil, Sel{1, 3})
	if overflow || sum != 6 || count != 2 {
		t.Fatalf("SumInt64 selected = (%d, %d, %v), want (6, 2, false)", sum, count, overflow)
	}
}

func TestSumInt64SelectedMask(t *testing.T) {
	mask := NewSelectionMask(5)
	mask.Set(1)
	mask.Set(3)
	sum, count, overflow := SumInt64Selected([]int64{10, 2, 10, 4, 10}, mask)
	if overflow || sum != 6 || count != mask.PopCount() {
		t.Fatalf("SumInt64Selected = (%d, %d, %v), want (6, %d, false)", sum, count, overflow, mask.PopCount())
	}
}

func TestEqInt32AllValid(t *testing.T) {
	got := EqInt32([]int32{1, 2, 2, 3}, nil, nil, nil, 2)
	assertSel(t, got, Sel{1, 2})
}

func TestBetweenInt32AllValid(t *testing.T) {
	got := BetweenInt32([]int32{1, 2, 3, 4}, nil, nil, nil, 2, 3)
	assertSel(t, got, Sel{1, 2})
}

func TestBetweenInt32RejectsInvalidRange(t *testing.T) {
	got := BetweenInt32([]int32{1, 2, 3}, nil, nil, nil, 10, 1)
	if len(got) != 0 {
		t.Fatalf("BetweenInt32 invalid range selected %d rows, want 0", len(got))
	}
}

func TestEqInt16AllValid(t *testing.T) {
	got := EqInt16([]int16{1, 2, 2, 3}, nil, nil, nil, 2)
	assertSel(t, got, Sel{1, 2})
}

func TestEqInt64ReusesOutputBuffer(t *testing.T) {
	x := []int64{1, 2, 3}
	out := make(Sel, 0, 8)
	got := EqInt64(x, nil, nil, out, 2)
	if cap(got) < cap(out) {
		t.Fatalf("output capacity shrunk: got %d, had %d", cap(got), cap(out))
	}
}

func TestEqInt64SelectionAndValidity(t *testing.T) {
	x := []int64{1, 2, 3, 2, 1}
	v := NewValidity(5)
	SetInvalid(v, 3)
	sel := Sel{1, 3, 4}
	got := EqInt64(x, v, sel, nil, 2)
	if len(got) != 1 {
		t.Fatalf("EqInt64 sel+valid selected %d, want 1", len(got))
	}
	if got[0] != 1 {
		t.Errorf("got row %d, want 1", got[0])
	}
}

func assertSel(t *testing.T, got Sel, want Sel) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("selection = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("selection = %v, want %v", got, want)
		}
	}
}
