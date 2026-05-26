package vector

import "testing"

func TestCmpOrdered(t *testing.T) {
	if got := CmpOrdered[int64](1, 2); got != -1 {
		t.Fatalf("CmpOrdered(1,2) = %d, want -1", got)
	}
	if got := CmpOrdered[int64](2, 1); got != 1 {
		t.Fatalf("CmpOrdered(2,1) = %d, want 1", got)
	}
	if got := CmpOrdered[int64](2, 2); got != 0 {
		t.Fatalf("CmpOrdered(2,2) = %d, want 0", got)
	}
	if got := CmpOrdered("a", "b"); got != -1 {
		t.Fatalf("CmpOrdered(a,b) = %d, want -1", got)
	}
	if got := CmpOrdered(2.5, 1.5); got != 1 {
		t.Fatalf("CmpOrdered(2.5,1.5) = %d, want 1", got)
	}
}

func TestCmpBool(t *testing.T) {
	if CmpBool(false, true) != -1 {
		t.Fatal("false<true")
	}
	if CmpBool(true, false) != 1 {
		t.Fatal("true>false")
	}
	if CmpBool(true, true) != 0 {
		t.Fatal("true==true")
	}
}

func TestDivIntZero(t *testing.T) {
	if _, err := DivInt[int64](10, 0); err == nil {
		t.Fatal("divide by zero must error")
	}
	got, err := DivInt[int64](10, 3)
	if err != nil || got != 3 {
		t.Fatalf("DivInt(10,3) = %d,%v want 3,nil", got, err)
	}
}

func TestModIntZero(t *testing.T) {
	if _, err := ModInt[int64](10, 0); err == nil {
		t.Fatal("mod by zero must error")
	}
	got, err := ModInt[int64](10, 3)
	if err != nil || got != 1 {
		t.Fatalf("ModInt(10,3) = %d,%v want 1,nil", got, err)
	}
}

func TestDivFloatZero(t *testing.T) {
	if _, err := DivFloat(1.0, 0); err == nil {
		t.Fatal("float divide by zero must error")
	}
	got, err := DivFloat(10, 4)
	if err != nil || got != 2.5 {
		t.Fatalf("DivFloat(10,4) = %v,%v want 2.5,nil", got, err)
	}
}

func TestFilterOrdered_Int64Equal(t *testing.T) {
	col := []int64{10, 20, 30, 20, 50}
	in := NewSelectionMask(5)
	in.FillAll()
	out := NewSelectionMask(5)
	n := FilterOrdered(col, nil, int64(20), FilterEqual, in, &out)
	if n != 2 {
		t.Fatalf("popcount = %d, want 2", n)
	}
	for i, want := range []bool{false, true, false, true, false} {
		if out.IsSet(i) != want {
			t.Fatalf("row %d set=%v want %v", i, out.IsSet(i), want)
		}
	}
}

func TestFilterOrdered_Int64Less(t *testing.T) {
	col := []int64{1, 5, 10, 15, 20}
	in := NewSelectionMask(5)
	in.FillAll()
	out := NewSelectionMask(5)
	n := FilterOrdered(col, nil, int64(10), FilterLess, in, &out)
	if n != 2 {
		t.Fatalf("popcount = %d, want 2", n)
	}
	if !out.IsSet(0) || !out.IsSet(1) || out.IsSet(2) {
		t.Fatalf("expected rows 0,1 set, row 2 unset; got %v %v %v", out.IsSet(0), out.IsSet(1), out.IsSet(2))
	}
}

func TestFilterOrdered_NullsExcluded(t *testing.T) {
	col := []int64{10, 50, 30, 40}
	v := NewValidity(4)
	v.SetInvalid(1)
	in := NewSelectionMask(4)
	in.FillAll()
	out := NewSelectionMask(4)
	n := FilterOrdered(col, v, int64(20), FilterGreater, in, &out)
	if n != 2 {
		t.Fatalf("popcount = %d, want 2 (rows 2 and 3; row 1 is null even though 50>20)", n)
	}
	if out.IsSet(1) {
		t.Fatalf("null row 1 must not be selected")
	}
	if !out.IsSet(2) || !out.IsSet(3) {
		t.Fatalf("rows 2 and 3 should pass; got %v %v", out.IsSet(2), out.IsSet(3))
	}
}

func TestFilterOrdered_IncomingSelHonored(t *testing.T) {
	col := []int64{10, 20, 30, 40}
	in := NewSelectionMask(4)
	in.Set(0)
	in.Set(2)
	out := NewSelectionMask(4)
	n := FilterOrdered(col, nil, int64(0), FilterGreater, in, &out)
	if n != 2 {
		t.Fatalf("popcount = %d, want 2 (rows 0,2)", n)
	}
	if out.IsSet(1) || out.IsSet(3) {
		t.Fatalf("rows 1,3 must not appear; incoming sel excluded them")
	}
}

func TestFilterOrdered_TwoWordRows(t *testing.T) {
	const rows = 130
	col := make([]int64, rows)
	for i := range col {
		col[i] = int64(i)
	}
	in := NewSelectionMask(rows)
	in.FillAll()
	out := NewSelectionMask(rows)
	n := FilterOrdered(col, nil, int64(100), FilterGreaterEqual, in, &out)
	if n != 30 {
		t.Fatalf("popcount = %d, want 30 (rows 100..129)", n)
	}
	for i := 100; i < rows; i++ {
		if !out.IsSet(i) {
			t.Fatalf("row %d should pass", i)
		}
	}
	for i := range 100 {
		if out.IsSet(i) {
			t.Fatalf("row %d should not pass", i)
		}
	}
}

func TestFilterBytes_Equal(t *testing.T) {
	vb := NewVarBytes(4, 0)
	vb.AppendString(0, "alice")
	vb.AppendString(1, "bob")
	vb.AppendString(2, "alice")
	vb.AppendString(3, "carol")
	in := NewSelectionMask(4)
	in.FillAll()
	out := NewSelectionMask(4)
	n := FilterBytes(&vb, nil, []byte("alice"), FilterEqual, in, &out)
	if n != 2 {
		t.Fatalf("popcount = %d, want 2", n)
	}
	if !out.IsSet(0) || out.IsSet(1) || !out.IsSet(2) || out.IsSet(3) {
		t.Fatalf("wrong rows: %v %v %v %v", out.IsSet(0), out.IsSet(1), out.IsSet(2), out.IsSet(3))
	}
}

func TestFilterBytes_NotEqualWithNulls(t *testing.T) {
	vb := NewVarBytes(3, 0)
	vb.AppendString(0, "x")
	vb.AppendString(1, "y")
	vb.AppendString(2, "x")
	v := NewValidity(3)
	v.SetInvalid(1)
	in := NewSelectionMask(3)
	in.FillAll()
	out := NewSelectionMask(3)
	n := FilterBytes(&vb, v, []byte("x"), FilterNotEqual, in, &out)
	if n != 0 {
		t.Fatalf("popcount = %d, want 0 (row 1 is null, rows 0/2 match x)", n)
	}
}

func TestBetweenOrdered_Int64(t *testing.T) {
	col := []int64{1, 5, 10, 15, 20, 25}
	in := NewSelectionMask(6)
	in.FillAll()
	out := NewSelectionMask(6)
	n := BetweenOrdered(col, nil, int64(5), int64(20), in, &out)
	if n != 4 {
		t.Fatalf("popcount = %d, want 4", n)
	}
	for i, want := range []bool{false, true, true, true, true, false} {
		if out.IsSet(i) != want {
			t.Fatalf("row %d set=%v want %v", i, out.IsSet(i), want)
		}
	}
}

func TestFilterOrdered_Strings(t *testing.T) {
	col := []string{"alice", "bob", "carol", "dave"}
	in := NewSelectionMask(4)
	in.FillAll()
	out := NewSelectionMask(4)
	n := FilterOrdered(col, nil, "carol", FilterLess, in, &out)
	if n != 2 {
		t.Fatalf("popcount = %d, want 2", n)
	}
	if !out.IsSet(0) || !out.IsSet(1) || out.IsSet(2) {
		t.Fatalf("expected alice, bob; got %v %v %v", out.IsSet(0), out.IsSet(1), out.IsSet(2))
	}
}
