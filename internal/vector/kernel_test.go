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
