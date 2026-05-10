package types

import "testing"

func TestValidityWordsForRowCount(t *testing.T) {
	cases := []struct {
		n    int
		want int
	}{
		{0, 0}, {1, 1}, {63, 1}, {64, 1}, {65, 2}, {127, 2}, {128, 2}, {129, 3}, {1000, 16}, {2048, 32},
	}
	for _, c := range cases {
		if got := ValidityWords(c.n); got != c.want {
			t.Errorf("ValidityWords(%d) = %d, want %d", c.n, got, c.want)
		}
	}
	if got := ValidityWords(-1); got != 0 {
		t.Errorf("ValidityWords(-1) = %d, want 0", got)
	}
}

func TestValidityIsValidAllTrue(t *testing.T) {
	v := NewValidity(64)
	for i := 0; i < 64; i++ {
		if !IsValid(v, i) {
			t.Errorf("row %d invalid in fresh validity", i)
		}
	}
}

func TestValidityIsValidAllFalse(t *testing.T) {
	v := make(Validity, 1)
	for i := 0; i < 64; i++ {
		if IsValid(v, i) {
			t.Errorf("row %d valid in zero validity", i)
		}
	}
}

func TestValidityNilMeansAllValid(t *testing.T) {
	if !IsValid(nil, 0) || !IsValid(nil, 4096) {
		t.Fatal("nil validity must report every row as valid")
	}
	if !Validity(nil).IsAllValid() {
		t.Fatal("nil Validity.IsAllValid must be true")
	}
}

func TestValiditySetInvalidIdempotent(t *testing.T) {
	v := NewValidity(8)
	SetInvalid(v, 3)
	SetInvalid(v, 3)
	if IsValid(v, 3) {
		t.Fatal("row 3 should be invalid after SetInvalid")
	}
	if !IsValid(v, 0) || !IsValid(v, 4) {
		t.Fatal("SetInvalid should not affect other rows")
	}
}

func TestValidityMixedPattern(t *testing.T) {
	v := NewValidity(8)
	SetInvalid(v, 1)
	SetInvalid(v, 4)
	want := []bool{true, false, true, true, false, true, true, true}
	for i, exp := range want {
		if IsValid(v, i) != exp {
			t.Errorf("row %d valid=%v, want %v", i, IsValid(v, i), exp)
		}
	}
}

func TestValidityNullCount(t *testing.T) {
	v := NewValidity(8)
	if NullCount(v, 8) != 0 {
		t.Fatal("fresh validity should have 0 nulls")
	}
	SetInvalid(v, 1)
	SetInvalid(v, 4)
	SetInvalid(v, 7)
	if got := NullCount(v, 8); got != 3 {
		t.Errorf("NullCount = %d, want 3", got)
	}
	if got := ValidCount(v, 8); got != 5 {
		t.Errorf("ValidCount = %d, want 5", got)
	}
	if NullCount(nil, 100) != 0 {
		t.Fatal("nil validity must report 0 nulls")
	}
}

func TestValidityCountAcrossWordBoundary(t *testing.T) {
	v := NewValidity(128)
	SetInvalid(v, 0)
	SetInvalid(v, 63)
	SetInvalid(v, 64)
	SetInvalid(v, 127)
	if got := NullCount(v, 128); got != 4 {
		t.Errorf("NullCount across word boundary = %d, want 4", got)
	}
}

func TestValidityFillValidPartialWord(t *testing.T) {
	v := make(Validity, ValidityWords(70))
	FillValid(v, 70)
	for i := 0; i < 70; i++ {
		if !IsValid(v, i) {
			t.Errorf("row %d invalid after FillValid(70)", i)
		}
	}
	// Bits beyond rowCount should not be set.
	expected := (uint64(1) << 6) - 1
	if v[1] != expected {
		t.Fatalf("FillValid produced word %x, want %x (only bits 0..5)", v[1], expected)
	}
}

func TestValiditySetValidIdempotent(t *testing.T) {
	v := NewValidity(8)
	SetInvalid(v, 5)
	SetValid(v, 5)
	SetValid(v, 5)
	if !IsValid(v, 5) {
		t.Fatal("SetValid did not restore validity")
	}
}
