// Validity invariant tests covering nil-as-all-valid, null-count verification, and bounds checks.
// Validity is the boundary primitive between wire payloads and runtime nullity.
package vector

import "testing"

func TestValidity_NullCountMismatchErrors(t *testing.T) {
	v := NewValidity(10)
	v.SetInvalid(2)
	v.SetInvalid(5)
	buf := make([]byte, 8)
	v.MarshalLE(buf)
	if _, _, err := UnmarshalValidity(buf, 10, 2, nil); err != nil {
		t.Fatalf("matched null count should succeed: %v", err)
	}
	if _, _, err := UnmarshalValidity(buf, 10, 3, nil); err == nil {
		t.Fatal("mismatched null count must error")
	}
	if _, _, err := UnmarshalValidity(buf, 10, -1, nil); err == nil {
		t.Fatal("negative null count must error")
	}
	if _, _, err := UnmarshalValidity(buf, 10, 11, nil); err == nil {
		t.Fatal("null count > rows must error")
	}
}

func TestValidity_NilIsAllValid(t *testing.T) {
	var v Validity
	if !v.IsValid(0) || !v.IsValid(100) {
		t.Fatal("nil validity must report all rows valid")
	}
	if v.NullCount(64) != 0 {
		t.Fatal("nil validity null count must be 0")
	}
	out, n, err := UnmarshalValidity(nil, 64, 0, nil)
	if err != nil || n != 0 || out != nil {
		t.Fatalf("nullCount=0 must return nil bitmap: out=%v n=%d err=%v", out, n, err)
	}
}

func TestValidity_NullCountEqualsRowsMinusValid(t *testing.T) {
	v := NewValidity(100)
	for _, r := range []int{0, 1, 17, 63, 64, 99} {
		v.SetInvalid(r)
	}
	rows := 100
	valid := 0
	for i := range rows {
		if v.IsValid(i) {
			valid++
		}
	}
	if got := v.NullCount(rows); got+valid != rows {
		t.Fatalf("NullCount=%d + valid=%d != rows=%d", got, valid, rows)
	}
}

func TestValidity_LenZeroNullCountZeroEqualsNil(t *testing.T) {
	out, n, err := UnmarshalValidity(nil, 0, 0, nil)
	if err != nil || n != 0 || out != nil {
		t.Fatalf("rows=0 nullCount=0 must return nil: out=%v n=%d err=%v", out, n, err)
	}
	var v Validity
	if v.NullCount(0) != 0 {
		t.Fatal("nil validity must have zero null count at rows=0")
	}
}
