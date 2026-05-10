package types

import "testing"

func textVarBytes(values []string) VarBytes {
	v := NewVarBytes(len(values), 0)
	for i, s := range values {
		v.AppendString(i, s)
	}
	return v
}

func TestEqBytesExact(t *testing.T) {
	v := textVarBytes([]string{"foo", "bar", "foo", "baz"})
	got := EqBytes(v, nil, nil, nil, []byte("foo"))
	if len(got) != 2 {
		t.Fatalf("EqBytes selected %d, want 2", len(got))
	}
	if got[0] != 0 || got[1] != 2 {
		t.Errorf("EqBytes selected %v, want [0 2]", got)
	}
}

func TestEqBytesEmpty(t *testing.T) {
	v := textVarBytes([]string{"foo", "", "bar", ""})
	got := EqBytes(v, nil, nil, nil, []byte(""))
	if len(got) != 2 {
		t.Fatalf("EqBytes empty selected %d, want 2", len(got))
	}
}

func TestEqBytesLengthMismatchSkips(t *testing.T) {
	v := textVarBytes([]string{"foo", "foofoo", "fo"})
	got := EqBytes(v, nil, nil, nil, []byte("foo"))
	if len(got) != 1 {
		t.Fatalf("EqBytes selected %d, want 1", len(got))
	}
}

func TestEqBytesWithNulls(t *testing.T) {
	v := textVarBytes([]string{"foo", "foo", "foo"})
	valid := NewValidity(3)
	SetInvalid(valid, 1)
	got := EqBytes(v, valid, nil, nil, []byte("foo"))
	if len(got) != 2 {
		t.Fatalf("EqBytes with nulls selected %d, want 2", len(got))
	}
}

func TestEqBytesWithSelection(t *testing.T) {
	v := textVarBytes([]string{"foo", "bar", "foo", "baz", "foo"})
	got := EqBytes(v, nil, Sel{1, 2, 4}, nil, []byte("foo"))
	if len(got) != 2 {
		t.Fatalf("EqBytes sel selected %d, want 2", len(got))
	}
}

func TestEqBytesUnicode(t *testing.T) {
	v := textVarBytes([]string{"café", "cafe", "café"})
	got := EqBytes(v, nil, nil, nil, []byte("café"))
	if len(got) != 2 {
		t.Fatalf("EqBytes unicode selected %d, want 2", len(got))
	}
}

func TestLengthBytes(t *testing.T) {
	v := textVarBytes([]string{"café"})
	if got := LengthBytes(v, 0); got != 5 {
		t.Fatalf("LengthBytes = %d, want 5", got)
	}
}

func TestLengthRunes(t *testing.T) {
	v := textVarBytes([]string{"café"})
	if got := LengthRunes(v, 0); got != 4 {
		t.Fatalf("LengthRunes = %d, want 4", got)
	}
}

func TestCompareBytesLess(t *testing.T) {
	v := textVarBytes([]string{"alpha"})
	if got := CompareBytes(v, 0, []byte("beta")); got >= 0 {
		t.Fatalf("CompareBytes = %d, want < 0", got)
	}
}

func TestCompareBytesEqual(t *testing.T) {
	v := textVarBytes([]string{"alpha"})
	if got := CompareBytes(v, 0, []byte("alpha")); got != 0 {
		t.Fatalf("CompareBytes = %d, want 0", got)
	}
}

func TestPrefixBytesEmptyMatchesAll(t *testing.T) {
	v := textVarBytes([]string{"foo", "bar", "baz"})
	got := PrefixBytes(v, nil, nil, nil, nil)
	if len(got) != 3 {
		t.Fatalf("PrefixBytes empty prefix selected %d, want 3", len(got))
	}
}

func TestPrefixBytesMatchesPrefix(t *testing.T) {
	v := textVarBytes([]string{"foobar", "foobaz", "qux", "foo"})
	got := PrefixBytes(v, nil, nil, nil, []byte("foo"))
	if len(got) != 3 {
		t.Fatalf("PrefixBytes selected %d, want 3", len(got))
	}
}

func TestPrefixBytesWithNulls(t *testing.T) {
	v := textVarBytes([]string{"foobar", "foobaz", "foo"})
	valid := NewValidity(3)
	SetInvalid(valid, 1)
	got := PrefixBytes(v, valid, nil, nil, []byte("foo"))
	if len(got) != 2 {
		t.Fatalf("PrefixBytes with nulls selected %d, want 2", len(got))
	}
}

func TestPrefixBytesWithSelection(t *testing.T) {
	v := textVarBytes([]string{"foobar", "qux", "foobaz"})
	got := PrefixBytes(v, nil, Sel{1, 2}, nil, []byte("foo"))
	if len(got) != 1 {
		t.Fatalf("PrefixBytes sel selected %d, want 1", len(got))
	}
}
