package types

import "testing"

func TestVarBytesEmpty(t *testing.T) {
	v := NewVarBytes(0, 0)
	if len(v.Offsets) != 1 {
		t.Errorf("empty VarBytes Offsets len = %d, want 1", len(v.Offsets))
	}
	if v.Offsets[0] != 0 {
		t.Errorf("empty VarBytes Offsets[0] = %d, want 0", v.Offsets[0])
	}
}

func TestVarBytesAppendBytes(t *testing.T) {
	v := NewVarBytes(2, 16)
	v.AppendBytes(0, []byte("foo"))
	v.AppendBytes(1, []byte("bar"))
	if string(v.Bytes(0)) != "foo" {
		t.Errorf("Bytes(0) = %q, want %q", v.Bytes(0), "foo")
	}
	if string(v.Bytes(1)) != "bar" {
		t.Errorf("Bytes(1) = %q, want %q", v.Bytes(1), "bar")
	}
}

func TestVarBytesAppendString(t *testing.T) {
	v := NewVarBytes(2, 16)
	v.AppendString(0, "foo")
	v.AppendString(1, "bar")
	if got := v.String(1); got != "bar" {
		t.Errorf("String(1) = %q, want %q", got, "bar")
	}
}

func TestVarBytesString(t *testing.T) {
	v := NewVarBytes(1, 8)
	v.AppendString(0, "hello")
	if got := v.String(0); got != "hello" {
		t.Errorf("String(0) = %q, want %q", got, "hello")
	}
}

func TestVarBytesStringCopy(t *testing.T) {
	v := NewVarBytes(1, 8)
	v.AppendString(0, "hello")
	if got := v.StringCopy(0); got != "hello" {
		t.Errorf("StringCopy(0) = %q, want %q", got, "hello")
	}
}

func TestVarBytesBytesCapacityStopsAtRowEnd(t *testing.T) {
	v := NewVarBytes(2, 8)
	v.AppendString(0, "ab")
	v.AppendString(1, "cd")
	b := v.Bytes(0)
	if cap(b) != len(b) {
		t.Fatalf("Bytes cap = %d, len = %d; want cap == len", cap(b), len(b))
	}
}

func TestVarBytesBytesHasLimitedCapacity(t *testing.T) {
	v := NewVarBytes(2, 8)
	v.AppendString(0, "foo")
	v.AppendString(1, "bar")

	b := v.Bytes(0)
	if len(b) != 3 {
		t.Fatalf("Bytes len = %d, want 3", len(b))
	}
	if cap(b) != 3 {
		t.Fatalf("Bytes cap = %d, want 3", cap(b))
	}
}

func TestVarBytesRows(t *testing.T) {
	v := NewVarBytes(3, 0)
	if got := v.Rows(); got != 3 {
		t.Fatalf("Rows = %d, want 3", got)
	}
}

func TestVarBytesReset(t *testing.T) {
	v := NewVarBytes(2, 8)
	v.AppendString(0, "ab")
	v.AppendString(1, "cd")
	v.Reset()
	if len(v.Data) != 0 || v.Offsets[0] != 0 || v.Offsets[1] != 0 || v.Offsets[2] != 0 {
		t.Fatalf("after Reset = %#v", v)
	}
}

func TestVarBytesStringCopySurvivesReset(t *testing.T) {
	v := NewVarBytes(1, 8)
	v.AppendString(0, "hello")

	s := v.StringCopy(0)
	v.Reset()

	if s != "hello" {
		t.Fatalf("StringCopy after Reset = %q, want hello", s)
	}
}

func TestVarBytesEmptyRowReturnsEmptyString(t *testing.T) {
	v := NewVarBytes(1, 8)
	v.Offsets[1] = 0
	if got := v.String(0); got != "" {
		t.Errorf("empty row String = %q, want empty", got)
	}
}

func TestVarBytesOffsetsCount(t *testing.T) {
	for _, n := range []int{0, 1, 5, 100, StandardBatchRows} {
		v := NewVarBytes(n, 0)
		if len(v.Offsets) != n+1 {
			t.Errorf("NewVarBytes(%d).Offsets len = %d, want %d", n, len(v.Offsets), n+1)
		}
	}
}

func TestVarBytesValidatesOffsets(t *testing.T) {
	v := NewVarBytes(3, 0)
	v.Data = []byte("0123456789")
	v.Offsets = []uint32{0, 5, 3, 10}
	if err := validateVarBytes(v, 3); err == nil {
		t.Fatal("expected non-monotonic-offset error")
	}
}

func TestVarBytesValidatesFirstOffset(t *testing.T) {
	v := VarBytes{
		Offsets: []uint32{1, 2},
		Data:    []byte("ab"),
	}

	if err := validateVarBytes(v, 1); err == nil {
		t.Fatal("expected first-offset-not-zero error")
	}
}

func TestVarBytesValidatesEndOffset(t *testing.T) {
	v := NewVarBytes(2, 0)
	v.Data = []byte("hi")
	v.Offsets = []uint32{0, 1, 5}
	if err := validateVarBytes(v, 2); err == nil {
		t.Fatal("expected end-offset-exceeds-data error")
	}
}

func TestVarBytesValidatesNoTrailingData(t *testing.T) {
	v := VarBytes{
		Offsets: []uint32{0, 1},
		Data:    []byte("ab"),
	}

	if err := validateVarBytes(v, 1); err == nil {
		t.Fatal("expected trailing-data error")
	}
}

func TestVarBytesValidatesOffsetCount(t *testing.T) {
	v := VarBytes{Offsets: []uint32{0}}
	if err := validateVarBytes(v, 2); err == nil {
		t.Fatal("expected offset-count-mismatch error")
	}
}
