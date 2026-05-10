package types

import "testing"

func TestTakeVarBytes(t *testing.T) {
	src := textVarBytes([]string{"a", "bb", "ccc", "dddd"})
	got := TakeVarBytes(VarBytes{}, src, Sel{2, 0, 3})

	if got.Rows() != 3 {
		t.Fatalf("Rows = %d, want 3", got.Rows())
	}
	if got.String(0) != "ccc" || got.String(1) != "a" || got.String(2) != "dddd" {
		t.Fatalf("TakeVarBytes got %q, %q, %q", got.String(0), got.String(1), got.String(2))
	}
}

func TestTakeVarBytesReusesBuffers(t *testing.T) {
	src := textVarBytes([]string{"a", "bb", "ccc"})
	dst := NewVarBytes(4, 32)
	offsetCap := cap(dst.Offsets)
	dataCap := cap(dst.Data)

	got := TakeVarBytes(dst, src, Sel{1, 2})

	if cap(got.Offsets) != offsetCap || cap(got.Data) != dataCap {
		t.Fatalf("TakeVarBytes did not reuse buffers: offsets cap %d/%d data cap %d/%d", cap(got.Offsets), offsetCap, cap(got.Data), dataCap)
	}
	if got.String(0) != "bb" || got.String(1) != "ccc" {
		t.Fatalf("TakeVarBytes got %q, %q", got.String(0), got.String(1))
	}
}
