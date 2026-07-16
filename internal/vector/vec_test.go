// Vec invariant tests plus UUID parse and format round-trips.
// FixedBytes is the codec hot path. The size test pins the slim-Vec layout goal. Parse and String are the boundary between SQL literals and the 16-byte UUID value.
package vector

import (
	"encoding/binary"
	"strings"
	"testing"
	"unsafe"
)

func TestVec_FixedBytesRoundTrip(t *testing.T) {
	v := NewVec(VecInt64, 4)
	dst := v.I64()
	dst[0], dst[1], dst[2], dst[3] = 10, 20, 30, 40
	raw := v.FixedBytes()
	if len(raw) != 32 {
		t.Fatalf("FixedBytes len=%d want 32", len(raw))
	}
	for i, want := range []int64{10, 20, 30, 40} {
		got := int64(binary.LittleEndian.Uint64(raw[i*8 : i*8+8]))
		if got != want {
			t.Fatalf("row %d: got %d want %d", i, got, want)
		}
	}
	var src [32]byte
	for i, val := range []int64{100, 200, 300, 400} {
		binary.LittleEndian.PutUint64(src[i*8:i*8+8], uint64(val))
	}
	var v2 Vec
	v2.Kind = VecInt64
	copy(v2.EnsureFixedBytes(4), src[:])
	for i, want := range []int64{100, 200, 300, 400} {
		if got := v2.I64()[i]; got != want {
			t.Fatalf("row %d: got %d want %d", i, got, want)
		}
	}
}

func TestVec_VarRoundTrip(t *testing.T) {
	v := NewVarVec(VecText, 2, 0)
	vb := v.Var()
	vb.AppendString(0, "short")
	vb.AppendString(1, "longer than twelve")
	if vb.String(0) != "short" || vb.String(1) != "longer than twelve" {
		t.Fatalf("varbytes round-trip: %q %q", vb.String(0), vb.String(1))
	}
}

func TestVec_HeaderSize(t *testing.T) {
	got := unsafe.Sizeof(Vec{})
	if got > 56 {
		t.Fatalf("Vec header size %d exceeds 56-byte target", got)
	}
	t.Logf("Vec size: %d bytes", got)
}

func TestVec_EnsureFixedBytesReusesBackingWhenFits(t *testing.T) {
	v := NewVec(VecInt64, 100)
	ptr := unsafe.Pointer(unsafe.SliceData(v.I64()))
	v.EnsureFixedBytes(100)
	if unsafe.Pointer(unsafe.SliceData(v.I64())) != ptr {
		t.Fatal("backing reallocated unnecessarily when capacity sufficed")
	}
}

func TestVec_EnsureFixedBytesGrows(t *testing.T) {
	v := NewVec(VecInt64, 4)
	buf := v.EnsureFixedBytes(10)
	if len(buf) != 80 {
		t.Fatalf("EnsureFixedBytes returned len=%d want 80", len(buf))
	}
	if v.Len != 10 || v.Cap < 10 {
		t.Fatalf("Len=%d Cap=%d after grow", v.Len, v.Cap)
	}
}

func TestVec_ResetForDecode_WiderKindRealloc(t *testing.T) {
	v := NewVec(VecInt16, 1024)
	v.ResetForDecode(VecInt64)
	buf := v.EnsureFixedBytes(1024)
	if len(buf) != 1024*8 {
		t.Fatalf("after kind widen, byte buffer = %d, want %d", len(buf), 1024*8)
	}
	out := v.I64()
	for i := range out {
		out[i] = int64(i) * 1_000_000
	}
	for i, x := range v.I64() {
		if x != int64(i)*1_000_000 {
			t.Fatalf("row %d corrupt: got %d", i, x)
		}
	}
}

func TestUUID_StringRoundTrip(t *testing.T) {
	canonical := "550e8400-e29b-41d4-a716-446655440000"
	u, err := ParseUUID(canonical)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if u.String() != canonical {
		t.Fatalf("round-trip: got %q want %q", u.String(), canonical)
	}
	upper, err := ParseUUID(strings.ToUpper(canonical))
	if err != nil {
		t.Fatalf("uppercase parse: %v", err)
	}
	if upper != u {
		t.Fatal("uppercase parse must match canonical")
	}
	bare, err := ParseUUID("550e8400e29b41d4a716446655440000")
	if err != nil {
		t.Fatalf("bare parse: %v", err)
	}
	if bare != u {
		t.Fatal("bare hex must match canonical")
	}
}
