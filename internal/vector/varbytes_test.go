// VarBytes invariant tests: inline-vs-offset boundary at 12/13 bytes, prefix populated.
// These pin the German Strings layout that codec dictionaries depend on.
package vector

import "testing"

func TestVarBytes_BroadcastSharesDataForLongValue(t *testing.T) {
	v := NewVarBytes(1000, 0)
	long := make([]byte, 200)
	for i := range long {
		long[i] = byte(i)
	}
	v.Broadcast(long)
	if len(v.data) != 200 {
		t.Fatalf("Broadcast stored value %d times; data len=%d want 200", len(v.data)/200, len(v.data))
	}
	for i := range 1000 {
		got := v.Bytes(i)
		if len(got) != 200 || got[0] != 0 || got[199] != byte(199) {
			t.Fatalf("row %d broadcast result corrupted", i)
		}
	}
}

func TestVarBytes_BroadcastShortInline(t *testing.T) {
	v := NewVarBytes(5, 0)
	v.Broadcast([]byte("hello"))
	if len(v.data) != 0 {
		t.Fatalf("inline broadcast must not touch data buffer; got %d bytes", len(v.data))
	}
	for i := range 5 {
		if v.String(i) != "hello" {
			t.Fatalf("row %d: %q", i, v.String(i))
		}
	}
}

func TestVarBytes_Inline12_Long13(t *testing.T) {
	v := NewVarBytes(3, 0)
	v.AppendString(0, "hello")
	v.AppendBytes(1, []byte("twelvebytes_"))
	v.AppendString(2, "thirteenchars")
	if v.Len(0) != 5 || v.String(0) != "hello" {
		t.Fatalf("inline short: len=%d s=%q", v.Len(0), v.String(0))
	}
	if v.Len(1) != 12 || v.String(1) != "twelvebytes_" {
		t.Fatalf("inline boundary: len=%d s=%q", v.Len(1), v.String(1))
	}
	if v.Len(2) != 13 || v.String(2) != "thirteenchars" {
		t.Fatalf("long: len=%d s=%q", v.Len(2), v.String(2))
	}
	if v.Prefix(2) == 0 {
		t.Fatal("long-string prefix must be populated")
	}
}

func TestVarBytes_PrefixEqualityNoDeref(t *testing.T) {
	v := NewVarBytes(2, 0)
	v.AppendString(0, "alpha_long_value_one")
	v.AppendString(1, "beta_long_value_two_")
	if v.Prefix(0) == v.Prefix(1) {
		t.Fatalf("long strings with differing first 4 bytes must have differing prefixes: %x %x", v.Prefix(0), v.Prefix(1))
	}
	w := NewVarBytes(2, 0)
	w.AppendString(0, "same_prefix_long_a")
	w.AppendString(1, "same_prefix_long_b")
	if w.Prefix(0) != w.Prefix(1) {
		t.Fatalf("matching first 4 bytes must yield equal prefixes: %x %x", w.Prefix(0), w.Prefix(1))
	}
}
