// Encoding invariant tests: wire byte 0 reserved, unknown bytes rejected, round-trip.
// Wire safety matters because v4 segment files lean on these mappings.
package schema

import "testing"

func TestEncoding_WireRejectsAutoAndUnknown(t *testing.T) {
	if Encoding(0).Valid() {
		t.Fatal("wire byte 0 must be rejected (EncInvalid)")
	}
	for b := byte(13); b < 16; b++ {
		if Encoding(b).Valid() {
			t.Fatalf("wire byte %d must be rejected as unknown", b)
		}
	}
	for e := EncPlain; e <= EncPcodec; e++ {
		got := Encoding(e.Wire())
		if !got.Valid() || got != e {
			t.Fatalf("round-trip failed for %v: got=%v valid=%v", e, got, got.Valid())
		}
	}
	defer func() {
		if recover() == nil {
			t.Fatal("EncInvalid.Wire() must panic")
		}
	}()
	_ = EncInvalid.Wire()
}
