// Encoding invariant tests: wire byte 0 reserved, unknown bytes rejected, round-trip.
// Wire safety matters because v4 segment files lean on these mappings.
package schema

import "testing"

func TestEncoding_WireRejectsAutoAndUnknown(t *testing.T) {
	if _, ok := EncodingFromWire(0); ok {
		t.Fatal("wire byte 0 must be rejected (EncodingAuto)")
	}
	for b := byte(13); b < 16; b++ {
		if _, ok := EncodingFromWire(b); ok {
			t.Fatalf("wire byte %d must be rejected as unknown", b)
		}
	}
	for e := EncodingFlat; e <= EncodingPcodec; e++ {
		got, ok := EncodingFromWire(e.Wire())
		if !ok || got != e {
			t.Fatalf("round-trip failed for %v: got=%v ok=%v", e, got, ok)
		}
	}
	defer func() {
		if recover() == nil {
			t.Fatal("EncodingAuto.Wire() must panic")
		}
	}()
	_ = EncodingAuto.Wire()
}
