// codec.go invariant tests: registry lookup found/unknown and EncodingAuto rejection.
// Self-registration is exercised indirectly by plain_test.go via init().
package codec

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestLookup_KnownAndUnknown(t *testing.T) {
	c, err := Lookup(types.EncodingFlat)
	if err != nil {
		t.Fatalf("Lookup(Flat): %v", err)
	}
	if c.Encoding() != types.EncodingFlat {
		t.Fatalf("Lookup returned codec for %v not Flat", c.Encoding())
	}
	if _, err := Lookup(types.Encoding(250)); err == nil {
		t.Fatal("Lookup of unregistered codec must error")
	}
}

type autoCodec struct{}

func (autoCodec) Encoding() types.Encoding                                 { return types.EncodingAuto }
func (autoCodec) Encode(types.Vec, *EncodeContext) ([]byte, error)         { return nil, nil }
func (autoCodec) Decode([]byte, types.VecKind, int, int, *types.Vec) error { return nil }

func TestRegister_RejectsEncodingAuto(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Register(EncodingAuto) must panic")
		}
	}()
	Register(autoCodec{})
}
