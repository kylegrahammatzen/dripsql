// codec.go invariant tests: registry lookup found/unknown and EncodingAuto rejection.
// Self-registration is exercised indirectly by plain_test.go via init().
package codec

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestLookup_KnownAndUnknown(t *testing.T) {
	c, err := Lookup(schema.EncodingFlat)
	if err != nil {
		t.Fatalf("Lookup(Flat): %v", err)
	}
	if c.Encoding() != schema.EncodingFlat {
		t.Fatalf("Lookup returned codec for %v not Flat", c.Encoding())
	}
	if _, err := Lookup(schema.Encoding(250)); err == nil {
		t.Fatal("Lookup of unregistered codec must error")
	}
}

type autoCodec struct{}

func (autoCodec) Encoding() schema.Encoding                                  { return schema.EncodingAuto }
func (autoCodec) Encode(vector.Vec, *EncodeContext) ([]byte, error)          { return nil, nil }
func (autoCodec) Decode([]byte, vector.VecKind, int, int, *vector.Vec) error { return nil }

func TestRegister_RejectsEncodingAuto(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Register(EncodingAuto) must panic")
		}
	}()
	Register(autoCodec{})
}
