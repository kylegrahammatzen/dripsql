// codec.go invariant tests: registry lookup found/unknown and EncInvalid rejection.
// Self-registration is exercised indirectly by plain_test.go via init().
package codec

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// Estimate is the shared test helper for exercising a single codec's Encode-or-ErrSkip path.
func Estimate(c Codec, v vector.Vec) (int, bool) {
	p, err := c.Encode(v, &EncodeContext{Scratch: NewScratchPool()})
	if err != nil {
		return 0, false
	}
	return len(p), true
}

func TestLookup_KnownAndUnknown(t *testing.T) {
	c, err := Lookup(schema.EncPlain)
	if err != nil {
		t.Fatalf("Lookup(Flat): %v", err)
	}
	if c.Encoding() != schema.EncPlain {
		t.Fatalf("Lookup returned codec for %v not Flat", c.Encoding())
	}
	if _, err := Lookup(schema.Encoding(250)); err == nil {
		t.Fatal("Lookup of unregistered codec must error")
	}
}

type autoCodec struct{}

func (autoCodec) Encoding() schema.Encoding                                  { return schema.EncInvalid }
func (autoCodec) Encode(vector.Vec, *EncodeContext) ([]byte, error)          { return nil, nil }
func (autoCodec) Decode([]byte, vector.VecKind, int, int, *vector.Vec) error { return nil }

func TestRegister_RejectsEncInvalid(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("Register(EncInvalid) must panic")
		}
	}()
	Register(autoCodec{})
}
