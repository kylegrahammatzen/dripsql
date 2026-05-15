// Codec encodes/decodes a column page between wire bytes and a types.Vec.
// Codecs self-register via init() and are dispatched by types.Encoding lookup. No fallback switch.
package codec

import (
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type Codec interface {
	Encoding() types.Encoding
	Encode(v types.Vec, scratch []byte) (payload []byte, err error)
	Decode(payload []byte, kind types.VecKind, rows int, nullCount int, dst *types.Vec) error
	Estimate(v types.Vec) (int, bool)
}

var registry = map[types.Encoding]Codec{}

func Register(c Codec) {
	e := c.Encoding()
	if e == types.EncodingAuto {
		panic("codec: cannot register EncodingAuto sentinel")
	}
	if _, dup := registry[e]; dup {
		panic(fmt.Sprintf("codec: %v already registered", e))
	}
	registry[e] = c
}

func Lookup(e types.Encoding) (Codec, error) {
	c, ok := registry[e]
	if !ok {
		return nil, fmt.Errorf("codec: no codec registered for %v", e)
	}
	return c, nil
}
