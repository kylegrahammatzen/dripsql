// Cascade picks the smallest-encoded codec for a Vec via Encode-or-ErrSkip.
// Last candidate wins ties. Plain is always last so ties favor it.
package codec

import (
	"errors"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// Plain is last so the <= tie-break selects it on ties.
func Candidates(k vector.VecKind) []Codec {
	base := []Codec{mustLookup(schema.EncodingConstant)}
	switch {
	case k.IsVarBytes():
		base = append(base, mustLookup(schema.EncodingDictionary), mustLookup(schema.EncodingFSST))
	case k.IsFORPackable() && k.FixedWidth() == 8:
		base = append(base,
			mustLookup(schema.EncodingSequence),
			mustLookup(schema.EncodingFORBitPack),
			mustLookup(schema.EncodingDeltaBitPack),
			mustLookup(schema.EncodingPcodec))
	case k.IsFORPackable():
		base = append(base,
			mustLookup(schema.EncodingFORBitPack),
			mustLookup(schema.EncodingDeltaBitPack),
			mustLookup(schema.EncodingPcodec))
	case k == vector.VecFloat32 || k == vector.VecFloat64:
		base = append(base, mustLookup(schema.EncodingALP))
		if k == vector.VecFloat64 {
			base = append(base, mustLookup(schema.EncodingALPRD), mustLookup(schema.EncodingPcodec))
		}
	}
	return append(base, mustLookup(schema.EncodingFlat))
}

func mustLookup(e schema.Encoding) Codec {
	c, err := Lookup(e)
	if err != nil {
		panic(err)
	}
	return c
}

// Encode runs every applicable candidate and keeps the smallest payload.
func Encode(v vector.Vec, ctx *EncodeContext) (schema.Encoding, []byte, error) {
	if ctx == nil {
		ctx = &EncodeContext{Scratch: NewScratchPool()}
	}
	if ctx.Scratch == nil {
		ctx.Scratch = NewScratchPool()
	}
	sp := ctx.Scratch
	sp.best = sp.best[:0]
	haveBest := false
	var bestEnc schema.Encoding

	for _, c := range Candidates(v.Kind) {
		sp.trial = sp.trial[:0]
		p, err := c.Encode(v, ctx)
		if errors.Is(err, ErrSkip) {
			continue
		}
		if err != nil {
			return 0, nil, fmt.Errorf("cascade encode: %v: %w", c.Encoding(), err)
		}
		if !haveBest || len(p) <= len(sp.best) {
			bestEnc = c.Encoding()
			sp.best = append(sp.best[:0], p...)
			haveBest = true
		}
		sp.SaveTrial(p)
	}
	if !haveBest {
		return 0, nil, fmt.Errorf("cascade encode: no codec accepted kind %v", v.Kind)
	}
	return bestEnc, sp.best, nil
}

// Thin wrapper for callers that only need the chosen codec and a size.
func Pick(v vector.Vec) (Codec, int, bool) {
	enc, payload, err := Encode(v, nil)
	if err != nil {
		return nil, 0, false
	}
	c, err := Lookup(enc)
	if err != nil {
		return nil, 0, false
	}
	return c, len(payload), true
}
