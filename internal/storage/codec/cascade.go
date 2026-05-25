// Cascade picks the smallest-encoded codec for a Vec via Encode-or-ErrSkip.
// Last candidate wins ties. Plain is always last so ties favor it.
package codec

import (
	"errors"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

// Plain is last so the <= tie-break selects it on ties.
func Candidates(k types.VecKind) []Codec {
	base := []Codec{mustLookup(types.EncodingConstant)}
	switch {
	case k.IsVarBytes():
		base = append(base, mustLookup(types.EncodingDictionary), mustLookup(types.EncodingFSST))
	case k.IsFORPackable() && k.FixedWidth() == 8:
		base = append(base,
			mustLookup(types.EncodingSequence),
			mustLookup(types.EncodingFORBitPack),
			mustLookup(types.EncodingDeltaBitPack),
			mustLookup(types.EncodingPcodec))
	case k.IsFORPackable():
		base = append(base,
			mustLookup(types.EncodingFORBitPack),
			mustLookup(types.EncodingDeltaBitPack),
			mustLookup(types.EncodingPcodec))
	case k == types.VecFloat32 || k == types.VecFloat64:
		base = append(base, mustLookup(types.EncodingALP))
		if k == types.VecFloat64 {
			base = append(base, mustLookup(types.EncodingALPRD))
		}
	}
	return append(base, mustLookup(types.EncodingFlat))
}

func mustLookup(e types.Encoding) Codec {
	c, err := Lookup(e)
	if err != nil {
		panic(err)
	}
	return c
}

// Encode runs every applicable candidate and keeps the smallest payload.
// The winner's bytes are copied into ctx.Scratch.best so later candidates can
// safely overwrite ctx.Scratch.trial. Last candidate wins ties.
func Encode(v types.Vec, ctx *EncodeContext) (types.Encoding, []byte, error) {
	if ctx == nil {
		ctx = &EncodeContext{Scratch: NewScratchPool()}
	}
	if ctx.Scratch == nil {
		ctx.Scratch = NewScratchPool()
	}
	sp := ctx.Scratch
	sp.best = sp.best[:0]
	haveBest := false
	var bestEnc types.Encoding

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
func Pick(v types.Vec) (Codec, int, bool) {
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
