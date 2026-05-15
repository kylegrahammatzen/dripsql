// Cascade picks the smallest-encoded codec for a Vec via Estimate. Ties favor Plain.
// Flate / Zstd are excluded by default and only used when types.CompressionPolicy opts in.
package codec

import (
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

// Plain is always last so Pick's <= tie-break selects it on ties.
func Candidates(k types.VecKind) []Codec {
	base := []Codec{mustLookup(types.EncodingConstant)}
	switch {
	case k.IsVarBytes():
		base = append(base, mustLookup(types.EncodingDictionary))
	case k.IsFORPackable() && k.FixedWidth() == 8:
		base = append(base,
			mustLookup(types.EncodingSequence),
			mustLookup(types.EncodingFORBitPack),
			mustLookup(types.EncodingDeltaBitPack))
	case k.IsFORPackable():
		base = append(base,
			mustLookup(types.EncodingFORBitPack),
			mustLookup(types.EncodingDeltaBitPack))
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

// Pick chooses the candidate with the smallest Estimate. Ties favor the
// later-listed candidate, which is Plain by convention. The boolean is false
// only when no candidate accepts the Vec, which should not happen since Plain
// accepts every supported kind.
func Pick(v types.Vec) (Codec, int, bool) {
	var best Codec
	bestSize := -1
	for _, c := range Candidates(v.Kind) {
		size, ok := c.Estimate(v)
		if !ok {
			continue
		}
		if bestSize == -1 || size <= bestSize {
			best = c
			bestSize = size
		}
	}
	if bestSize == -1 {
		return nil, 0, false
	}
	return best, bestSize, true
}
