// Cascade tests: kind-driven candidate sets and Pick chooses minimum-size.
package codec

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func encodingSet(codecs []Codec) map[types.Encoding]bool {
	out := make(map[types.Encoding]bool, len(codecs))
	for _, c := range codecs {
		out[c.Encoding()] = true
	}
	return out
}

func TestCascade_Candidates_VarBytes(t *testing.T) {
	got := encodingSet(Candidates(types.VecText))
	want := []types.Encoding{
		types.EncodingConstant,
		types.EncodingDictionary,
		types.EncodingFlat,
	}
	for _, w := range want {
		if !got[w] {
			t.Fatalf("VarBytes candidates missing %v", w)
		}
	}
	if got[types.EncodingFORBitPack] || got[types.EncodingSequence] {
		t.Fatal("VarBytes must not include FOR or Sequence")
	}
	if got[types.EncodingFlate] || got[types.EncodingZstd] {
		t.Fatal("Compression must be opt-in via policy, not auto-picked by Pick")
	}
}

func TestCascade_Candidates_FORPackableWidth8(t *testing.T) {
	got := encodingSet(Candidates(types.VecInt64))
	for _, w := range []types.Encoding{
		types.EncodingConstant,
		types.EncodingSequence,
		types.EncodingFORBitPack,
		types.EncodingDeltaBitPack,
		types.EncodingFlat,
	} {
		if !got[w] {
			t.Fatalf("Int64 candidates missing %v", w)
		}
	}
	if got[types.EncodingDictionary] {
		t.Fatal("Int64 must not include Dictionary")
	}
}

func TestCascade_Candidates_FORPackableWidth4(t *testing.T) {
	got := encodingSet(Candidates(types.VecInt32))
	if got[types.EncodingSequence] {
		t.Fatal("Int32 (width 4) must not include Sequence (width-8 only)")
	}
	if !got[types.EncodingFORBitPack] {
		t.Fatal("Int32 must include FOR")
	}
}

func TestCascade_Candidates_Bool(t *testing.T) {
	got := encodingSet(Candidates(types.VecBool))
	if got[types.EncodingFORBitPack] || got[types.EncodingDictionary] {
		t.Fatal("Bool must only have base candidates (Constant + Flat)")
	}
	for _, w := range []types.Encoding{types.EncodingConstant, types.EncodingFlat} {
		if !got[w] {
			t.Fatalf("Bool candidates missing %v", w)
		}
	}
}

func TestCascade_Pick_ConstantBeatsAll(t *testing.T) {
	v := types.NewVec(types.VecInt64, 1000)
	for i := range v.I64() {
		v.I64()[i] = 42
	}
	c, _, ok := Pick(v)
	if !ok {
		t.Fatal("Pick must succeed")
	}
	if c.Encoding() != types.EncodingConstant {
		t.Fatalf("Pick should choose Constant for all-equal data, got %v", c.Encoding())
	}
}

func TestCascade_Pick_SequenceBeatsForArithmetic(t *testing.T) {
	v := types.NewVec(types.VecInt64, 1000)
	for i := range v.I64() {
		v.I64()[i] = int64(i)*5 + 7
	}
	c, _, ok := Pick(v)
	if !ok {
		t.Fatal("Pick must succeed")
	}
	if c.Encoding() != types.EncodingSequence {
		t.Fatalf("Pick should choose Sequence for arithmetic progression, got %v", c.Encoding())
	}
}

func TestCascade_Pick_DictionaryBeatsForLowCardinality(t *testing.T) {
	v := types.NewVarVec(types.VecText, 1000, 0)
	vb := v.Var()
	tokens := []string{"alpha", "beta", "gamma"}
	for i := range int(v.Len) {
		vb.AppendString(i, tokens[i%len(tokens)])
	}
	c, _, ok := Pick(v)
	if !ok {
		t.Fatal("Pick must succeed")
	}
	if c.Encoding() != types.EncodingDictionary {
		t.Fatalf("Pick should choose Dictionary for low-cardinality text, got %v", c.Encoding())
	}
}

func TestCascade_Pick_PlainFallback(t *testing.T) {
	v := types.NewVec(types.VecFloat64, 100)
	for i := range v.F64() {
		v.F64()[i] = float64(i) * 1.5
	}
	c, size, ok := Pick(v)
	if !ok {
		t.Fatal("Pick must succeed")
	}
	if c.Encoding() != types.EncodingFlat {
		t.Fatalf("Pick should fall back to Flat for floats (no FOR), got %v", c.Encoding())
	}
	if size != 100*8 {
		t.Fatalf("Flat float64 size = %d, want %d", size, 100*8)
	}
}

func TestCascade_Pick_TieFavorsPlain_ZeroRows(t *testing.T) {
	v := types.NewVec(types.VecInt64, 0)
	c, size, ok := Pick(v)
	if !ok {
		t.Fatal("Pick must succeed on zero-row column")
	}
	if c.Encoding() != types.EncodingFlat {
		t.Fatalf("zero-row tie should resolve to Plain (Flat), got %v", c.Encoding())
	}
	if size != 0 {
		t.Fatalf("zero-row size = %d, want 0", size)
	}
}
