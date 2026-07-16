// Cascade tests covering kind-driven candidate sets and Pick choosing minimum-size.
package codec

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func encodingSet(codecs []Codec) map[schema.Encoding]bool {
	out := make(map[schema.Encoding]bool, len(codecs))
	for _, c := range codecs {
		out[c.Encoding()] = true
	}
	return out
}

func TestCascade_Candidates_VarBytes(t *testing.T) {
	got := encodingSet(Candidates(vector.VecText))
	want := []schema.Encoding{
		schema.EncConstant,
		schema.EncDict,
		schema.EncPlain,
	}
	for _, w := range want {
		if !got[w] {
			t.Fatalf("VarBytes candidates missing %v", w)
		}
	}
	if got[schema.EncFOR] || got[schema.EncSequence] {
		t.Fatal("VarBytes must not include FOR or Sequence")
	}
	if got[schema.EncFlate] || got[schema.EncZstd] {
		t.Fatal("Compression must be opt-in via policy, not auto-picked by Pick")
	}
}

func TestCascade_Candidates_FORPackableWidth8(t *testing.T) {
	got := encodingSet(Candidates(vector.VecInt64))
	for _, w := range []schema.Encoding{
		schema.EncConstant,
		schema.EncSequence,
		schema.EncFOR,
		schema.EncDelta,
		schema.EncPlain,
	} {
		if !got[w] {
			t.Fatalf("Int64 candidates missing %v", w)
		}
	}
	if got[schema.EncDict] {
		t.Fatal("Int64 must not include Dictionary")
	}
}

func TestCascade_Candidates_FORPackableWidth4(t *testing.T) {
	got := encodingSet(Candidates(vector.VecInt32))
	if got[schema.EncSequence] {
		t.Fatal("Int32 (width 4) must not include Sequence (width-8 only)")
	}
	if !got[schema.EncFOR] {
		t.Fatal("Int32 must include FOR")
	}
}

func TestCascade_Candidates_Bool(t *testing.T) {
	got := encodingSet(Candidates(vector.VecBool))
	if got[schema.EncFOR] || got[schema.EncDict] {
		t.Fatal("Bool must only have base candidates (Constant + Flat)")
	}
	for _, w := range []schema.Encoding{schema.EncConstant, schema.EncPlain} {
		if !got[w] {
			t.Fatalf("Bool candidates missing %v", w)
		}
	}
}

func TestCascade_Encode_ConstantBeatsAll(t *testing.T) {
	v := vector.NewVec(vector.VecInt64, 1000)
	for i := range v.I64() {
		v.I64()[i] = 42
	}
	enc, _, err := Encode(v, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if enc != schema.EncConstant {
		t.Fatalf("Encode should choose Constant for all-equal data, got %v", enc)
	}
}

func TestCascade_Encode_SequenceBeatsForArithmetic(t *testing.T) {
	v := vector.NewVec(vector.VecInt64, 1000)
	for i := range v.I64() {
		v.I64()[i] = int64(i)*5 + 7
	}
	enc, _, err := Encode(v, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if enc != schema.EncSequence {
		t.Fatalf("Encode should choose Sequence for arithmetic progression, got %v", enc)
	}
}

func TestCascade_Encode_DictionaryBeatsForLowCardinality(t *testing.T) {
	v := vector.NewVarVec(vector.VecText, 1000, 0)
	vb := v.Var()
	tokens := []string{"alpha", "beta", "gamma"}
	for i := range int(v.Len) {
		vb.AppendString(i, tokens[i%len(tokens)])
	}
	enc, _, err := Encode(v, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if enc != schema.EncDict {
		t.Fatalf("Encode should choose Dictionary for low-cardinality text, got %v", enc)
	}
}

func TestCascade_Encode_PlainFallback(t *testing.T) {
	v := vector.NewVec(vector.VecFloat64, 100)
	for i := range v.F64() {
		v.F64()[i] = float64(i) * 1.5
	}
	enc, payload, err := Encode(v, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if enc != schema.EncPlain {
		t.Fatalf("Encode should fall back to Flat for floats (no FOR), got %v", enc)
	}
	if len(payload) != 100*8 {
		t.Fatalf("Flat float64 size = %d, want %d", len(payload), 100*8)
	}
}

func TestCascade_Encode_TieFavorsPlain_ZeroRows(t *testing.T) {
	v := vector.NewVec(vector.VecInt64, 0)
	enc, payload, err := Encode(v, nil)
	if err != nil {
		t.Fatalf("Encode on zero-row column: %v", err)
	}
	if enc != schema.EncPlain {
		t.Fatalf("zero-row tie should resolve to Plain (Flat), got %v", enc)
	}
	if len(payload) != 0 {
		t.Fatalf("zero-row size = %d, want 0", len(payload))
	}
}

func TestCascade_FactsPickDictionaryForLowCardinalityText(t *testing.T) {
	v := vector.NewVarVec(vector.VecText, 2048, 0)
	vb := v.Var()
	tokens := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for i := range int(v.Len) {
		vb.AppendString(i, tokens[i%len(tokens)])
	}
	indices, entries, dictBytes, ok := buildDict(v)
	if !ok {
		t.Fatal("buildDict must accept low cardinality text")
	}
	facts := &PageFacts{Rows: int(v.Len), Kind: v.Kind, VarBytes: &VarBytesFacts{
		DictFits:    true,
		DictBytes:   dictBytes,
		DictEntries: entries,
		DictIndices: indices,
	}}
	enc, payload, err := Encode(v, &EncodeContext{Scratch: NewScratchPool(), Facts: facts})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if enc != schema.EncDict {
		t.Fatalf("Encode with dictionary facts = %v, want %v", enc, schema.EncDict)
	}
	c, err := Lookup(enc)
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	var dst vector.Vec
	if err := c.Decode(payload, v.Kind, int(v.Len), 0, &dst); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	for i := range int(v.Len) {
		if got, want := string(dst.Var().Bytes(i)), string(v.Var().Bytes(i)); got != want {
			t.Fatalf("row %d = %q, want %q", i, got, want)
		}
	}
}

func TestCascade_FactsKeepConstantWinnerForRepeatedText(t *testing.T) {
	v := vector.NewVarVec(vector.VecText, 2048, 0)
	vb := v.Var()
	for i := range int(v.Len) {
		vb.AppendString(i, "alpha")
	}
	indices, entries, dictBytes, ok := buildDict(v)
	if !ok {
		t.Fatal("buildDict must accept repeated text")
	}
	facts := &PageFacts{Rows: int(v.Len), Kind: v.Kind, VarBytes: &VarBytesFacts{
		DictFits:    true,
		DictBytes:   dictBytes,
		DictEntries: entries,
		DictIndices: indices,
	}}
	enc, _, err := Encode(v, &EncodeContext{Scratch: NewScratchPool(), Facts: facts})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if enc != schema.EncConstant {
		t.Fatalf("Encode with repeated text facts = %v, want %v", enc, schema.EncConstant)
	}
}
