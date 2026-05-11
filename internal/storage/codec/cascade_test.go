package codec

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestTextCandidatesPickDictionaryBeforePlain(t *testing.T) {
	v := textVec("checkout", "checkout", "login", "checkout")
	best, ok := TextCandidates().Pick(v)
	if !ok {
		t.Fatal("Pick returned !ok")
	}
	if best.Encoding() != types.EncodingDictionary {
		t.Fatalf("encoding = %s, want dictionary", best.Encoding())
	}
}

func TestFixedCandidatesPickFORBitPack(t *testing.T) {
	v := types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: 4, I64: []int64{100, 101, 102, 103}}
	best, ok := FixedCandidates().Pick(v)
	if !ok {
		t.Fatal("Pick returned !ok")
	}
	if best.Encoding() != types.EncodingFORBitPack {
		t.Fatalf("encoding = %s, want for+bitpack", best.Encoding())
	}
}
