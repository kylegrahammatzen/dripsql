// Codec encode/decode microbenches across the codecs the cascade chooses on real workloads.
// Fixtures use 2048-row pages to match the StandardBatchRows page storage seals today.
// Run with: go test ./internal/storage/codec -bench=Codec -benchmem -run=^$
package codec

import (
	"math/rand"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type codecBenchCase struct {
	name string
	enc  types.Encoding
	kind types.VecKind
	fill func(v types.Vec)
}

func codecBenchCases() []codecBenchCase {
	return []codecBenchCase{
		{"plain_int64", types.EncodingFlat, types.VecInt64, fillInt64Random},
		{"for_int64", types.EncodingFORBitPack, types.VecInt64, fillInt64FOR},
		{"delta_int64", types.EncodingDeltaBitPack, types.VecInt64, fillInt64Delta},
		{"sequence_int64", types.EncodingSequence, types.VecInt64, fillInt64Sequence},
		{"constant_int64", types.EncodingConstant, types.VecInt64, fillInt64Constant},
		{"plain_text", types.EncodingFlat, types.VecText, fillTextRandom},
		{"dict_text_lowcard", types.EncodingDictionary, types.VecText, fillTextLowCard},
	}
}

func BenchmarkCodec_Encode(b *testing.B) {
	const rows = 2048
	for _, c := range codecBenchCases() {
		b.Run(c.name, func(b *testing.B) {
			codec, err := Lookup(c.enc)
			if err != nil {
				b.Fatal(err)
			}
			v := newFilledVec(c.kind, rows, c.fill)
			ctx := &EncodeContext{Scratch: NewScratchPool()}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := codec.Encode(v, ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCodec_Decode(b *testing.B) {
	const rows = 2048
	for _, c := range codecBenchCases() {
		b.Run(c.name, func(b *testing.B) {
			codec, err := Lookup(c.enc)
			if err != nil {
				b.Fatal(err)
			}
			src := newFilledVec(c.kind, rows, c.fill)
			payload, err := codec.Encode(src, nil)
			if err != nil {
				b.Fatal(err)
			}
			var dst types.Vec
			b.ReportAllocs()
			b.SetBytes(int64(len(payload)))
			b.ResetTimer()
			for b.Loop() {
				dst.ResetForDecode(c.kind)
				if err := codec.Decode(payload, c.kind, rows, 0, &dst); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func newFilledVec(kind types.VecKind, rows int, fill func(types.Vec)) types.Vec {
	var v types.Vec
	if kind == types.VecText || kind == types.VecBytes || kind == types.VecJSON {
		v = types.NewVarVec(kind, rows, 0)
	} else {
		v = types.NewVec(kind, rows)
	}
	fill(v)
	return v
}

func fillInt64Random(v types.Vec) {
	r := rand.New(rand.NewSource(1))
	for i := range v.I64() {
		v.I64()[i] = r.Int63()
	}
}

func fillInt64FOR(v types.Vec) {
	for i := range v.I64() {
		v.I64()[i] = 1_700_000_000 + int64(i)
	}
}

func fillInt64Delta(v types.Vec) {
	r := rand.New(rand.NewSource(4))
	s := v.I64()
	s[0] = 1_000_000
	for i := 1; i < len(s); i++ {
		s[i] = s[i-1] + int64(r.Intn(8))
	}
}

func fillInt64Sequence(v types.Vec) {
	for i := range v.I64() {
		v.I64()[i] = int64(i)
	}
}

func fillInt64Constant(v types.Vec) {
	s := v.I64()
	for i := range s {
		s[i] = 42
	}
}

func fillTextRandom(v types.Vec) {
	r := rand.New(rand.NewSource(2))
	letters := []byte("abcdefghijklmnopqrstuvwxyz")
	buf := make([]byte, 32)
	for i := 0; i < int(v.Len); i++ {
		n := 5 + r.Intn(12)
		for j := 0; j < n; j++ {
			buf[j] = letters[r.Intn(len(letters))]
		}
		v.Var().AppendBytes(i, buf[:n])
	}
}

func fillTextLowCard(v types.Vec) {
	r := rand.New(rand.NewSource(3))
	labels := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma"), []byte("delta"), []byte("epsilon")}
	for i := 0; i < int(v.Len); i++ {
		v.Var().AppendBytes(i, labels[r.Intn(len(labels))])
	}
}
