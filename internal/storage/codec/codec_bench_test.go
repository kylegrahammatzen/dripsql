// Codec encode/decode microbenches across the codecs the cascade chooses on real workloads.
// Fixtures use 2048-row pages to match the StandardBatchRows page storage seals today.
// Run via go test ./internal/storage/codec -bench=Codec -benchmem -run=^$
package codec

import (
	"math/rand"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type codecBenchCase struct {
	name string
	enc  schema.Encoding
	kind vector.VecKind
	fill func(v vector.Vec)
}

func codecBenchCases() []codecBenchCase {
	return []codecBenchCase{
		{"Plain/Int64", schema.EncPlain, vector.VecInt64, fillInt64Random},
		{"FOR/Int64", schema.EncFOR, vector.VecInt64, fillInt64FOR},
		{"Delta/Int64", schema.EncDelta, vector.VecInt64, fillInt64Delta},
		{"Sequence/Int64", schema.EncSequence, vector.VecInt64, fillInt64Sequence},
		{"Constant/Int64", schema.EncConstant, vector.VecInt64, fillInt64Constant},
		{"Pcodec/Int64", schema.EncPcodec, vector.VecInt64, fillInt64Multimodal},
		{"Plain/Text", schema.EncPlain, vector.VecText, fillTextRandom},
		{"Dict/Text", schema.EncDict, vector.VecText, fillTextLowCard},
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
			var dst vector.Vec
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

func newFilledVec(kind vector.VecKind, rows int, fill func(vector.Vec)) vector.Vec {
	var v vector.Vec
	if kind == vector.VecText || kind == vector.VecBytes || kind == vector.VecJSON {
		v = vector.NewVarVec(kind, rows, 0)
	} else {
		v = vector.NewVec(kind, rows)
	}
	fill(v)
	return v
}

func fillInt64Random(v vector.Vec) {
	r := rand.New(rand.NewSource(1))
	for i := range v.I64() {
		v.I64()[i] = r.Int63()
	}
}

func fillInt64FOR(v vector.Vec) {
	for i := range v.I64() {
		v.I64()[i] = 1_700_000_000 + int64(i)
	}
}

func fillInt64Delta(v vector.Vec) {
	r := rand.New(rand.NewSource(4))
	s := v.I64()
	s[0] = 1_000_000
	for i := 1; i < len(s); i++ {
		s[i] = s[i-1] + int64(r.Intn(8))
	}
}

func fillInt64Sequence(v vector.Vec) {
	for i := range v.I64() {
		v.I64()[i] = int64(i)
	}
}

func fillInt64Multimodal(v vector.Vec) {
	s := v.I64()
	for i := range s {
		switch (i / 1024) % 2 {
		case 0:
			s[i] = int64(1_000 + i%64)
		case 1:
			s[i] = int64(10_000_000_000 + int64(i%256))
		}
	}
}

func fillInt64Constant(v vector.Vec) {
	s := v.I64()
	for i := range s {
		s[i] = 42
	}
}

func fillTextRandom(v vector.Vec) {
	r := rand.New(rand.NewSource(2))
	letters := []byte("abcdefghijklmnopqrstuvwxyz")
	buf := make([]byte, 32)
	for i := range int(v.Len) {
		n := 5 + r.Intn(12)
		for j := range n {
			buf[j] = letters[r.Intn(len(letters))]
		}
		v.Var().AppendBytes(i, buf[:n])
	}
}

func fillTextLowCard(v vector.Vec) {
	r := rand.New(rand.NewSource(3))
	labels := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma"), []byte("delta"), []byte("epsilon")}
	for i := range int(v.Len) {
		v.Var().AppendBytes(i, labels[r.Intn(len(labels))])
	}
}
