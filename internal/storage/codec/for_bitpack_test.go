// FOR+bitpack round-trip + estimate tests across FOR-packable kinds and width edges.
package codec

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestFORBitpack_EncodingIsFOR(t *testing.T) {
	if (forBitpackCodec{}).Encoding() != schema.EncFOR {
		t.Fatal("forBitpackCodec must claim FORBitPack encoding")
	}
}

func TestFORBitpack_RoundTrip(t *testing.T) {
	cases := []struct {
		name string
		kind vector.VecKind
		rows int
		fill func(v vector.Vec)
		read func(v vector.Vec, i int) int64
	}{
		{"Int64", vector.VecInt64, 2048,
			func(v vector.Vec) {
				for i := range v.I64() {
					v.I64()[i] = 1_000_000 + int64(i)*7
				}
			},
			func(v vector.Vec, i int) int64 { return v.I64()[i] }},
		{"Int32", vector.VecInt32, 1500,
			func(v vector.Vec) {
				for i := range v.I32() {
					v.I32()[i] = -500 + int32(i)
				}
			},
			func(v vector.Vec, i int) int64 { return int64(v.I32()[i]) }},
		{"Int16", vector.VecInt16, 200,
			func(v vector.Vec) {
				for i := range v.I16() {
					v.I16()[i] = int16(i)
				}
			},
			func(v vector.Vec, i int) int64 { return int64(v.I16()[i]) }},
		{"Timestamp", vector.VecTimestamp, 128,
			func(v vector.Vec) {
				for i := range v.I64() {
					v.I64()[i] = 1700000000_000_000_000 + int64(i)*1_000_000_000
				}
			},
			func(v vector.Vec, i int) int64 { return v.I64()[i] }},
		{"Enum32", vector.VecEnum32, 64,
			func(v vector.Vec) {
				for i := range v.U32() {
					v.U32()[i] = uint32(100 + i)
				}
			},
			func(v vector.Vec, i int) int64 { return int64(v.U32()[i]) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := vector.NewVec(tc.kind, tc.rows)
			tc.fill(src)
			payload, err := forBitpackCodec{}.Encode(src, nil)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			var dst vector.Vec
			if err := (forBitpackCodec{}).Decode(payload, tc.kind, tc.rows, 0, &dst); err != nil {
				t.Fatalf("Decode: %v", err)
			}
			for i := range tc.rows {
				if got, want := tc.read(dst, i), tc.read(src, i); got != want {
					t.Fatalf("row %d: got %d want %d", i, got, want)
				}
			}
		})
	}
}

func TestFORBitpack_EstimateRejectsConstant(t *testing.T) {
	src := vector.NewVec(vector.VecInt64, 100)
	for i := range src.I64() {
		src.I64()[i] = 42
	}
	if _, ok := Estimate(forBitpackCodec{}, src); ok {
		t.Fatal("Estimate must reject constant column (width=0)")
	}
}

func TestFORBitpack_EstimateRejectsWidthExceedsStorage(t *testing.T) {
	src := vector.NewVec(vector.VecInt16, 100)
	for i := range src.I16() {
		if i%2 == 0 {
			src.I16()[i] = -30000
		} else {
			src.I16()[i] = 30000
		}
	}
	if _, ok := Estimate(forBitpackCodec{}, src); ok {
		t.Fatal("Estimate must reject when residual width >= storage width")
	}
}

func TestFORBitpack_EstimateRejectsNonFORKinds(t *testing.T) {
	for _, kind := range []vector.VecKind{vector.VecBool, vector.VecFloat64, vector.VecText, vector.VecUUID} {
		var v vector.Vec
		if kind == vector.VecText {
			v = vector.NewVarVec(kind, 4, 0)
		} else {
			v = vector.NewVec(kind, 4)
		}
		if _, ok := Estimate(forBitpackCodec{}, v); ok {
			t.Fatalf("Estimate must reject %v", kind)
		}
	}
}

func TestFORBitpack_DecodeRejects(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		kind    vector.VecKind
		rows    int
	}{
		{"truncatedHeader", make([]byte, 5), vector.VecInt64, 4},
		{"zeroRowsNonEmpty", []byte{1}, vector.VecInt64, 0},
		{"negativeRows", nil, vector.VecInt64, -1},
		{"nonFORKind", make([]byte, forHeaderSize+128), vector.VecText, 1},
	}
	for _, tc := range cases {
		var dst vector.Vec
		if err := (forBitpackCodec{}).Decode(tc.payload, tc.kind, tc.rows, 0, &dst); err == nil {
			t.Fatalf("%s: Decode must reject", tc.name)
		}
	}
}

func TestFORBitpack_EstimateMatchesEncode(t *testing.T) {
	src := vector.NewVec(vector.VecInt64, 100)
	for i := range src.I64() {
		src.I64()[i] = int64(i) * 13
	}
	est, ok := Estimate(forBitpackCodec{}, src)
	if !ok {
		t.Fatal("Estimate must accept arithmetic series")
	}
	payload, err := forBitpackCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(payload) != est {
		t.Fatalf("len(Encode)=%d != Estimate=%d", len(payload), est)
	}
}

func TestCodec_BitpackShared_FORDeltaIdentical(t *testing.T) {
	makeVec := func(vals ...int64) vector.Vec {
		v := vector.NewVec(vector.VecInt64, len(vals))
		copy(v.I64(), vals)
		return v
	}
	encodeBitpack := func(width int, residuals ...uint64) []byte {
		buf := make([]byte, PackedSize(len(residuals), width))
		Pack(width, residuals, buf)
		return buf
	}

	forPayload, err := forBitpackCodec{}.Encode(makeVec(10, 20, 35, 50), nil)
	if err != nil {
		t.Fatalf("FOR Encode: %v", err)
	}
	if want := encodeBitpack(6, 0, 10, 25, 40); !bytes.Equal(forPayload[forHeaderSize:], want) {
		t.Fatalf("FOR bitpack section diverged from shared Pack")
	}

	deltaPayload, err := deltaBitpackCodec{}.Encode(makeVec(10, 30, 55, 85), nil)
	if err != nil {
		t.Fatalf("Delta Encode: %v", err)
	}
	if want := encodeBitpack(4, 0, 5, 10); !bytes.Equal(deltaPayload[deltaHeaderSize:], want) {
		t.Fatalf("Delta bitpack section diverged from shared Pack")
	}
}

func TestFORBitpack_DecodeRangeChecksInt16(t *testing.T) {
	src := vector.NewVec(vector.VecInt16, 2)
	src.I16()[0], src.I16()[1] = 0, 100
	payload, err := forBitpackCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	binary.LittleEndian.PutUint64(payload[0:8], uint64(int64(math.MaxInt16+1)))
	var dst vector.Vec
	if err := (forBitpackCodec{}).Decode(payload, vector.VecInt16, 2, 0, &dst); err == nil {
		t.Fatal("FOR decode must error when residuals would overflow int16")
	}
}

func TestFORBitpack_DecodeRangeChecksEnum32(t *testing.T) {
	src := vector.NewVec(vector.VecEnum32, 2)
	src.U32()[0], src.U32()[1] = 1, 2
	payload, err := forBitpackCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	negBase := int64(-1000)
	binary.LittleEndian.PutUint64(payload[0:8], uint64(negBase))
	var dst vector.Vec
	if err := (forBitpackCodec{}).Decode(payload, vector.VecEnum32, 2, 0, &dst); err == nil {
		t.Fatal("FOR decode must error when residuals would underflow uint32")
	}
}
