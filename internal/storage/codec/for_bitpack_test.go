// FOR+bitpack round-trip + estimate tests across FOR-packable kinds and width edges.
package codec

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestFORBitpack_EncodingIsFOR(t *testing.T) {
	if (forBitpackCodec{}).Encoding() != types.EncodingFORBitPack {
		t.Fatal("forBitpackCodec must claim FORBitPack encoding")
	}
}

func TestFORBitpack_RoundTrip(t *testing.T) {
	cases := []struct {
		name string
		kind types.VecKind
		rows int
		fill func(v types.Vec)
		read func(v types.Vec, i int) int64
	}{
		{"Int64", types.VecInt64, 2048,
			func(v types.Vec) {
				for i := range v.I64() {
					v.I64()[i] = 1_000_000 + int64(i)*7
				}
			},
			func(v types.Vec, i int) int64 { return v.I64()[i] }},
		{"Int32", types.VecInt32, 1500,
			func(v types.Vec) {
				for i := range v.I32() {
					v.I32()[i] = -500 + int32(i)
				}
			},
			func(v types.Vec, i int) int64 { return int64(v.I32()[i]) }},
		{"Int16", types.VecInt16, 200,
			func(v types.Vec) {
				for i := range v.I16() {
					v.I16()[i] = int16(i)
				}
			},
			func(v types.Vec, i int) int64 { return int64(v.I16()[i]) }},
		{"Timestamp", types.VecTimestamp, 128,
			func(v types.Vec) {
				for i := range v.I64() {
					v.I64()[i] = 1700000000_000_000_000 + int64(i)*1_000_000_000
				}
			},
			func(v types.Vec, i int) int64 { return v.I64()[i] }},
		{"Enum32", types.VecEnum32, 64,
			func(v types.Vec) {
				for i := range v.U32() {
					v.U32()[i] = uint32(100 + i)
				}
			},
			func(v types.Vec, i int) int64 { return int64(v.U32()[i]) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := types.NewVec(tc.kind, tc.rows)
			tc.fill(src)
			payload, err := forBitpackCodec{}.Encode(src, nil)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			var dst types.Vec
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
	src := types.NewVec(types.VecInt64, 100)
	for i := range src.I64() {
		src.I64()[i] = 42
	}
	if _, ok := Estimate(forBitpackCodec{}, src); ok {
		t.Fatal("Estimate must reject constant column (width=0)")
	}
}

func TestFORBitpack_EstimateRejectsWidthExceedsStorage(t *testing.T) {
	src := types.NewVec(types.VecInt16, 100)
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
	for _, kind := range []types.VecKind{types.VecBool, types.VecFloat64, types.VecText, types.VecUUID} {
		var v types.Vec
		if kind == types.VecText {
			v = types.NewVarVec(kind, 4, 0)
		} else {
			v = types.NewVec(kind, 4)
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
		kind    types.VecKind
		rows    int
	}{
		{"truncatedHeader", make([]byte, 5), types.VecInt64, 4},
		{"zeroRowsNonEmpty", []byte{1}, types.VecInt64, 0},
		{"negativeRows", nil, types.VecInt64, -1},
		{"nonFORKind", make([]byte, forHeaderSize+128), types.VecText, 1},
	}
	for _, tc := range cases {
		var dst types.Vec
		if err := (forBitpackCodec{}).Decode(tc.payload, tc.kind, tc.rows, 0, &dst); err == nil {
			t.Fatalf("%s: Decode must reject", tc.name)
		}
	}
}

func TestFORBitpack_EstimateMatchesEncode(t *testing.T) {
	src := types.NewVec(types.VecInt64, 100)
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
	makeVec := func(vals ...int64) types.Vec {
		v := types.NewVec(types.VecInt64, len(vals))
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
	src := types.NewVec(types.VecInt16, 2)
	src.I16()[0], src.I16()[1] = 0, 100
	payload, err := forBitpackCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	binary.LittleEndian.PutUint64(payload[0:8], uint64(int64(math.MaxInt16+1)))
	var dst types.Vec
	if err := (forBitpackCodec{}).Decode(payload, types.VecInt16, 2, 0, &dst); err == nil {
		t.Fatal("FOR decode must error when residuals would overflow int16")
	}
}

func TestFORBitpack_DecodeRangeChecksEnum32(t *testing.T) {
	src := types.NewVec(types.VecEnum32, 2)
	src.U32()[0], src.U32()[1] = 1, 2
	payload, err := forBitpackCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	negBase := int64(-1000)
	binary.LittleEndian.PutUint64(payload[0:8], uint64(negBase))
	var dst types.Vec
	if err := (forBitpackCodec{}).Decode(payload, types.VecEnum32, 2, 0, &dst); err == nil {
		t.Fatal("FOR decode must error when residuals would underflow uint32")
	}
}
