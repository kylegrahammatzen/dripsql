// Plain codec invariant tests: round-trip per kind class, truncated-payload rejection.
// These pin the wire layout storage will rely on for v4 Flat-encoded pages.
package codec

import (
	"math"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestPlain_EncodingIsFlat(t *testing.T) {
	if (plainCodec{}).Encoding() != schema.EncPlain {
		t.Fatal("plainCodec must claim Flat encoding")
	}
}

func TestPlain_Int64RoundTrip(t *testing.T) {
	src := vector.NewVec(vector.VecInt64, 5)
	for i, val := range []int64{10, -20, 0, 1 << 40, -1 << 40} {
		src.I64()[i] = val
	}
	payload, err := plainCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(payload) != 40 {
		t.Fatalf("payload len=%d want 40", len(payload))
	}
	var dst vector.Vec
	if err := (plainCodec{}).Decode(payload, vector.VecInt64, 5, 0, &dst); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	for i, want := range []int64{10, -20, 0, 1 << 40, -1 << 40} {
		if got := dst.I64()[i]; got != want {
			t.Fatalf("row %d: got %d want %d", i, got, want)
		}
	}
}

func TestPlain_BoolRoundTrip(t *testing.T) {
	src := vector.NewVec(vector.VecBool, 17)
	bits := src.BoolBits()
	bits[0] = 0b10101010
	bits[1] = 0b01010101
	bits[2] = 0b00000001
	payload, err := plainCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(payload) != 3 {
		t.Fatalf("payload len=%d want 3", len(payload))
	}
	var dst vector.Vec
	if err := (plainCodec{}).Decode(payload, vector.VecBool, 17, 0, &dst); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	got := dst.BoolBits()
	if got[0] != 0b10101010 || got[1] != 0b01010101 || got[2] != 0b00000001 {
		t.Fatalf("bits round-trip mismatch: %08b %08b %08b", got[0], got[1], got[2])
	}
}

func TestPlain_VarBytesRoundTrip(t *testing.T) {
	src := vector.NewVarVec(vector.VecText, 4, 0)
	vb := src.Var()
	vb.AppendString(0, "")
	vb.AppendString(1, "short")
	vb.AppendString(2, "twelvebytes_")
	vb.AppendString(3, "longer than thirteen bytes total")
	payload, err := plainCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var dst vector.Vec
	if err := (plainCodec{}).Decode(payload, vector.VecText, 4, 0, &dst); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	got := dst.Var()
	want := []string{"", "short", "twelvebytes_", "longer than thirteen bytes total"}
	for i, w := range want {
		if got.String(i) != w {
			t.Fatalf("row %d: got %q want %q", i, got.String(i), w)
		}
	}
}

func TestPlain_DecodeRejectsTrailingBytes(t *testing.T) {
	var dst vector.Vec
	src := make([]byte, 16+1)
	if err := (plainCodec{}).Decode(src, vector.VecInt32, 4, 0, &dst); err == nil {
		t.Fatal("Decode must reject trailing bytes")
	}
}

func TestPlain_DecodeSetsEncAndClearsValid(t *testing.T) {
	dst := vector.Vec{Kind: vector.VecInt64, Valid: vector.NewValidity(4)}
	src := make([]byte, 32)
	if err := (plainCodec{}).Decode(src, vector.VecInt64, 4, 0, &dst); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if dst.Enc != schema.EncPlain {
		t.Fatalf("dst.Enc=%v want Flat", dst.Enc)
	}
	if dst.Valid != nil {
		t.Fatal("dst.Valid must be cleared by Decode")
	}
}

func TestPlain_DecodeRejectsNegativeRows(t *testing.T) {
	var dst vector.Vec
	if err := (plainCodec{}).Decode(nil, vector.VecInt64, -1, 0, &dst); err == nil {
		t.Fatal("Decode must reject negative rows")
	}
	if err := (plainCodec{}).Decode(make([]byte, 32), vector.VecInt64, 4, 5, &dst); err == nil {
		t.Fatal("Decode must reject nullCount > rows")
	}
}

func TestPlain_BoolEncodeMasksTail(t *testing.T) {
	src := vector.NewVec(vector.VecBool, 3)
	bits := src.BoolBits()
	bits[0] = 0xFF
	payload, err := plainCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if payload[0] != 0b00000111 {
		t.Fatalf("bool encode must mask tail; got %08b want 00000111", payload[0])
	}
}

func TestPlain_DecodeRejectsTruncated(t *testing.T) {
	var dst vector.Vec
	if err := (plainCodec{}).Decode([]byte{0, 0, 0, 0}, vector.VecInt64, 4, 0, &dst); err == nil {
		t.Fatal("fixed-width decode must reject short payload")
	}
	if err := (plainCodec{}).Decode([]byte{0, 0}, vector.VecBool, 17, 0, &dst); err == nil {
		t.Fatal("bool decode must reject short payload")
	}
	if err := (plainCodec{}).Decode([]byte{5, 0, 0, 0}, vector.VecText, 1, 0, &dst); err == nil {
		t.Fatal("varbytes decode must reject truncated value (header says 5 bytes, none follow)")
	}
	if err := (plainCodec{}).Decode([]byte{5, 0, 0}, vector.VecText, 1, 0, &dst); err == nil {
		t.Fatal("varbytes decode must reject truncated header (3 bytes < 4)")
	}
}

func TestPlain_EstimateMatchesEncode(t *testing.T) {
	src := vector.NewVarVec(vector.VecText, 3, 0)
	src.Var().AppendString(0, "abc")
	src.Var().AppendString(1, "longer than twelve")
	src.Var().AppendString(2, "")
	est, ok := Estimate(plainCodec{}, src)
	if !ok {
		t.Fatal("Estimate must succeed for varbytes")
	}
	payload, err := plainCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(payload) != est {
		t.Fatalf("Encode len=%d != Estimate=%d", len(payload), est)
	}
}

func TestPlain_EncodeReusesScratch(t *testing.T) {
	src := vector.NewVec(vector.VecInt32, 4)
	for i := range src.I32() {
		src.I32()[i] = int32(i)
	}
	sp := NewScratchPool()
	sp.trial = make([]byte, 0, 64)
	trialBase := &sp.trial[:1][0]
	ctx := &EncodeContext{Scratch: sp}
	payload, err := plainCodec{}.Encode(src, ctx)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if &payload[0] != trialBase {
		t.Fatal("Encode must reuse pool scratch when capacity is sufficient")
	}
}

func TestValidateDecodeArgs_RowsCapInt32(t *testing.T) {
	if err := validateDecodeArgs(math.MaxInt32+1, 0); err == nil {
		t.Fatal("validateDecodeArgs must reject rows > int32 max")
	}
	if err := validateDecodeArgs(math.MaxInt32, 0); err != nil {
		t.Fatalf("validateDecodeArgs must accept exactly int32 max: %v", err)
	}
}
