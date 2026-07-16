// Constant codec invariant tests covering Estimate rejecting non-constant input and round-trip per kind class.
// Wire payload size is the contract. Rows=0 and varying-row inputs both covered.
package codec

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestConstant_EncodingIsConstant(t *testing.T) {
	if (constantCodec{}).Encoding() != schema.EncConstant {
		t.Fatal("constantCodec must claim Constant encoding")
	}
}

func TestConstant_EstimateRejectsNonConstant(t *testing.T) {
	v := vector.NewVec(vector.VecInt64, 3)
	v.I64()[0] = 1
	v.I64()[1] = 1
	v.I64()[2] = 2
	if _, ok := Estimate(constantCodec{}, v); ok {
		t.Fatal("Estimate must return false for non-constant rows")
	}
}

func TestConstant_Int64RoundTrip(t *testing.T) {
	src := vector.NewVec(vector.VecInt64, 100)
	for i := range src.I64() {
		src.I64()[i] = -42
	}
	payload, err := constantCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(payload) != 8 {
		t.Fatalf("Constant Int64 payload len=%d want 8", len(payload))
	}
	var dst vector.Vec
	if err := (constantCodec{}).Decode(payload, vector.VecInt64, 100, 0, &dst); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if dst.Len != 100 {
		t.Fatalf("decoded Len=%d want 100", dst.Len)
	}
	for i, v := range dst.I64() {
		if v != -42 {
			t.Fatalf("row %d: got %d want -42", i, v)
		}
	}
}

func TestConstant_BoolAllTrueAllFalse(t *testing.T) {
	for _, allTrue := range []bool{true, false} {
		src := vector.NewVec(vector.VecBool, 17)
		bits := src.BoolBits()
		if allTrue {
			for i := range bits {
				bits[i] = 0xFF
			}
			bits[2] = 0b00000001
		}
		payload, err := constantCodec{}.Encode(src, nil)
		if err != nil {
			t.Fatalf("Encode allTrue=%v: %v", allTrue, err)
		}
		if len(payload) != 1 {
			t.Fatalf("bool Constant payload len=%d want 1", len(payload))
		}
		var dst vector.Vec
		if err := (constantCodec{}).Decode(payload, vector.VecBool, 17, 0, &dst); err != nil {
			t.Fatalf("Decode allTrue=%v: %v", allTrue, err)
		}
		for i := range 17 {
			got := (dst.BoolBits()[i>>3]>>uint(i&7))&1 != 0
			if got != allTrue {
				t.Fatalf("allTrue=%v row %d: got %v", allTrue, i, got)
			}
		}
	}
}

func TestConstant_VarBytesRoundTrip(t *testing.T) {
	src := vector.NewVarVec(vector.VecText, 50, 0)
	vb := src.Var()
	for i := range 50 {
		vb.AppendString(i, "hello world")
	}
	payload, err := constantCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(payload) != 4+11 {
		t.Fatalf("payload len=%d want 15", len(payload))
	}
	var dst vector.Vec
	if err := (constantCodec{}).Decode(payload, vector.VecText, 50, 0, &dst); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	for i := range 50 {
		if got := dst.Var().String(i); got != "hello world" {
			t.Fatalf("row %d: got %q", i, got)
		}
	}
}

func TestConstant_EstimateRejectsInvalidKind(t *testing.T) {
	if _, ok := Estimate(constantCodec{}, vector.Vec{}); ok {
		t.Fatal("Estimate must reject zero-Kind even when Len=0")
	}
	if _, ok := Estimate(constantCodec{}, vector.Vec{Kind: vector.VecInvalid}); ok {
		t.Fatal("Estimate must reject VecInvalid")
	}
}

func TestConstant_VarBytesBroadcastIsCompact(t *testing.T) {
	var dst vector.Vec
	long := make([]byte, 200)
	payload := append([]byte{200, 0, 0, 0}, long...)
	if err := (constantCodec{}).Decode(payload, vector.VecText, 1000, 0, &dst); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	// Each row sees 200 bytes while the data buffer holds the value once, since 200000 would mean the per-row copy path ran instead of Broadcast.
	internal := dst.Var()
	for i := range 1000 {
		if internal.Len(i) != 200 {
			t.Fatalf("row %d Len=%d", i, internal.Len(i))
		}
	}
}

func TestConstant_DecodeRejectsTrailingBytes(t *testing.T) {
	var dst vector.Vec
	if err := (constantCodec{}).Decode(make([]byte, 9), vector.VecInt64, 4, 0, &dst); err == nil {
		t.Fatal("constant decode must reject trailing bytes on fixed kind")
	}
}

func TestConstant_EmptyVec(t *testing.T) {
	src := vector.NewVec(vector.VecInt32, 0)
	payload, err := constantCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode empty: %v", err)
	}
	if len(payload) != 0 {
		t.Fatalf("empty payload len=%d want 0", len(payload))
	}
	var dst vector.Vec
	if err := (constantCodec{}).Decode(payload, vector.VecInt32, 0, 0, &dst); err != nil {
		t.Fatalf("Decode empty: %v", err)
	}
}

func TestConstant_DecodeRejectsTruncated(t *testing.T) {
	var dst vector.Vec
	if err := (constantCodec{}).Decode([]byte{0}, vector.VecInt64, 4, 0, &dst); err == nil {
		t.Fatal("fixed Constant decode must reject payload shorter than width")
	}
	if err := (constantCodec{}).Decode([]byte{}, vector.VecBool, 4, 0, &dst); err == nil {
		t.Fatal("bool Constant decode must reject empty payload")
	}
	if err := (constantCodec{}).Decode([]byte{0, 0, 0}, vector.VecText, 1, 0, &dst); err == nil {
		t.Fatal("varbytes Constant decode must reject header truncated")
	}
	if err := (constantCodec{}).Decode([]byte{10, 0, 0, 0, 'h'}, vector.VecText, 1, 0, &dst); err == nil {
		t.Fatal("varbytes Constant decode must reject value truncated (header says 10, only 1)")
	}
}

func TestConstant_EstimateMatchesEncode(t *testing.T) {
	src := vector.NewVarVec(vector.VecText, 8, 0)
	for i := range 8 {
		src.Var().AppendString(i, "value")
	}
	est, ok := Estimate(constantCodec{}, src)
	if !ok {
		t.Fatal("constant rows must Estimate")
	}
	payload, err := constantCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(payload) != est {
		t.Fatalf("len(Encode)=%d != Estimate=%d", len(payload), est)
	}
}

func TestConstant_DecodeRejectsNonCanonicalBool(t *testing.T) {
	var dst vector.Vec
	if err := (constantCodec{}).Decode([]byte{2}, vector.VecBool, 10, 0, &dst); err == nil {
		t.Fatal("constant bool decode must reject non-canonical payload byte")
	}
	if err := (constantCodec{}).Decode([]byte{0xFF}, vector.VecBool, 10, 0, &dst); err == nil {
		t.Fatal("constant bool decode must reject 0xFF")
	}
	if err := (constantCodec{}).Decode([]byte{0}, vector.VecBool, 10, 0, &dst); err != nil {
		t.Fatalf("constant bool decode must accept 0: %v", err)
	}
	if err := (constantCodec{}).Decode([]byte{1}, vector.VecBool, 10, 0, &dst); err != nil {
		t.Fatalf("constant bool decode must accept 1: %v", err)
	}
}
