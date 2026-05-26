// ALP round-trip tests across float32 and float64.
// Skip semantics: NaN, Inf, -0, non-decimal floats fall back to ErrSkip.
package codec

import (
	"math"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestALP_EncodingIsALP(t *testing.T) {
	if (alpCodec{}).Encoding() != schema.EncALP {
		t.Fatal("alpCodec must claim ALP encoding")
	}
}

func TestALP_RoundTrip_Float64_Decimals(t *testing.T) {
	src := vector.NewVec(vector.VecFloat64, 256)
	for i := range src.F64() {
		src.F64()[i] = 100.0 + float64(i)*0.01
	}
	payload, err := alpCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var dst vector.Vec
	if err := (alpCodec{}).Decode(payload, vector.VecFloat64, 256, 0, &dst); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	for i := range src.F64() {
		if dst.F64()[i] != src.F64()[i] {
			t.Fatalf("row %d: got %v want %v", i, dst.F64()[i], src.F64()[i])
		}
	}
}

func TestALP_RoundTrip_Float64_Integers(t *testing.T) {
	src := vector.NewVec(vector.VecFloat64, 100)
	for i := range src.F64() {
		src.F64()[i] = float64(i * 7)
	}
	payload, err := alpCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var dst vector.Vec
	if err := (alpCodec{}).Decode(payload, vector.VecFloat64, 100, 0, &dst); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	for i := range src.F64() {
		if dst.F64()[i] != src.F64()[i] {
			t.Fatalf("row %d: got %v want %v", i, dst.F64()[i], src.F64()[i])
		}
	}
}

func TestALP_RoundTrip_Float32(t *testing.T) {
	src := vector.NewVec(vector.VecFloat32, 128)
	for i := range src.F32() {
		src.F32()[i] = float32(i) * 0.5
	}
	payload, err := alpCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var dst vector.Vec
	if err := (alpCodec{}).Decode(payload, vector.VecFloat32, 128, 0, &dst); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	for i := range src.F32() {
		if dst.F32()[i] != src.F32()[i] {
			t.Fatalf("row %d: got %v want %v", i, dst.F32()[i], src.F32()[i])
		}
	}
}

func TestALP_SkipsNaN(t *testing.T) {
	src := vector.NewVec(vector.VecFloat64, 4)
	src.F64()[0] = 1.0
	src.F64()[1] = math.NaN()
	src.F64()[2] = 2.0
	src.F64()[3] = 3.0
	if _, err := (alpCodec{}).Encode(src, nil); err != ErrSkip {
		t.Fatalf("expected ErrSkip, got %v", err)
	}
}

func TestALP_SkipsInf(t *testing.T) {
	src := vector.NewVec(vector.VecFloat64, 3)
	src.F64()[0] = 1.0
	src.F64()[1] = math.Inf(1)
	src.F64()[2] = 2.0
	if _, err := (alpCodec{}).Encode(src, nil); err != ErrSkip {
		t.Fatalf("expected ErrSkip, got %v", err)
	}
}

func TestALP_SkipsNegativeZero(t *testing.T) {
	src := vector.NewVec(vector.VecFloat64, 3)
	src.F64()[0] = 1.0
	src.F64()[1] = math.Copysign(0, -1)
	src.F64()[2] = 2.0
	if _, err := (alpCodec{}).Encode(src, nil); err != ErrSkip {
		t.Fatalf("expected ErrSkip, got %v", err)
	}
}

func TestALP_HandlesIrrationals(t *testing.T) {
	// Pi/E/Sqrt2 either round-trip at some e (a valid win) or ErrSkip. Either is fine.
	src := vector.NewVec(vector.VecFloat64, 3)
	src.F64()[0] = math.Pi
	src.F64()[1] = math.E
	src.F64()[2] = math.Sqrt2
	payload, err := (alpCodec{}).Encode(src, nil)
	if err == ErrSkip {
		return
	}
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var dst vector.Vec
	if err := (alpCodec{}).Decode(payload, vector.VecFloat64, 3, 0, &dst); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	for i := range src.F64() {
		if dst.F64()[i] != src.F64()[i] {
			t.Fatalf("row %d: got %v want %v", i, dst.F64()[i], src.F64()[i])
		}
	}
}

func TestALP_RejectsConstant(t *testing.T) {
	src := vector.NewVec(vector.VecFloat64, 64)
	for i := range src.F64() {
		src.F64()[i] = 3.14
	}
	if _, err := (alpCodec{}).Encode(src, nil); err != ErrSkip {
		t.Fatalf("expected ErrSkip (span=0 means width<storageBits fails), got %v", err)
	}
}

func TestALP_CascadePicksALPOverPlain(t *testing.T) {
	src := vector.NewVec(vector.VecFloat64, 256)
	for i := range src.F64() {
		src.F64()[i] = 100.0 + float64(i)*0.01
	}
	enc, payload, err := Encode(src, nil)
	if err != nil {
		t.Fatalf("cascade Encode: %v", err)
	}
	if enc != schema.EncALP {
		t.Fatalf("cascade picked %v, expected ALP", enc)
	}
	if len(payload) >= 256*8 {
		t.Fatalf("ALP payload %d not smaller than plain %d", len(payload), 256*8)
	}
}
