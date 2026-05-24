// ALP-RD round-trip + cascade selection for irrational/scientific floats.
package codec

import (
	"math"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestALPRD_EncodingIsALPRD(t *testing.T) {
	if (alpRDCodec{}).Encoding() != types.EncodingALPRD {
		t.Fatal("alpRDCodec must claim ALP-RD encoding")
	}
}

func TestALPRD_RoundTrip_Irrationals(t *testing.T) {
	src := types.NewVec(types.VecFloat64, 4)
	src.F64()[0] = math.Pi
	src.F64()[1] = math.E
	src.F64()[2] = math.Sqrt2
	src.F64()[3] = math.Phi
	payload, err := alpRDCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var dst types.Vec
	if err := (alpRDCodec{}).Decode(payload, types.VecFloat64, 4, 0, &dst); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	for i, want := range []float64{math.Pi, math.E, math.Sqrt2, math.Phi} {
		got := dst.F64()[i]
		if math.Float64bits(got) != math.Float64bits(want) {
			t.Fatalf("row %d bits %x != %x", i, math.Float64bits(got), math.Float64bits(want))
		}
	}
}

func TestALPRD_PreservesNaNAndInf(t *testing.T) {
	nan := math.NaN()
	pos := math.Inf(1)
	neg := math.Inf(-1)
	negZero := math.Copysign(0, -1)
	src := types.NewVec(types.VecFloat64, 4)
	src.F64()[0] = nan
	src.F64()[1] = pos
	src.F64()[2] = neg
	src.F64()[3] = negZero
	payload, err := alpRDCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var dst types.Vec
	if err := (alpRDCodec{}).Decode(payload, types.VecFloat64, 4, 0, &dst); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !math.IsNaN(dst.F64()[0]) {
		t.Fatal("NaN not preserved")
	}
	if !math.IsInf(dst.F64()[1], 1) {
		t.Fatal("+Inf not preserved")
	}
	if !math.IsInf(dst.F64()[2], -1) {
		t.Fatal("-Inf not preserved")
	}
	if math.Float64bits(dst.F64()[3]) != math.Float64bits(negZero) {
		t.Fatal("-0 not preserved bitwise")
	}
}

func TestALPRD_CascadePicksRDForIrrationals(t *testing.T) {
	// 2048 irrationals: ALP-decimal won't round-trip the irrational base, and at
	// this page size the FastLanes block rounding doesn't bury ALP-RD's win.
	// The cascade should pick ALP-RD over Plain.
	src := types.NewVec(types.VecFloat64, 2048)
	for i := range src.F64() {
		// Vary mantissa via Ldexp on Pi so the top-16 head dict stays small
		// (just exponent-varying) but the tail is 48-bit-noisy.
		src.F64()[i] = math.Pi * (1.0 + float64(i%64)/1000.0)
	}
	enc, payload, err := Encode(src, nil)
	if err != nil {
		t.Fatalf("cascade Encode: %v", err)
	}
	if enc == types.EncodingFlat {
		t.Fatalf("cascade fell to plain for irrational floats: payload %d bytes", len(payload))
	}
}

func TestALPRD_RejectsTooManyDistinctHeads(t *testing.T) {
	// 257 distinct exponent values forces >256 distinct top-16-bits heads.
	src := types.NewVec(types.VecFloat64, 257)
	for i := range src.F64() {
		src.F64()[i] = math.Ldexp(1.0, i)
	}
	_, err := (alpRDCodec{}).Encode(src, nil)
	if err != ErrSkip {
		t.Fatalf("expected ErrSkip for too many heads, got %v", err)
	}
}
