// Sequence codec invariant tests: arithmetic progression detection, kind scope,
// round-trip across width-8 FOR-packable kinds, decode bounds.
package codec

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestSequence_Encoding(t *testing.T) {
	if (sequenceCodec{}).Encoding() != types.EncodingSequence {
		t.Fatal("sequenceCodec must claim Sequence encoding")
	}
}

func TestSequence_EstimateRejects(t *testing.T) {
	int32Seq := types.NewVec(types.VecInt32, 100)
	for i := range int32Seq.I32() {
		int32Seq.I32()[i] = int32(i * 5)
	}
	nonArith := types.NewVec(types.VecInt64, 4)
	copy(nonArith.I64(), []int64{0, 10, 20, 31})

	cases := []struct {
		name string
		v    types.Vec
	}{
		{"width4", int32Seq},
		{"bool", types.NewVec(types.VecBool, 64)},
		{"varbytes", types.NewVarVec(types.VecText, 4, 0)},
		{"rows0", types.NewVec(types.VecInt64, 0)},
		{"rows1", types.NewVec(types.VecInt64, 1)},
		{"nonArithmetic", nonArith},
	}
	for _, tc := range cases {
		if _, ok := Estimate(sequenceCodec{}, tc.v); ok {
			t.Fatalf("%s: Estimate must reject", tc.name)
		}
	}
}

func sequenceRoundTrip(t *testing.T, kind types.VecKind, rows int, start, step int64) {
	t.Helper()
	src := types.NewVec(kind, rows)
	for i := range src.I64() {
		src.I64()[i] = start + int64(i)*step
	}
	payload, err := sequenceCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(payload) != 16 {
		t.Fatalf("payload len=%d want 16", len(payload))
	}
	var dst types.Vec
	if err := (sequenceCodec{}).Decode(payload, kind, rows, 0, &dst); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if int(dst.Len) != rows {
		t.Fatalf("Len=%d want %d", dst.Len, rows)
	}
	for i, v := range dst.I64() {
		if want := start + int64(i)*step; v != want {
			t.Fatalf("row %d: got %d want %d", i, v, want)
		}
	}
}

func TestSequence_RoundTrip(t *testing.T) {
	sequenceRoundTrip(t, types.VecInt64, 1000, -100, 7)
	sequenceRoundTrip(t, types.VecTimestamp, 64, 1700000000000000000, 1000000000)
}

func TestSequence_DecodeRejects(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		kind    types.VecKind
		rows    int
	}{
		{"trailingBytes", make([]byte, 17), types.VecInt64, 4},
		{"negativeRows", nil, types.VecInt64, -1},
		{"shortPayload", make([]byte, 8), types.VecInt64, 4},
		{"unsupportedInt32", make([]byte, 16), types.VecInt32, 4},
		{"unsupportedText", make([]byte, 16), types.VecText, 4},
	}
	for _, tc := range cases {
		var dst types.Vec
		if err := (sequenceCodec{}).Decode(tc.payload, tc.kind, tc.rows, 0, &dst); err == nil {
			t.Fatalf("%s: Decode must reject", tc.name)
		}
	}
}

func TestSequence_EstimateMatchesEncode(t *testing.T) {
	src := types.NewVec(types.VecInt64, 50)
	for i := range src.I64() {
		src.I64()[i] = int64(i) * -3
	}
	est, ok := Estimate(sequenceCodec{}, src)
	if !ok {
		t.Fatal("arithmetic rows must Estimate")
	}
	payload, err := sequenceCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(payload) != est {
		t.Fatalf("len(Encode)=%d != Estimate=%d", len(payload), est)
	}
}

func TestSequence_DecodeRejectsSingleRow(t *testing.T) {
	var dst types.Vec
	if err := (sequenceCodec{}).Decode(make([]byte, 16), types.VecInt64, 1, 0, &dst); err == nil {
		t.Fatal("sequence decode must reject rows=1 (encoder rejects rows<2)")
	}
}
