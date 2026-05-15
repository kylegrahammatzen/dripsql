// Delta+bitpack round-trip + estimate tests for monotonic and near-monotonic series.
package codec

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestDeltaBitpack_EncodingIsDelta(t *testing.T) {
	if (deltaBitpackCodec{}).Encoding() != types.EncodingDeltaBitPack {
		t.Fatal("deltaBitpackCodec must claim DeltaBitPack encoding")
	}
}

func TestDeltaBitpack_RoundTrip(t *testing.T) {
	cases := []struct {
		name string
		kind types.VecKind
		rows int
		fill func(v types.Vec)
		read func(v types.Vec, i int) int64
	}{
		{"MonotonicInt64", types.VecInt64, 2048,
			func(v types.Vec) {
				val := int64(1_000_000_000)
				for i := range v.I64() {
					v.I64()[i] = val
					val += int64(3 + i%5)
				}
			},
			func(v types.Vec, i int) int64 { return v.I64()[i] }},
		{"Timestamp", types.VecTimestamp, 100,
			func(v types.Vec) {
				val := int64(1700000000_000_000_000)
				for i := range v.I64() {
					v.I64()[i] = val
					val += 1_000_000_000 + int64(i%3)
				}
			},
			func(v types.Vec, i int) int64 { return v.I64()[i] }},
		{"Int32", types.VecInt32, 500,
			func(v types.Vec) {
				val := int32(-1000)
				for i := range v.I32() {
					v.I32()[i] = val
					val += int32(1 + i%2)
				}
			},
			func(v types.Vec, i int) int64 { return int64(v.I32()[i]) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := types.NewVec(tc.kind, tc.rows)
			tc.fill(src)
			payload, err := deltaBitpackCodec{}.Encode(src, nil)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			var dst types.Vec
			if err := (deltaBitpackCodec{}).Decode(payload, tc.kind, tc.rows, 0, &dst); err != nil {
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

func TestDeltaBitpack_EstimateRejectsConstantDeltas(t *testing.T) {
	src := types.NewVec(types.VecInt64, 100)
	for i := range src.I64() {
		src.I64()[i] = int64(i) * 5
	}
	if _, ok := (deltaBitpackCodec{}).Estimate(src); ok {
		t.Fatal("Estimate must reject when all deltas are equal (Sequence wins)")
	}
}

func TestDeltaBitpack_EstimateRejectsTooFewRows(t *testing.T) {
	for _, n := range []int{0, 1} {
		v := types.NewVec(types.VecInt64, n)
		if _, ok := (deltaBitpackCodec{}).Estimate(v); ok {
			t.Fatalf("Estimate must reject rows=%d", n)
		}
	}
}

func TestDeltaBitpack_EstimateRejectsNonFORKinds(t *testing.T) {
	for _, kind := range []types.VecKind{types.VecBool, types.VecFloat32, types.VecText, types.VecUUID} {
		var v types.Vec
		if kind == types.VecText {
			v = types.NewVarVec(kind, 4, 0)
		} else {
			v = types.NewVec(kind, 4)
		}
		if _, ok := (deltaBitpackCodec{}).Estimate(v); ok {
			t.Fatalf("Estimate must reject %v", kind)
		}
	}
}

func TestDeltaBitpack_DecodeRejects(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		kind    types.VecKind
		rows    int
	}{
		{"truncatedHeader", make([]byte, 10), types.VecInt64, 4},
		{"rowsLessThan2", make([]byte, deltaHeaderSize), types.VecInt64, 1},
		{"negativeRows", nil, types.VecInt64, -1},
		{"nonFORKind", make([]byte, deltaHeaderSize+128), types.VecText, 2},
	}
	for _, tc := range cases {
		var dst types.Vec
		if err := (deltaBitpackCodec{}).Decode(tc.payload, tc.kind, tc.rows, 0, &dst); err == nil {
			t.Fatalf("%s: Decode must reject", tc.name)
		}
	}
}

func TestDeltaBitpack_EstimateMatchesEncode(t *testing.T) {
	src := types.NewVec(types.VecInt64, 100)
	val := int64(0)
	for i := range src.I64() {
		src.I64()[i] = val
		val += int64(1 + i%4)
	}
	est, ok := deltaBitpackCodec{}.Estimate(src)
	if !ok {
		t.Fatal("Estimate must accept near-monotonic series")
	}
	payload, err := deltaBitpackCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(payload) != est {
		t.Fatalf("len(Encode)=%d != Estimate=%d", len(payload), est)
	}
}

func TestDeltaBitpack_DecodeRangeChecksInt32(t *testing.T) {
	src := types.NewVec(types.VecInt32, 3)
	src.I32()[0], src.I32()[1], src.I32()[2] = 0, 10, 25
	payload, err := deltaBitpackCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	binary.LittleEndian.PutUint64(payload[0:8], uint64(int64(math.MaxInt32-5)))
	var dst types.Vec
	if err := (deltaBitpackCodec{}).Decode(payload, types.VecInt32, 3, 0, &dst); err == nil {
		t.Fatal("Delta decode must error when accumulated values would overflow int32")
	}
}
