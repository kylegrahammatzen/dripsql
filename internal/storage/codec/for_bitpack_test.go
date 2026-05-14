package codec

import (
	"reflect"
	"strconv"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestFORBitPackRoundTripNumericKinds(t *testing.T) {
	tests := []struct {
		name string
		vec  types.Vec
	}{
		{name: "int16", vec: types.Vec{Kind: types.VecInt16, Encoding: types.EncodingFlat, Len: 5, I16: []int16{-4, -3, -2, -1, 0}}},
		{name: "int32", vec: types.Vec{Kind: types.VecInt32, Encoding: types.EncodingFlat, Len: 5, I32: []int32{100, 101, 102, 103, 104}}},
		{name: "date", vec: types.Vec{Kind: types.VecDate, Encoding: types.EncodingFlat, Len: 5, I32: []int32{20000, 20001, 20003, 20004, 20006}}},
		{name: "int64", vec: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: 5, I64: []int64{-10, -8, -7, -4, -3}}},
		{name: "timestamp", vec: types.Vec{Kind: types.VecTimestamp, Encoding: types.EncodingFlat, Len: 5, I64: []int64{1700000000000, 1700000000001, 1700000000002, 1700000000003, 1700000000004}}},
		{name: "time", vec: types.Vec{Kind: types.VecTime, Encoding: types.EncodingFlat, Len: 5, I64: []int64{3600000000, 3600001000, 3600002000, 3600003000, 3600004000}}},
		{name: "enum32", vec: types.Vec{Kind: types.VecEnum32, Encoding: types.EncodingFlat, Len: 6, U32: []uint32{1, 2, 1, 3, 2, 1}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page, err := (FORBitPack{}).Encode(tt.vec)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			got, err := (FORBitPack{}).Decode(page)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if !forBitPackVecEqual(got, tt.vec) {
				t.Fatalf("decoded vec = %#v, want %#v", got, tt.vec)
			}
		})
	}
}

func TestFORBitPackRoundTripNulls(t *testing.T) {
	valid := types.NewValidity(6)
	types.SetInvalid(valid, 1)
	types.SetInvalid(valid, 4)
	v := types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: 6, I64: []int64{-12, 999, -9, -8, 999, -7}, Valid: valid}
	page, err := (FORBitPack{}).Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := (FORBitPack{}).Decode(page)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !reflect.DeepEqual(got.Valid, valid) {
		t.Fatalf("validity = %#v, want %#v", got.Valid, valid)
	}
	for row := 0; row < v.Len; row++ {
		if types.IsValid(valid, row) && got.I64[row] != v.I64[row] {
			t.Fatalf("row %d = %d, want %d", row, got.I64[row], v.I64[row])
		}
	}
}

func TestFORBitPackEstimateMatchesPayload(t *testing.T) {
	v := types.Vec{Kind: types.VecInt32, Encoding: types.EncodingFlat, Len: 8, I32: []int32{1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007}}
	page, err := (FORBitPack{}).Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	size, ok := (FORBitPack{}).Estimate(v)
	if !ok || size != len(page.Payload) {
		t.Fatalf("Estimate = %d/%v, payload len = %d", size, ok, len(page.Payload))
	}
	plainSize, ok := (Plain{}).Estimate(v)
	if !ok {
		t.Fatal("Plain estimate rejected vector")
	}
	if size >= plainSize {
		t.Fatalf("FOR estimate = %d, plain estimate = %d; expected FOR to be smaller", size, plainSize)
	}
}

func TestFORBitPackRejectsUnsupportedInputs(t *testing.T) {
	if _, ok := (FORBitPack{}).Estimate(textVec("a", "b")); ok {
		t.Fatal("text estimate should be rejected")
	}
	if _, ok := (FORBitPack{}).Estimate(types.Vec{Kind: types.VecInt64, Encoding: types.EncodingConstant, Len: 2, Encoded: &types.EncodedState{ConstantI64: 7, ConstantValid: true}}); ok {
		t.Fatal("non-flat estimate should be rejected")
	}
	if _, ok := (FORBitPack{}).Estimate(types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: 3, I64: []int64{7, 7, 7}}); ok {
		t.Fatal("constant page estimate should be rejected")
	}
	valid := types.NewValidity(2)
	types.SetInvalid(valid, 0)
	types.SetInvalid(valid, 1)
	if _, ok := (FORBitPack{}).Estimate(types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: 2, I64: []int64{1, 2}, Valid: valid}); ok {
		t.Fatal("all-null estimate should be rejected")
	}
}

func TestFORBitPackDecodeIntoReusesBuffers(t *testing.T) {
	v := types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: 4, I64: []int64{20, 21, 22, 23}}
	page, err := (FORBitPack{}).Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	dst := types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, I64: make([]int64, 0, 16)}
	if err := (FORBitPack{}).DecodeInto(page, &dst); err != nil {
		t.Fatalf("DecodeInto first: %v", err)
	}
	before := &dst.I64[:cap(dst.I64)][0]
	if err := (FORBitPack{}).DecodeInto(page, &dst); err != nil {
		t.Fatalf("DecodeInto second: %v", err)
	}
	after := &dst.I64[:cap(dst.I64)][0]
	if before != after {
		t.Fatal("DecodeInto did not reuse int64 buffer")
	}
}

func TestFORBitPackMalformedPayloads(t *testing.T) {
	v := types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: 4, I64: []int64{20, 21, 22, 23}}
	page, err := (FORBitPack{}).Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	badHeader := page
	badHeader.Payload = badHeader.Payload[:8]
	if _, err := (FORBitPack{}).Decode(badHeader); err == nil {
		t.Fatal("expected truncated header error")
	}
	badWidth := page
	badWidth.Payload = append([]byte(nil), page.Payload...)
	badWidth.Payload[8] = 65
	if _, err := (FORBitPack{}).Decode(badWidth); err == nil {
		t.Fatal("expected invalid width error")
	}
	badValues := page
	badValues.Payload = badValues.Payload[:len(badValues.Payload)-1]
	if _, err := (FORBitPack{}).Decode(badValues); err == nil {
		t.Fatal("expected truncated values error")
	}
}

func TestFORBitPackBulkUnpackMatchesNaive(t *testing.T) {
	for _, width := range []int{1, 2, 3, 7, 8, 10, 12, 16, 24, 31, 32, 40, 56, 63, 64} {
		t.Run(strconv.Itoa(width), func(t *testing.T) {
			payload := make([]byte, (types.StandardBatchRows*width+7)/8)
			mask := uint64(^uint64(0))
			if width < 64 {
				mask = (uint64(1) << uint(width)) - 1
			}
			for row := range types.StandardBatchRows {
				value := (uint64(row*row) + uint64(row<<3) + 17) & mask
				bitpackSet(payload, row, width, value)
			}
			dst := make([]int64, types.StandardBatchRows)
			bitpackUnpack(payload, dst, 0, width)
			for row := range types.StandardBatchRows {
				want := int64(bitpackGetNaive(payload, row, width))
				if dst[row] != want {
					t.Fatalf("row %d bulk = %d, naive = %d", row, dst[row], want)
				}
			}
			if width <= 32 {
				dst32 := make([]int32, types.StandardBatchRows)
				bitpackUnpack(payload, dst32, 0, width)
				for row := range types.StandardBatchRows {
					want := int32(bitpackGetNaive(payload, row, width))
					if dst32[row] != want {
						t.Fatalf("row %d int32 bulk = %d, naive = %d", row, dst32[row], want)
					}
				}
			}
			if width <= 16 {
				dst16 := make([]int16, types.StandardBatchRows)
				bitpackUnpack(payload, dst16, 0, width)
				for row := range types.StandardBatchRows {
					want := int16(bitpackGetNaive(payload, row, width))
					if dst16[row] != want {
						t.Fatalf("row %d int16 bulk = %d, naive = %d", row, dst16[row], want)
					}
				}
			}
		})
	}
}

func TestFORBitPackPicker(t *testing.T) {
	tenant := make([]int64, types.StandardBatchRows)
	for i := range tenant {
		tenant[i] = int64(i & 255)
	}
	v := types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: len(tenant), I64: tenant}
	best, ok := PickSmallest(v, Plain{}, Constant{}, FORBitPack{})
	if !ok {
		t.Fatal("PickSmallest returned no codec")
	}
	if best.Encoding() != types.EncodingFORBitPack {
		t.Fatalf("PickSmallest chose %s, expected for+bitpack", best.Encoding())
	}
}

func forBitPackVecEqual(a, b types.Vec) bool {
	if a.Kind != b.Kind || a.Encoding != types.EncodingFlat || b.Encoding != types.EncodingFlat || a.Len != b.Len || !reflect.DeepEqual(a.Valid, b.Valid) {
		return false
	}
	switch a.Kind {
	case types.VecInt16:
		return reflect.DeepEqual(a.I16[:a.Len], b.I16[:b.Len])
	case types.VecInt32, types.VecDate:
		return reflect.DeepEqual(a.I32[:a.Len], b.I32[:b.Len])
	case types.VecInt64, types.VecTimestamp, types.VecTime:
		return reflect.DeepEqual(a.I64[:a.Len], b.I64[:b.Len])
	case types.VecEnum32:
		return reflect.DeepEqual(a.U32[:a.Len], b.U32[:b.Len])
	default:
		return false
	}
}
