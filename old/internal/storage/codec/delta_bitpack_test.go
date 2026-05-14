package codec

import (
	"reflect"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestDeltaBitPackRoundTripSortedInt64(t *testing.T) {
	v := types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: 8, I64: []int64{
		1_700_000_000_000,
		1_700_000_000_010,
		1_700_000_000_021,
		1_700_000_000_033,
		1_700_000_000_046,
		1_700_000_000_060,
		1_700_000_000_075,
		1_700_000_000_091,
	}}
	page, err := (DeltaBitPack{}).Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := (DeltaBitPack{}).Decode(page)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !reflect.DeepEqual(got.I64[:got.Len], v.I64[:v.Len]) {
		t.Fatalf("decoded = %v, want %v", got.I64[:got.Len], v.I64[:v.Len])
	}
	if got.Encoding != types.EncodingFlat {
		t.Fatalf("decoded encoding = %s, want flat", got.Encoding)
	}
}

func TestDeltaBitPackRoundTripDate(t *testing.T) {
	v := types.Vec{Kind: types.VecDate, Encoding: types.EncodingFlat, Len: 5, I32: []int32{20000, 20001, 20002, 20003, 20004}}
	page, err := (DeltaBitPack{}).Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := (DeltaBitPack{}).Decode(page)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !reflect.DeepEqual(got.I32[:got.Len], v.I32[:v.Len]) {
		t.Fatalf("decoded = %v, want %v", got.I32[:got.Len], v.I32[:v.Len])
	}
}

func TestDeltaBitPackBeatsFORWhenSorted(t *testing.T) {
	// Sorted timestamps over a wide absolute range with small deltas — FOR
	// pays for the wide range, Delta pays only for the small deltas.
	rows := 256
	values := make([]int64, rows)
	base := int64(1_700_000_000_000)
	for i := range values {
		values[i] = base + int64(i)*7
	}
	v := types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: rows, I64: values}
	forSize, ok := (FORBitPack{}).Estimate(v)
	if !ok {
		t.Fatal("FORBitPack.Estimate returned !ok")
	}
	deltaSize, ok := (DeltaBitPack{}).Estimate(v)
	if !ok {
		t.Fatal("DeltaBitPack.Estimate returned !ok")
	}
	if deltaSize >= forSize {
		t.Fatalf("delta (%d) should beat FOR (%d) on sorted data", deltaSize, forSize)
	}
}

func TestDeltaBitPackPickerOnSortedData(t *testing.T) {
	rows := 128
	values := make([]int64, rows)
	for i := range values {
		values[i] = 1_700_000_000_000 + int64(i)*3
	}
	v := types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: rows, I64: values}
	best, ok := PickSmallest(v, Plain{}, Constant{}, FORBitPack{}, DeltaBitPack{})
	if !ok {
		t.Fatal("PickSmallest returned no codec")
	}
	if best.Encoding() != types.EncodingDeltaBitPack {
		t.Fatalf("PickSmallest chose %s, want delta+bitpack", best.Encoding())
	}
}

func TestDeltaBitPackRejectsNullableInput(t *testing.T) {
	valid := types.NewValidity(4)
	types.SetInvalid(valid, 2)
	v := types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: 4, Valid: valid, I64: []int64{1, 2, 0, 4}}
	if _, ok := (DeltaBitPack{}).Estimate(v); ok {
		t.Fatal("Estimate should reject vectors with nulls")
	}
	if _, err := (DeltaBitPack{}).Encode(v); err == nil {
		t.Fatal("Encode should reject vectors with nulls")
	}
}
