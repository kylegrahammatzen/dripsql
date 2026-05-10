package codec

import (
	"reflect"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestConstantCodecRoundTripInt64(t *testing.T) {
	v := types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: 4, I64: []int64{7, 7, 7, 7}}
	page, err := (Constant{}).Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if page.Encoding != types.EncodingConstant || len(page.Payload) != 8 {
		t.Fatalf("page = %#v", page)
	}
	got, err := (Constant{}).Decode(page)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Encoding != types.EncodingFlat || !reflect.DeepEqual(got.I64, []int64{7, 7, 7, 7}) {
		t.Fatalf("decoded vec = %#v", got)
	}
}

func TestConstantCodecRoundTripText(t *testing.T) {
	v := textVec("checkout", "checkout", "checkout")
	page, err := (Constant{}).Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if page.Encoding != types.EncodingConstant || string(page.Payload) != "checkout" {
		t.Fatalf("page = %#v", page)
	}
	got, err := (Constant{}).Decode(page)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	for row := 0; row < got.Len; row++ {
		if got.Var.String(row) != "checkout" {
			t.Fatalf("row %d = %q", row, got.Var.String(row))
		}
	}
}

func TestConstantCodecRoundTripAllNull(t *testing.T) {
	v := types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: 3, I64: []int64{1, 2, 3}, Valid: types.NewValidity(3)}
	for row := 0; row < v.Len; row++ {
		types.SetInvalid(v.Valid, row)
	}
	page, err := (Constant{}).Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if page.NullCount != 3 || len(page.Payload) != 0 {
		t.Fatalf("page = %#v", page)
	}
	got, err := (Constant{}).Decode(page)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	for row := 0; row < got.Len; row++ {
		if types.IsValid(got.Valid, row) {
			t.Fatalf("row %d should be invalid", row)
		}
	}
}

func TestConstantCodecEstimateAndRejectsNonConstant(t *testing.T) {
	v := types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: 3, I64: []int64{7, 7, 7}}
	if size, ok := (Constant{}).Estimate(v); !ok || size != 8 {
		t.Fatalf("Estimate = %d/%v, want 8/true", size, ok)
	}
	if _, ok := (Constant{}).Estimate(types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: 2, I64: []int64{1, 2}}); ok {
		t.Fatal("non-constant estimate should be invalid")
	}
	if _, err := (Constant{}).Encode(types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: 2, I64: []int64{1, 2}}); err == nil {
		t.Fatal("expected non-constant encode error")
	}
}
