package codec

import (
	"reflect"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestPlainRoundTripInt64(t *testing.T) {
	valid := types.NewValidity(4)
	types.SetInvalid(valid, 2)
	v := types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: 4, Valid: valid, I64: []int64{1, 2, 3, 4}}
	page, err := Plain{}.Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Plain{}.Decode(page)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Kind != types.VecInt64 || got.Encoding != types.EncodingFlat || got.Len != 4 || got.I64[3] != 4 || types.IsValid(got.Valid, 2) {
		t.Fatalf("vec = %#v", got)
	}
}

func TestPlainDecodeIntoReusesBuffers(t *testing.T) {
	first, err := Plain{}.Encode(types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: 3, I64: []int64{1, 2, 3}})
	if err != nil {
		t.Fatalf("Encode first: %v", err)
	}
	second, err := Plain{}.Encode(types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: 3, I64: []int64{4, 5, 6}})
	if err != nil {
		t.Fatalf("Encode second: %v", err)
	}
	var dst types.Vec
	if err := (Plain{}).DecodeInto(first, &dst); err != nil {
		t.Fatalf("DecodeInto first: %v", err)
	}
	ptr := &dst.I64[0]
	if err := (Plain{}).DecodeInto(second, &dst); err != nil {
		t.Fatalf("DecodeInto second: %v", err)
	}
	if &dst.I64[0] != ptr {
		t.Fatal("DecodeInto did not reuse int64 buffer")
	}
	if dst.I64[0] != 4 || dst.I64[2] != 6 {
		t.Fatalf("I64 = %v, want [4 5 6]", dst.I64)
	}
}

func TestPlainDecodeIntoZeroAllocsAfterPrewarm(t *testing.T) {
	page := plainInt64BenchPage(t)
	var dst types.Vec
	if err := (Plain{}).DecodeInto(page, &dst); err != nil {
		t.Fatalf("prewarm DecodeInto: %v", err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		if err := (Plain{}).DecodeInto(page, &dst); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("DecodeInto allocs/run = %v, want 0", allocs)
	}
}

func TestPlainDecodeIntoReusesVarBytesBuffers(t *testing.T) {
	first, err := Plain{}.Encode(textVec("aa", "bb"))
	if err != nil {
		t.Fatalf("Encode first: %v", err)
	}
	second, err := Plain{}.Encode(textVec("cc", "dd"))
	if err != nil {
		t.Fatalf("Encode second: %v", err)
	}
	var dst types.Vec
	if err := (Plain{}).DecodeInto(first, &dst); err != nil {
		t.Fatalf("DecodeInto first: %v", err)
	}
	offsetsPtr := &dst.Var.Offsets[0]
	dataPtr := &dst.Var.Data[0]
	if err := (Plain{}).DecodeInto(second, &dst); err != nil {
		t.Fatalf("DecodeInto second: %v", err)
	}
	if &dst.Var.Offsets[0] != offsetsPtr || &dst.Var.Data[0] != dataPtr {
		t.Fatal("DecodeInto did not reuse varbytes buffers")
	}
	if dst.Var.String(0) != "cc" || dst.Var.String(1) != "dd" {
		t.Fatalf("Var = %#v", dst.Var)
	}
}

func TestPlainRoundTripBool(t *testing.T) {
	v := types.Vec{Kind: types.VecBool, Encoding: types.EncodingFlat, Len: 3, BoolBits: []uint64{0b101}}
	page, err := Plain{}.Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Plain{}.Decode(page)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.BoolBits[0] != 0b101 {
		t.Fatalf("bool bits = %b", got.BoolBits[0])
	}
}

func TestPlainRoundTripText(t *testing.T) {
	v := textVec("signup", "checkout", "login")
	page, err := Plain{}.Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Plain{}.Decode(page)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Kind != types.VecText || got.Var.String(1) != "checkout" {
		t.Fatalf("vec = %#v", got)
	}
}

func TestPlainRoundTripSupportedKinds(t *testing.T) {
	uuidA := types.UUID16{0: 1, 15: 2}
	uuidB := types.UUID16{0: 3, 15: 4}
	tests := []struct {
		name  string
		vec   types.Vec
		check func(*testing.T, types.Vec)
	}{
		{name: "int16", vec: types.Vec{Kind: types.VecInt16, Encoding: types.EncodingFlat, Len: 3, I16: []int16{1, -2, 3}}, check: func(t *testing.T, got types.Vec) {
			if !reflect.DeepEqual(got.I16, []int16{1, -2, 3}) {
				t.Fatalf("I16 = %v", got.I16)
			}
		}},
		{name: "int32", vec: types.Vec{Kind: types.VecInt32, Encoding: types.EncodingFlat, Len: 3, I32: []int32{1, -2, 3}}, check: func(t *testing.T, got types.Vec) {
			if !reflect.DeepEqual(got.I32, []int32{1, -2, 3}) {
				t.Fatalf("I32 = %v", got.I32)
			}
		}},
		{name: "date", vec: types.Vec{Kind: types.VecDate, Encoding: types.EncodingFlat, Len: 2, I32: []int32{10, 20}}, check: func(t *testing.T, got types.Vec) {
			if !reflect.DeepEqual(got.I32, []int32{10, 20}) {
				t.Fatalf("Date I32 = %v", got.I32)
			}
		}},
		{name: "int64", vec: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: 3, I64: []int64{1, -2, 3}}, check: func(t *testing.T, got types.Vec) {
			if !reflect.DeepEqual(got.I64, []int64{1, -2, 3}) {
				t.Fatalf("I64 = %v", got.I64)
			}
		}},
		{name: "decimal64", vec: types.Vec{Kind: types.VecDecimal64, Encoding: types.EncodingFlat, Len: 2, I64: []int64{1234, -50}}, check: func(t *testing.T, got types.Vec) {
			if !reflect.DeepEqual(got.I64, []int64{1234, -50}) {
				t.Fatalf("Decimal I64 = %v", got.I64)
			}
		}},
		{name: "timestamp", vec: types.Vec{Kind: types.VecTimestamp, Encoding: types.EncodingFlat, Len: 2, I64: []int64{100, 200}}, check: func(t *testing.T, got types.Vec) {
			if !reflect.DeepEqual(got.I64, []int64{100, 200}) {
				t.Fatalf("Timestamp I64 = %v", got.I64)
			}
		}},
		{name: "time", vec: types.Vec{Kind: types.VecTime, Encoding: types.EncodingFlat, Len: 2, I64: []int64{300, 400}}, check: func(t *testing.T, got types.Vec) {
			if !reflect.DeepEqual(got.I64, []int64{300, 400}) {
				t.Fatalf("Time I64 = %v", got.I64)
			}
		}},
		{name: "float32", vec: types.Vec{Kind: types.VecFloat32, Encoding: types.EncodingFlat, Len: 3, F32: []float32{1.25, -2.5, 3}}, check: func(t *testing.T, got types.Vec) {
			if !reflect.DeepEqual(got.F32, []float32{1.25, -2.5, 3}) {
				t.Fatalf("F32 = %v", got.F32)
			}
		}},
		{name: "float64", vec: types.Vec{Kind: types.VecFloat64, Encoding: types.EncodingFlat, Len: 3, F64: []float64{1.25, -2.5, 3}}, check: func(t *testing.T, got types.Vec) {
			if !reflect.DeepEqual(got.F64, []float64{1.25, -2.5, 3}) {
				t.Fatalf("F64 = %v", got.F64)
			}
		}},
		{name: "uuid", vec: types.Vec{Kind: types.VecUUID, Encoding: types.EncodingFlat, Len: 2, UUID: []types.UUID16{uuidA, uuidB}}, check: func(t *testing.T, got types.Vec) {
			if !reflect.DeepEqual(got.UUID, []types.UUID16{uuidA, uuidB}) {
				t.Fatalf("UUID = %v", got.UUID)
			}
		}},
		{name: "enum32", vec: types.Vec{Kind: types.VecEnum32, Encoding: types.EncodingFlat, Len: 3, U32: []uint32{1, 2, 3}}, check: func(t *testing.T, got types.Vec) {
			if !reflect.DeepEqual(got.U32, []uint32{1, 2, 3}) {
				t.Fatalf("U32 = %v", got.U32)
			}
		}},
		{name: "bytes", vec: varVec(types.VecBytes, []string{"aa", "", "ccc"}), check: func(t *testing.T, got types.Vec) {
			if got.Var.String(0) != "aa" || got.Var.String(1) != "" || got.Var.String(2) != "ccc" {
				t.Fatalf("bytes var = %#v", got.Var)
			}
		}},
		{name: "json", vec: varVec(types.VecJSON, []string{`{"a":1}`, `[]`}), check: func(t *testing.T, got types.Vec) {
			if got.Var.String(0) != `{"a":1}` || got.Var.String(1) != `[]` {
				t.Fatalf("json var = %#v", got.Var)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			page, err := Plain{}.Encode(tt.vec)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			got, err := Plain{}.Decode(page)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if got.Kind != tt.vec.Kind || got.Encoding != types.EncodingFlat || got.Len != tt.vec.Len {
				t.Fatalf("got header = %#v, want kind=%s len=%d", got, tt.vec.Kind, tt.vec.Len)
			}
			tt.check(t, got)
		})
	}
}

func TestPlainRoundTripEmptyPage(t *testing.T) {
	v := types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: 0}
	page, err := Plain{}.Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Plain{}.Decode(page)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Kind != types.VecInt64 || got.Len != 0 || len(got.I64) != 0 {
		t.Fatalf("vec = %#v", got)
	}
}

func TestPlainRoundTripSingleRow(t *testing.T) {
	v := types.Vec{Kind: types.VecInt32, Encoding: types.EncodingFlat, Len: 1, I32: []int32{99}}
	page, err := Plain{}.Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Plain{}.Decode(page)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(got.I32) != 1 || got.I32[0] != 99 {
		t.Fatalf("I32 = %v", got.I32)
	}
}

func TestPlainRoundTripAllNullPage(t *testing.T) {
	valid := types.NewValidity(3)
	for row := range 3 {
		types.SetInvalid(valid, row)
	}
	v := types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: 3, Valid: valid, I64: []int64{1, 2, 3}}
	page, err := Plain{}.Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Plain{}.Decode(page)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if page.NullCount != 3 || types.ValidCount(got.Valid, got.Len) != 0 {
		t.Fatalf("page nulls = %d vec valid = %#v", page.NullCount, got.Valid)
	}
}

func TestPlainEstimateMatchesPayload(t *testing.T) {
	v := types.Vec{Kind: types.VecInt32, Encoding: types.EncodingFlat, Len: 3, I32: []int32{1, 2, 3}}
	size, ok := Plain{}.Estimate(v)
	if !ok {
		t.Fatal("Estimate returned !ok")
	}
	page, err := Plain{}.Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if size != len(page.Payload) {
		t.Fatalf("estimate = %d, payload = %d", size, len(page.Payload))
	}
}

func TestPlainDecodeTruncated(t *testing.T) {
	_, err := Plain{}.Decode(Page{Kind: types.VecInt64, Encoding: types.EncodingFlat, Rows: 2, Payload: []byte{1}})
	if err == nil {
		t.Fatal("expected truncated payload error")
	}
}

func textVec(values ...string) types.Vec {
	return varVec(types.VecText, values)
}

func varVec(kind types.VecKind, values []string) types.Vec {
	v := types.NewVarBytes(len(values), 0)
	for i, value := range values {
		v.AppendString(i, value)
	}
	return types.Vec{Kind: kind, Encoding: types.EncodingFlat, Len: len(values), Var: v}
}
