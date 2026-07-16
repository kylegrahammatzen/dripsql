// Pcodec round-trip and skip-precondition tests across int64 and int32 kinds.
// Verifies multimodal data round-trips and that small pages bypass the codec.
package codec

import (
	"errors"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestPcodec_RoundTrip_Int64Multimodal(t *testing.T) {
	rows := 4096
	v := vector.Vec{Kind: vector.VecInt64, Len: int32(rows)}
	v.EnsureFixedBytes(rows)
	for i := range v.I64() {
		switch (i / 1024) % 3 {
		case 0:
			v.I64()[i] = int64(1000 + i%50)
		case 1:
			v.I64()[i] = int64(10_000_000_000 + int64(i%500))
		case 2:
			v.I64()[i] = int64(-77 + int64(i%20))
		}
	}
	c := pcodecCodec{}
	ctx := &EncodeContext{Scratch: NewScratchPool()}
	payload, err := c.Encode(v, ctx)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var dst vector.Vec
	if err := c.Decode(payload, vector.VecInt64, rows, 0, &dst); err != nil {
		t.Fatalf("decode: %v", err)
	}
	got := dst.I64()
	want := v.I64()
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("row %d: got %d want %d", i, got[i], want[i])
		}
	}
}

func TestPcodec_RoundTrip_Int32(t *testing.T) {
	rows := 3000
	v := vector.Vec{Kind: vector.VecInt32, Len: int32(rows)}
	v.EnsureFixedBytes(rows)
	for i := range v.I32() {
		v.I32()[i] = int32(-1000 + i*3)
	}
	c := pcodecCodec{}
	payload, err := c.Encode(v, &EncodeContext{Scratch: NewScratchPool()})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var dst vector.Vec
	if err := c.Decode(payload, vector.VecInt32, rows, 0, &dst); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for i, got := range dst.I32() {
		if got != v.I32()[i] {
			t.Fatalf("row %d: got %d want %d", i, got, v.I32()[i])
		}
	}
}

func TestPcodec_SkipsShortPage(t *testing.T) {
	v := vector.Vec{Kind: vector.VecInt64, Len: 100}
	v.EnsureFixedBytes(100)
	_, err := (pcodecCodec{}).Encode(v, &EncodeContext{Scratch: NewScratchPool()})
	if !errors.Is(err, ErrSkip) {
		t.Fatalf("expected ErrSkip for short page, got %v", err)
	}
}

func TestPcodec_RoundTrip_Float64Clustered(t *testing.T) {
	rows := 4096
	v := vector.Vec{Kind: vector.VecFloat64, Len: int32(rows)}
	v.EnsureFixedBytes(rows)
	for i := range v.F64() {
		switch (i / 1024) % 2 {
		case 0:
			v.F64()[i] = 1.5 + float64(i%32)*0.001
		case 1:
			v.F64()[i] = 1e9 + float64(i%32)
		}
	}
	c := pcodecCodec{}
	payload, err := c.Encode(v, &EncodeContext{Scratch: NewScratchPool()})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var dst vector.Vec
	if err := c.Decode(payload, vector.VecFloat64, rows, 0, &dst); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for i, want := range v.F64() {
		if dst.F64()[i] != want {
			t.Fatalf("row %d: got %v want %v", i, dst.F64()[i], want)
		}
	}
}

func TestPcodec_SkipsFloat32(t *testing.T) {
	v := vector.Vec{Kind: vector.VecFloat32, Len: 4096}
	v.EnsureFixedBytes(4096)
	_, err := (pcodecCodec{}).Encode(v, &EncodeContext{Scratch: NewScratchPool()})
	if !errors.Is(err, ErrSkip) {
		t.Fatalf("expected ErrSkip for float32, got %v", err)
	}
}

func TestPcodec_EncodingIsPcodec(t *testing.T) {
	if (pcodecCodec{}).Encoding() != schema.EncPcodec {
		t.Fatal("pcodecCodec must claim EncPcodec")
	}
}
