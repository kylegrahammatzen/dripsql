// Pcodec round-trip and skip-precondition tests across int64 and int32 kinds.
// Verifies multimodal data round-trips and that small pages bypass the codec.
package codec

import (
	"errors"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestPcodec_RoundTrip_Int64Multimodal(t *testing.T) {
	rows := 4096
	v := types.Vec{Kind: types.VecInt64, Len: int32(rows)}
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
	var dst types.Vec
	if err := c.Decode(payload, types.VecInt64, rows, 0, &dst); err != nil {
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
	v := types.Vec{Kind: types.VecInt32, Len: int32(rows)}
	v.EnsureFixedBytes(rows)
	for i := range v.I32() {
		v.I32()[i] = int32(-1000 + i*3)
	}
	c := pcodecCodec{}
	payload, err := c.Encode(v, &EncodeContext{Scratch: NewScratchPool()})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	var dst types.Vec
	if err := c.Decode(payload, types.VecInt32, rows, 0, &dst); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for i, got := range dst.I32() {
		if got != v.I32()[i] {
			t.Fatalf("row %d: got %d want %d", i, got, v.I32()[i])
		}
	}
}

func TestPcodec_SkipsShortPage(t *testing.T) {
	v := types.Vec{Kind: types.VecInt64, Len: 100}
	v.EnsureFixedBytes(100)
	_, err := (pcodecCodec{}).Encode(v, &EncodeContext{Scratch: NewScratchPool()})
	if !errors.Is(err, ErrSkip) {
		t.Fatalf("expected ErrSkip for short page, got %v", err)
	}
}

func TestPcodec_SkipsNonFOR(t *testing.T) {
	v := types.Vec{Kind: types.VecFloat64, Len: 4096}
	v.EnsureFixedBytes(4096)
	_, err := (pcodecCodec{}).Encode(v, &EncodeContext{Scratch: NewScratchPool()})
	if !errors.Is(err, ErrSkip) {
		t.Fatalf("expected ErrSkip for float64, got %v", err)
	}
}

func TestPcodec_EncodingIsPcodec(t *testing.T) {
	if (pcodecCodec{}).Encoding() != types.EncodingPcodec {
		t.Fatal("pcodecCodec must claim EncodingPcodec")
	}
}
