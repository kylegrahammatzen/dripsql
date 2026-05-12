package codec

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestZstdRoundTripText(t *testing.T) {
	v := compressibleTextVec(512)
	page, err := Zstd{}.Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if page.Encoding != types.EncodingZstd {
		t.Fatalf("encoding = %s, want zstd", page.Encoding)
	}
	plainSize, ok := Plain{}.Estimate(v)
	if !ok {
		t.Fatal("Plain estimate returned !ok")
	}
	if len(page.Payload) >= plainSize {
		t.Fatalf("zstd payload = %d, plain = %d", len(page.Payload), plainSize)
	}
	got, err := Zstd{}.Decode(page)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Encoding != types.EncodingFlat || got.Var.String(0) != v.Var.String(0) || got.Var.String(511) != v.Var.String(511) {
		t.Fatalf("vec = %#v", got)
	}
}

func TestZstdDecodeSelected(t *testing.T) {
	v := compressibleTextVec(128)
	page, err := Zstd{}.Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	sel := types.NewSelectionMask(v.Len)
	sel.Set(5)
	sel.Set(99)
	got, err := Zstd{}.DecodeSelected(page, sel)
	if err != nil {
		t.Fatalf("DecodeSelected: %v", err)
	}
	if got.Var.String(5) != v.Var.String(5) || got.Var.String(99) != v.Var.String(99) {
		t.Fatalf("selected varbytes = %#v", got.Var)
	}
}

func TestZstdPrepareRejectsUncompressibleText(t *testing.T) {
	if _, ok := (Zstd{}).Prepare(textVec("a", "b", "c")); ok {
		t.Fatal("Prepare accepted tiny uncompressible text")
	}
}
