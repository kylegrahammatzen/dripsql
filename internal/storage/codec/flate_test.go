package codec

import (
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestFlateRoundTripText(t *testing.T) {
	v := compressibleTextVec(512)
	page, err := Flate{}.Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if page.Encoding != types.EncodingFlate {
		t.Fatalf("encoding = %s, want flate", page.Encoding)
	}
	plainSize, ok := Plain{}.Estimate(v)
	if !ok {
		t.Fatal("Plain estimate returned !ok")
	}
	if len(page.Payload) >= plainSize {
		t.Fatalf("flate payload = %d, plain = %d", len(page.Payload), plainSize)
	}

	got, err := Flate{}.Decode(page)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Encoding != types.EncodingFlat || got.Var.String(0) != v.Var.String(0) || got.Var.String(511) != v.Var.String(511) {
		t.Fatalf("vec = %#v", got)
	}
}

func TestFlateDecodeSelected(t *testing.T) {
	v := compressibleTextVec(128)
	page, err := Flate{}.Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	sel := types.NewSelectionMask(v.Len)
	sel.Set(5)
	sel.Set(99)
	got, err := Flate{}.DecodeSelected(page, sel)
	if err != nil {
		t.Fatalf("DecodeSelected: %v", err)
	}
	if got.Var.String(5) != v.Var.String(5) || got.Var.String(99) != v.Var.String(99) || got.Var.String(6) != "" {
		t.Fatalf("selected varbytes = %#v", got.Var)
	}
}

func TestFlateDecodeIntoAvoidsVarBytesDataCopy(t *testing.T) {
	v := compressibleTextVec(256)
	page, err := Flate{}.Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var dst types.Vec
	if err := (Flate{}).DecodeInto(page, &dst); err != nil {
		t.Fatalf("DecodeInto: %v", err)
	}
	if dst.Var.String(123) != v.Var.String(123) {
		t.Fatalf("row 123 = %q, want %q", dst.Var.String(123), v.Var.String(123))
	}
	if cap(dst.Var.Data) == len(dst.Var.Data) {
		t.Fatal("decoded data appears to be copied into a right-sized buffer")
	}
}

func TestFlatePrepareRejectsUncompressibleText(t *testing.T) {
	if _, ok := (Flate{}).Prepare(textVec("a", "b", "c")); ok {
		t.Fatal("Prepare accepted tiny uncompressible text")
	}
}

func compressibleTextVec(rows int) types.Vec {
	values := make([]string, rows)
	for i := range values {
		values[i] = strings.Repeat("/users/events/", 8) + strings.Repeat(string(byte('a'+i%26)), 16)
	}
	return textVec(values...)
}
