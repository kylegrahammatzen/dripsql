// FSST codec round-trip + cascade-preference test on repetitive URL-shaped strings.
// Asserts decode is bitwise equal to input and that cascade picks FSST when it beats plain.
package codec

import (
	"fmt"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestFSST_RoundTrip(t *testing.T) {
	inputs := []string{
		"https://example.com/path/a",
		"https://example.com/path/b",
		"https://example.com/path/c",
		"https://example.com/path/d",
		"https://example.com/static/img.png",
		"https://example.com/static/css.css",
		"https://example.com/static/js.js",
		"",
		"x",
	}
	v := vector.NewVarVec(vector.VecText, len(inputs), 0)
	vb := v.Var()
	for i, s := range inputs {
		vb.AppendString(i, s)
	}
	codec := fsstCodec{}
	payload, err := codec.Encode(v, &EncodeContext{Scratch: NewScratchPool()})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var got vector.Vec
	if err := codec.Decode(payload, vector.VecText, len(inputs), 0, &got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	gb := got.Var()
	for i, want := range inputs {
		if string(gb.Bytes(i)) != want {
			t.Fatalf("row %d: got %q want %q", i, gb.Bytes(i), want)
		}
	}
}

func TestFSST_CascadeBeatsPlainOnRepetition(t *testing.T) {
	const n = 256
	v := vector.NewVarVec(vector.VecText, n, 0)
	vb := v.Var()
	for i := range n {
		vb.AppendString(i, fmt.Sprintf("https://example.com/page/%05d.html", i))
	}
	enc, payload, err := Encode(v, &EncodeContext{Scratch: NewScratchPool()})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if enc != schema.EncodingFSST {
		t.Fatalf("expected FSST winner, got %v (%d bytes)", enc, len(payload))
	}
}
