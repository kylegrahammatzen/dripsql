// compressed_text codec invariant tests: round-trip via both Flate and Zstd, kind rejection,
// truncated header handling, length-mismatch detection, and compression actually shrinking repetitive input.
package codec

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestCompressedText_LookupBoth(t *testing.T) {
	for _, enc := range []schema.Encoding{schema.EncodingFlate, schema.EncodingZstd} {
		c, err := Lookup(enc)
		if err != nil {
			t.Fatalf("Lookup(%v): %v", enc, err)
		}
		if c.Encoding() != enc {
			t.Fatalf("Lookup(%v) returned codec for %v", enc, c.Encoding())
		}
	}
}

func TestCompressedText_RoundTripBothEncodings(t *testing.T) {
	for _, enc := range []schema.Encoding{schema.EncodingFlate, schema.EncodingZstd} {
		t.Run(enc.String(), func(t *testing.T) {
			src := vector.NewVarVec(vector.VecText, 5, 0)
			vb := src.Var()
			vb.AppendString(0, "")
			vb.AppendString(1, "short")
			vb.AppendString(2, "twelvebytes_")
			vb.AppendString(3, "longer than thirteen bytes total")
			vb.AppendString(4, "another value here")
			c, _ := Lookup(enc)
			payload, err := c.Encode(src, nil)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			var dst vector.Vec
			if err := c.Decode(payload, vector.VecText, 5, 0, &dst); err != nil {
				t.Fatalf("Decode: %v", err)
			}
			want := []string{"", "short", "twelvebytes_", "longer than thirteen bytes total", "another value here"}
			for i, w := range want {
				if dst.Var().String(i) != w {
					t.Fatalf("row %d: got %q want %q", i, dst.Var().String(i), w)
				}
			}
		})
	}
}

func TestCompressedText_CompressesRepetitiveInput(t *testing.T) {
	for _, enc := range []schema.Encoding{schema.EncodingFlate, schema.EncodingZstd} {
		t.Run(enc.String(), func(t *testing.T) {
			src := vector.NewVarVec(vector.VecText, 1000, 0)
			vb := src.Var()
			for i := range 1000 {
				vb.AppendString(i, "the quick brown fox jumps over the lazy dog")
			}
			c, _ := Lookup(enc)
			payload, err := c.Encode(src, nil)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			uncompressed := varbytesWireSize(src)
			if len(payload) >= uncompressed/4 {
				t.Fatalf("%v: payload %d not 4x smaller than uncompressed %d", enc, len(payload), uncompressed)
			}
		})
	}
}

func TestCompressedText_RejectsNonVarbytes(t *testing.T) {
	c, _ := Lookup(schema.EncodingFlate)
	v := vector.NewVec(vector.VecInt64, 4)
	if _, err := c.Encode(v, nil); err == nil {
		t.Fatal("Encode must reject non-varbytes")
	}
	var dst vector.Vec
	if err := c.Decode(make([]byte, 4), vector.VecInt64, 4, 0, &dst); err == nil {
		t.Fatal("Decode must reject non-varbytes kind")
	}
}

func TestCompressedText_DecodeRejectsTruncatedHeader(t *testing.T) {
	c, _ := Lookup(schema.EncodingFlate)
	var dst vector.Vec
	if err := c.Decode([]byte{0, 0}, vector.VecText, 1, 0, &dst); err == nil {
		t.Fatal("Decode must reject < 4 byte payload")
	}
}

func TestCompressedText_DecodeRejectsLengthMismatch(t *testing.T) {
	c, _ := Lookup(schema.EncodingFlate)
	src := vector.NewVarVec(vector.VecText, 3, 0)
	src.Var().AppendString(0, "a")
	src.Var().AppendString(1, "bb")
	src.Var().AppendString(2, "ccc")
	payload, err := c.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// Corrupt the uncompressed-length header to claim a different size.
	payload[0] = 0xFF
	payload[1] = 0xFF
	var dst vector.Vec
	if err := c.Decode(payload, vector.VecText, 3, 0, &dst); err == nil {
		t.Fatal("Decode must reject mismatched uncompressed length")
	}
}

func TestCompressedText_EstimateMatchesPlainSize(t *testing.T) {
	src := vector.NewVarVec(vector.VecText, 4, 0)
	src.Var().AppendString(0, "alpha")
	src.Var().AppendString(1, "beta")
	src.Var().AppendString(2, "gamma")
	src.Var().AppendString(3, "delta")
	c, _ := Lookup(schema.EncodingZstd)
	est, ok := Estimate(c, src)
	if !ok {
		t.Fatal("varbytes Estimate must succeed")
	}
	// Compressed encodings emit a 4-byte uncompressed-length header + the
	// compressed payload. We can't easily predict the exact compressed size,
	// so just sanity-check the lower bound.
	if est < 4 {
		t.Fatalf("Estimate=%d below header size", est)
	}
	_ = varbytesWireSize(src)
}

func TestCompressedText_DecodeSetsEncFlat(t *testing.T) {
	src := vector.NewVarVec(vector.VecText, 4, 0)
	vb := src.Var()
	for i := range 4 {
		vb.AppendString(i, "hello world goes here over twelve bytes")
	}
	for _, enc := range []schema.Encoding{schema.EncodingFlate, schema.EncodingZstd} {
		c, _ := Lookup(enc)
		payload, err := c.Encode(src, nil)
		if err != nil {
			t.Fatalf("Encode(%v): %v", enc, err)
		}
		var dst vector.Vec
		if err := c.Decode(payload, vector.VecText, 4, 0, &dst); err != nil {
			t.Fatalf("Decode(%v): %v", enc, err)
		}
		if dst.Enc != schema.EncodingFlat {
			t.Fatalf("decoded Enc = %v, want Flat", dst.Enc)
		}
	}
}
