// Dictionary codec round-trip + boundary tests.
package codec

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestDictionary_EncodingIsDictionary(t *testing.T) {
	if (dictionaryCodec{}).Encoding() != schema.EncodingDictionary {
		t.Fatal("dictionaryCodec must claim Dictionary encoding")
	}
}

func TestDictionary_RoundTrip_ShortValues(t *testing.T) {
	rows := 100
	values := []string{"red", "green", "blue", "yellow"}
	src := vector.NewVarVec(vector.VecText, rows, 0)
	vb := src.Var()
	for i := range rows {
		vb.AppendString(i, values[i%len(values)])
	}
	payload, err := dictionaryCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var dst vector.Vec
	if err := (dictionaryCodec{}).Decode(payload, vector.VecText, rows, 0, &dst); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	dvb := dst.Var()
	for i := range rows {
		got := string(dvb.Bytes(i))
		want := values[i%len(values)]
		if got != want {
			t.Fatalf("row %d: got %q want %q", i, got, want)
		}
	}
}

func TestDictionary_RoundTrip_LongValues(t *testing.T) {
	rows := 50
	values := [][]byte{
		bytes.Repeat([]byte{'a'}, 32),
		bytes.Repeat([]byte{'b'}, 64),
		bytes.Repeat([]byte{'c'}, 100),
	}
	src := vector.NewVarVec(vector.VecBytes, rows, 0)
	vb := src.Var()
	for i := range rows {
		vb.AppendBytes(i, values[i%len(values)])
	}
	payload, err := dictionaryCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var dst vector.Vec
	if err := (dictionaryCodec{}).Decode(payload, vector.VecBytes, rows, 0, &dst); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	dvb := dst.Var()
	for i := range rows {
		if !bytes.Equal(dvb.Bytes(i), values[i%len(values)]) {
			t.Fatalf("row %d mismatch", i)
		}
	}
}

func TestDictionary_ExactBoundary_256Values(t *testing.T) {
	rows := 256
	src := vector.NewVarVec(vector.VecText, rows, 0)
	vb := src.Var()
	for i := range rows {
		vb.AppendString(i, fmt.Sprintf("v%d", i))
	}
	if _, ok := Estimate(dictionaryCodec{}, src); !ok {
		t.Fatal("Estimate must accept exactly 256 distinct values")
	}
	payload, err := dictionaryCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var dst vector.Vec
	if err := (dictionaryCodec{}).Decode(payload, vector.VecText, rows, 0, &dst); err != nil {
		t.Fatalf("Decode: %v", err)
	}
}

func TestDictionary_RejectsAbove256(t *testing.T) {
	rows := 257
	src := vector.NewVarVec(vector.VecText, rows, 0)
	vb := src.Var()
	for i := range rows {
		vb.AppendString(i, fmt.Sprintf("v%d", i))
	}
	if _, ok := Estimate(dictionaryCodec{}, src); ok {
		t.Fatal("Estimate must reject 257 distinct values")
	}
	if _, err := (dictionaryCodec{}).Encode(src, nil); err == nil {
		t.Fatal("Encode must reject 257 distinct values")
	}
}

func TestDictionary_EstimateRejectsNonVarBytes(t *testing.T) {
	for _, kind := range []vector.VecKind{vector.VecInt64, vector.VecBool, vector.VecUUID, vector.VecFloat32} {
		v := vector.NewVec(kind, 4)
		if _, ok := Estimate(dictionaryCodec{}, v); ok {
			t.Fatalf("Estimate must reject %v", kind)
		}
	}
}

func TestDictionary_DecodeRejects(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		kind    vector.VecKind
		rows    int
	}{
		{"truncatedHeader", []byte{1}, vector.VecText, 4},
		{"dictCountZero", []byte{0, 0, 0, 0, 0, 0}, vector.VecText, 4},
		{"negativeRows", nil, vector.VecText, -1},
		{"nonVarBytesKind", []byte{1, 0, 4, 0, 0, 0, 't', 'e', 's', 't', 0}, vector.VecInt64, 1},
	}
	for _, tc := range cases {
		var dst vector.Vec
		if err := (dictionaryCodec{}).Decode(tc.payload, tc.kind, tc.rows, 0, &dst); err == nil {
			t.Fatalf("%s: Decode must reject", tc.name)
		}
	}
}

func TestDictionary_EstimateMatchesEncode(t *testing.T) {
	rows := 50
	src := vector.NewVarVec(vector.VecText, rows, 0)
	vb := src.Var()
	for i := range rows {
		vb.AppendString(i, fmt.Sprintf("k%d", i%5))
	}
	est, ok := Estimate(dictionaryCodec{}, src)
	if !ok {
		t.Fatal("Estimate must accept low-cardinality varbytes")
	}
	payload, err := dictionaryCodec{}.Encode(src, nil)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(payload) != est {
		t.Fatalf("len(Encode)=%d != Estimate=%d", len(payload), est)
	}
}
