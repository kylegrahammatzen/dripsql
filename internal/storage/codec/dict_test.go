package codec

import (
	"fmt"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestDictionaryRoundTripText(t *testing.T) {
	v := textVec("signup", "checkout", "signup", "login")
	page, err := Dictionary{}.Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Dictionary{}.Decode(page)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Encoding != types.EncodingDictionary || len(got.DictIDs) != 4 || got.DictIDs[0] != got.DictIDs[2] || got.DictValues.String(int(got.DictIDs[1])) != "checkout" {
		t.Fatalf("vec = %#v", got)
	}
}

func TestDictionaryDecodeIntoReusesBuffers(t *testing.T) {
	first, err := Dictionary{}.Encode(textVec("signup", "checkout", "signup"))
	if err != nil {
		t.Fatalf("Encode first: %v", err)
	}
	second, err := Dictionary{}.Encode(textVec("login", "checkout", "login"))
	if err != nil {
		t.Fatalf("Encode second: %v", err)
	}
	var dst types.Vec
	if err := (Dictionary{}).DecodeInto(first, &dst); err != nil {
		t.Fatalf("DecodeInto first: %v", err)
	}
	idsPtr := &dst.DictIDs[0]
	offsetsPtr := &dst.DictValues.Offsets[0]
	dataPtr := &dst.DictValues.Data[0]
	if err := (Dictionary{}).DecodeInto(second, &dst); err != nil {
		t.Fatalf("DecodeInto second: %v", err)
	}
	if &dst.DictIDs[0] != idsPtr || &dst.DictValues.Offsets[0] != offsetsPtr || &dst.DictValues.Data[0] != dataPtr {
		t.Fatal("DecodeInto did not reuse dictionary buffers")
	}
	if dst.DictValues.String(int(dst.DictIDs[0])) != "login" {
		t.Fatalf("decoded dict = %#v", dst)
	}
}

func TestDictionaryDecodeIntoZeroAllocsAfterPrewarm(t *testing.T) {
	page := dictTextBenchPage(t)
	var dst types.Vec
	if err := (Dictionary{}).DecodeInto(page, &dst); err != nil {
		t.Fatalf("prewarm DecodeInto: %v", err)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		if err := (Dictionary{}).DecodeInto(page, &dst); err != nil {
			panic(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("DecodeInto allocs/run = %v, want 0", allocs)
	}
}

func TestDictionaryRoundTripWithNull(t *testing.T) {
	v := textVec("signup", "checkout", "login")
	v.Valid = types.NewValidity(3)
	types.SetInvalid(v.Valid, 1)
	page, err := Dictionary{}.Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Dictionary{}.Decode(page)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if types.IsValid(got.Valid, 1) || got.DictValues.String(int(got.DictIDs[0])) != "signup" {
		t.Fatalf("vec = %#v", got)
	}
}

func TestDictionaryRoundTripOneDistinct(t *testing.T) {
	v := textVec("checkout", "checkout", "checkout")
	page, err := Dictionary{}.Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Dictionary{}.Decode(page)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.DictValues.Rows() != 1 || got.DictValues.String(0) != "checkout" {
		t.Fatalf("dict values = %#v", got.DictValues)
	}
	for row, id := range got.DictIDs {
		if id != 0 {
			t.Fatalf("DictIDs[%d] = %d, want 0", row, id)
		}
	}
}

func TestDictionaryRoundTripFourDistinct(t *testing.T) {
	v := textVec("a", "b", "c", "d", "a")
	page, err := Dictionary{}.Encode(v)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Dictionary{}.Decode(page)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.DictValues.Rows() != 4 || got.DictIDs[0] != got.DictIDs[4] {
		t.Fatalf("vec = %#v", got)
	}
	if got.DictValues.String(int(got.DictIDs[3])) != "d" {
		t.Fatalf("dict value for row 3 = %q", got.DictValues.String(int(got.DictIDs[3])))
	}
}

func TestDictionaryRoundTripMaxDistinct(t *testing.T) {
	values := make([]string, DictMaxValues)
	for i := range values {
		values[i] = fmt.Sprintf("v%03d", i)
	}
	page, err := Dictionary{}.Encode(textVec(values...))
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := Dictionary{}.Decode(page)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.DictValues.Rows() != DictMaxValues {
		t.Fatalf("dict rows = %d, want %d", got.DictValues.Rows(), DictMaxValues)
	}
	if got.DictIDs[0] != 0 || got.DictIDs[DictMaxValues-1] != uint8(DictMaxValues-1) {
		t.Fatalf("dict ids first/last = %d/%d", got.DictIDs[0], got.DictIDs[DictMaxValues-1])
	}
	if got.DictValues.String(DictMaxValues-1) != "v255" {
		t.Fatalf("last dict value = %q", got.DictValues.String(DictMaxValues-1))
	}
}

func TestDictionaryEstimateSmallerThanPlainForRepeats(t *testing.T) {
	v := textVec("checkout", "checkout", "checkout", "checkout")
	dictSize, ok := Dictionary{}.Estimate(v)
	if !ok {
		t.Fatal("dict estimate !ok")
	}
	plainSize, ok := Plain{}.Estimate(v)
	if !ok {
		t.Fatal("plain estimate !ok")
	}
	if dictSize >= plainSize {
		t.Fatalf("dict size = %d, plain size = %d", dictSize, plainSize)
	}
}

func TestDictionaryRejectsTooManyDistinct(t *testing.T) {
	values := make([]string, DictMaxValues+1)
	for i := range values {
		values[i] = fmt.Sprintf("v%d", i)
	}
	_, err := Dictionary{}.Encode(textVec(values...))
	if err == nil {
		t.Fatal("expected dictionary overflow")
	}
}

func TestDictionaryRejectsAllNullPage(t *testing.T) {
	v := textVec("signup", "checkout")
	v.Valid = types.NewValidity(2)
	types.SetInvalid(v.Valid, 0)
	types.SetInvalid(v.Valid, 1)
	_, err := Dictionary{}.Encode(v)
	if err == nil {
		t.Fatal("expected all-null dictionary error")
	}
}

func TestDictionaryDecodeTruncated(t *testing.T) {
	_, err := Dictionary{}.Decode(Page{Kind: types.VecText, Encoding: types.EncodingDictionary, Rows: 1, Payload: []byte{1}})
	if err == nil {
		t.Fatal("expected truncated dictionary error")
	}
}

func TestPickSmallestChoosesDictionary(t *testing.T) {
	best, ok := PickSmallest(textVec("x", "x", "x", "x"), Plain{}, Dictionary{})
	if !ok || best.Encoding() != types.EncodingDictionary {
		t.Fatalf("best = %#v ok=%v", best, ok)
	}
}

func TestPickSmallestFallsBackToPlainWhenDictionaryInvalid(t *testing.T) {
	v := textVec("x", "y")
	v.Valid = types.NewValidity(2)
	types.SetInvalid(v.Valid, 0)
	types.SetInvalid(v.Valid, 1)
	best, ok := PickSmallest(v, Plain{}, Dictionary{})
	if !ok || best.Encoding() != types.EncodingFlat {
		t.Fatalf("best = %#v ok=%v", best, ok)
	}
}

func TestRegisterDuplicateCodec(t *testing.T) {
	if err := Register(Constant{}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := Register(Constant{}); err == nil {
		t.Fatal("expected duplicate registration error")
	}
	if _, ok := Lookup(types.EncodingConstant); !ok {
		t.Fatal("registered codec missing")
	}
}
