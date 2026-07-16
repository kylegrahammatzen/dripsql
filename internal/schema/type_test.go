// Type and VecKindOf tests: Parse normalization, mixed-state rejection, SQL-to-physical mapping.
// VecKindOf is the bridge SQL-side types use to provision the physical Vec.
package schema

import "testing"

func TestType_ParseRoundTrip(t *testing.T) {
	cases := []Type{Bool, Int16, Int32, Int64, Float32, Float64, Decimal, Text, Bytes, UUID, Timestamp, Time, Date, JSON}
	for _, ty := range cases {
		if got := Parse(ty.String()); got != ty {
			t.Errorf("Parse(%q)=%v want %v", ty.String(), got, ty)
		}
	}
}

func TestType_ParseUnknownReturnsNamed(t *testing.T) {
	got := Parse("currency")
	if got.Kind != KindNamed || got.Name != "currency" {
		t.Fatalf("Parse(unknown): got %v", got)
	}
}

func TestType_ParseNormalizesCaseAndWhitespace(t *testing.T) {
	cases := []struct {
		in   string
		want Type
	}{
		{"INT64", Int64},
		{" int64 ", Int64},
		{"\tText\n", Text},
		{" UUID ", UUID},
		{" MY_ENUM ", Named("my_enum")},
	}
	for _, c := range cases {
		got := Parse(c.in)
		if got != c.want {
			t.Errorf("Parse(%q)=%v want %v", c.in, got, c.want)
		}
	}
}

func TestType_RejectsMixedKindName(t *testing.T) {
	bad := Type{Kind: KindInt64, Name: "foo"}
	if bad.Valid() {
		t.Fatal("non-named Type with Name must be invalid")
	}
	if bad.String() != "int64" {
		t.Fatalf("non-named Type.String must fall back to Kind, got %q", bad.String())
	}
}

func TestType_ValidRejectsEmptyNamed(t *testing.T) {
	if (Type{Kind: KindNamed}).Valid() {
		t.Fatal("named type with empty name must be invalid")
	}
	if !Named("status").Valid() {
		t.Fatal("named type with non-empty name must be valid")
	}
	if (Type{}).Valid() {
		t.Fatal("zero Type must be invalid")
	}
}

