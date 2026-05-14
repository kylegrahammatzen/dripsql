package types

import "testing"

func TestTypeParseKnownNames(t *testing.T) {
	cases := []struct {
		name string
		want Type
	}{
		{"bool", Bool},
		{"int16", Int16},
		{"int32", Int32},
		{"int64", Int64},
		{"float32", Float32},
		{"float64", Float64},
		{"decimal", Decimal},
		{"text", Text},
		{"bytes", Bytes},
		{"uuid", UUID},
		{"timestamp", Timestamp},
		{"time", Time},
		{"date", Date},
		{"json", JSON},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := Parse(c.name)
			if got != c.want {
				t.Errorf("Parse(%q) = %v, want %v", c.name, got, c.want)
			}
		})
	}
}

func TestTypeParseUnknownReturnsNamed(t *testing.T) {
	cases := []string{"my_enum", "Status", "currency"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			got := Parse(name)
			if !got.IsNamed() {
				t.Fatalf("Parse(%q).IsNamed() = false, want true", name)
			}
			if got.Name != name {
				t.Errorf("Parse(%q).Name = %q, want %q", name, got.Name, name)
			}
		})
	}
}

func TestTypeStringRoundTrip(t *testing.T) {
	for _, ty := range []Type{Bool, Int16, Int32, Int64, Float32, Float64, Decimal, Text, Bytes, UUID, Timestamp, Time, Date, JSON} {
		got := Parse(ty.String())
		if got != ty {
			t.Errorf("Parse(%q) = %v, want %v", ty.String(), got, ty)
		}
	}
}

func TestNamedTypeMustHaveName(t *testing.T) {
	bad := Type{Kind: KindNamed}
	if bad.Valid() {
		t.Fatal("named type with empty name reports valid")
	}
	good := Named("status")
	if !good.Valid() {
		t.Fatal("named type with non-empty name reports invalid")
	}
}

func TestKindStringForAllKinds(t *testing.T) {
	want := map[Kind]string{
		KindInvalid:   "invalid",
		KindBool:      "bool",
		KindInt16:     "int16",
		KindInt32:     "int32",
		KindInt64:     "int64",
		KindFloat32:   "float32",
		KindFloat64:   "float64",
		KindDecimal:   "decimal",
		KindText:      "text",
		KindBytes:     "bytes",
		KindUUID:      "uuid",
		KindTimestamp: "timestamp",
		KindTime:      "time",
		KindDate:      "date",
		KindJSON:      "json",
		KindNamed:     "named",
	}
	for k, name := range want {
		if got := k.String(); got != name {
			t.Errorf("Kind(%d).String() = %q, want %q", k, got, name)
		}
	}
}

func TestEncodingStringForAllEncodings(t *testing.T) {
	want := map[Encoding]string{
		EncodingFlat:       "flat",
		EncodingDictionary: "dictionary",
		EncodingConstant:   "constant",
		EncodingSequence:   "sequence",
		EncodingFORBitPack: "for+bitpack",
		EncodingFlate:      "flate",
	}
	for e, name := range want {
		if got := e.String(); got != name {
			t.Errorf("Encoding(%d).String() = %q, want %q", e, got, name)
		}
	}
}

func TestVecKindOfReturnsCorrectVectorKind(t *testing.T) {
	cases := []struct {
		typ  Type
		want VecKind
	}{
		{Bool, VecBool},
		{Int16, VecInt16},
		{Int32, VecInt32},
		{Int64, VecInt64},
		{Float32, VecFloat32},
		{Float64, VecFloat64},
		{Decimal, VecDecimal64},
		{Text, VecText},
		{Bytes, VecBytes},
		{UUID, VecUUID},
		{Timestamp, VecTimestamp},
		{Time, VecTime},
		{Date, VecDate},
		{JSON, VecJSON},
		{Named("foo"), VecEnum32},
	}
	for _, c := range cases {
		t.Run(c.typ.String(), func(t *testing.T) {
			got, err := VecKindOf(c.typ)
			if err != nil {
				t.Fatalf("VecKindOf(%v): %v", c.typ, err)
			}
			if got != c.want {
				t.Errorf("VecKindOf(%v) = %v, want %v", c.typ, got, c.want)
			}
		})
	}
}

func TestVecKindOfRejectsInvalid(t *testing.T) {
	if _, err := VecKindOf(Type{}); err == nil {
		t.Fatal("VecKindOf(zero) returned nil error")
	}
	if _, err := VecKindOf(Type{Kind: KindNamed}); err == nil {
		t.Fatal("VecKindOf(named-without-name) returned nil error")
	}
}
