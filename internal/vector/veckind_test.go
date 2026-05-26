// VecKind invariant tests cover the per-kind metadata table and the schema.Type round-trip.
package vector

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
)

func TestVecKind_InfoTable(t *testing.T) {
	cases := []struct {
		k       VecKind
		width   Width
		varB    bool
		forPack bool
		name    string
	}{
		{VecBool, WidthBool, false, false, "bool"},
		{VecInt16, 2, false, true, "int16"},
		{VecInt32, 4, false, true, "int32"},
		{VecInt64, 8, false, true, "int64"},
		{VecFloat32, 4, false, false, "float32"},
		{VecFloat64, 8, false, false, "float64"},
		{VecDecimal64, 8, false, true, "decimal64"},
		{VecText, WidthVarBytes, true, false, "text"},
		{VecBytes, WidthVarBytes, true, false, "bytes"},
		{VecJSON, WidthVarBytes, true, false, "json"},
		{VecUUID, 16, false, false, "uuid"},
		{VecTimestamp, 8, false, true, "timestamp"},
		{VecTime, 8, false, true, "time"},
		{VecDate, 4, false, true, "date"},
		{VecEnum32, 4, false, true, "enum32"},
	}
	for _, c := range cases {
		if got := c.k.FixedWidth(); got != c.width {
			t.Errorf("%v FixedWidth=%d want %d", c.k, got, c.width)
		}
		if got := c.k.IsVarBytes(); got != c.varB {
			t.Errorf("%v IsVarBytes=%v want %v", c.k, got, c.varB)
		}
		if got := c.k.IsFORPackable(); got != c.forPack {
			t.Errorf("%v IsFORPackable=%v want %v", c.k, got, c.forPack)
		}
		if got := c.k.String(); got != c.name {
			t.Errorf("%v String=%q want %q", c.k, got, c.name)
		}
	}
}

func TestVecKindOf_Mapping(t *testing.T) {
	cases := []struct {
		typ  schema.Type
		want VecKind
	}{
		{schema.Bool, VecBool},
		{schema.Int16, VecInt16},
		{schema.Int32, VecInt32},
		{schema.Int64, VecInt64},
		{schema.Float32, VecFloat32},
		{schema.Float64, VecFloat64},
		{schema.Decimal, VecDecimal64},
		{schema.Text, VecText},
		{schema.Bytes, VecBytes},
		{schema.UUID, VecUUID},
		{schema.Timestamp, VecTimestamp},
		{schema.Time, VecTime},
		{schema.Date, VecDate},
		{schema.JSON, VecJSON},
		{schema.Named("status"), VecEnum32},
	}
	for _, c := range cases {
		got, err := VecKindOf(c.typ)
		if err != nil {
			t.Errorf("VecKindOf(%v): %v", c.typ, err)
			continue
		}
		if got != c.want {
			t.Errorf("VecKindOf(%v)=%v want %v", c.typ, got, c.want)
		}
	}
	if _, err := VecKindOf(schema.Type{}); err == nil {
		t.Fatal("VecKindOf(zero) must error")
	}
	if _, err := VecKindOf(schema.Type{Kind: schema.KindNamed}); err == nil {
		t.Fatal("VecKindOf(named-empty) must error")
	}
}

func TestVecKindOf_RejectsMixedKindName(t *testing.T) {
	bad := schema.Type{Kind: schema.KindInt64, Name: "foo"}
	if _, err := VecKindOf(bad); err == nil {
		t.Fatal("VecKindOf must reject mixed Kind/Name")
	}
}
