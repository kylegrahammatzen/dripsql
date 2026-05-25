// Bridge tests cover TypeFromVecKind and VecKindOf for every logical and physical kind.
package vector

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
)

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
