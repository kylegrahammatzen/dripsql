// VecKind invariant tests: every kind's metadata-table row must agree with the
// per-kind FixedWidth, IsVarBytes, IsFORPackable, and String results.
package types

import "testing"

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
