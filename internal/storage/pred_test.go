// Pred tests: invariant validation, schema binding, and end-to-end Skips/Apply through delegation.
// Delegation path goes away in the Phase 2 cleanup commit so these tests then prove the inlined logic.
package storage

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestPred_ValidateLeafInvariants(t *testing.T) {
	cases := []struct {
		name string
		p    Pred
		want bool
	}{
		{"eq_valid", Pred{Op: OpEq, Col: "x", Kind: types.VecInt64, I64: 7}, true},
		{"eq_missing_col", Pred{Op: OpEq, Kind: types.VecInt64, I64: 7}, false},
		{"isnull_valid", Pred{Op: OpIsNull, Col: "x", Kind: types.VecInt64}, true},
		{"in_empty_set", Pred{Op: OpIn, Col: "x", Set: nil}, false},
		{"not_missing_child", Pred{Op: OpNot}, false},
		{"and_one_child", Pred{Op: OpAnd, Children: []Pred{{Op: OpEq, Col: "x", Kind: types.VecInt64}}}, false},
		{"and_valid", Pred{Op: OpAnd, Children: []Pred{
			{Op: OpEq, Col: "x", Kind: types.VecInt64},
			{Op: OpEq, Col: "y", Kind: types.VecInt64},
		}}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validatePred(c.p)
			if c.want && err != nil {
				t.Fatalf("want valid, got %v", err)
			}
			if !c.want && err == nil {
				t.Fatalf("want invalid, got nil err")
			}
		})
	}
}

func TestPred_BindResolvesKind(t *testing.T) {
	lookup := func(name string) (types.VecKind, bool) {
		if name == "id" {
			return types.VecInt64, true
		}
		if name == "label" {
			return types.VecText, true
		}
		return 0, false
	}
	raw := Pred{Op: OpAnd, Children: []Pred{
		{Op: OpEq, Col: "id", I64: 42},
		{Op: OpLt, Col: "label", Bytes: []byte("z")},
	}}
	bound, err := BindPred(raw, lookup)
	if err != nil {
		t.Fatalf("BindPred: %v", err)
	}
	if bound.Children[0].Kind != types.VecInt64 {
		t.Fatalf("id Kind = %v, want VecInt64", bound.Children[0].Kind)
	}
	if bound.Children[1].Kind != types.VecText {
		t.Fatalf("label Kind = %v, want VecText", bound.Children[1].Kind)
	}
	if _, err := BindPred(Pred{Op: OpEq, Col: "missing", I64: 1}, lookup); err == nil {
		t.Fatal("BindPred(missing column) must error")
	}
}

func TestPred_BindIsNotMutating(t *testing.T) {
	raw := Pred{Op: OpEq, Col: "id", I64: 7}
	lookup := func(name string) (types.VecKind, bool) { return types.VecInt64, true }
	if _, err := BindPred(raw, lookup); err != nil {
		t.Fatal(err)
	}
	if raw.Kind != 0 {
		t.Fatal("BindPred must not mutate the raw Pred")
	}
}
