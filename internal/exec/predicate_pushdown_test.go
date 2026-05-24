// loweredPredicate maps the binary comparison ops to storage predicates with reversed-operand
// handling and overflow guards on the inclusive variants.
package exec

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

func cmp(op sql.ExprOp, col string, lit int64) sql.BoundExpr {
	return sql.BoundExpr{
		Op: op,
		Args: []sql.BoundExpr{
			{Op: sql.ExprColumn, Column: col},
			{Op: sql.ExprLiteral, Literal: lit},
		},
	}
}

func TestLoweredPredicate_IntComparisons(t *testing.T) {
	cases := []struct {
		name string
		op   sql.ExprOp
		want storage.Predicate
	}{
		{"eq", sql.ExprEqual, storage.EqInt64{Column: "x", Value: 10}},
		{"ne", sql.ExprNotEqual, storage.Not{Child: storage.EqInt64{Column: "x", Value: 10}}},
		{"lt", sql.ExprLess, storage.LtInt64{Column: "x", Value: 10}},
		{"gt", sql.ExprGreater, storage.GtInt64{Column: "x", Value: 10}},
		{"le", sql.ExprLessEqual, storage.LtInt64{Column: "x", Value: 11}},
		{"ge", sql.ExprGreaterEqual, storage.GtInt64{Column: "x", Value: 9}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := loweredPredicate(cmp(tc.op, "x", 10))
			if !ok {
				t.Fatalf("expected lowering for %v", tc.op)
			}
			if got != tc.want {
				t.Errorf("got %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestLoweredPredicate_OverflowGuards(t *testing.T) {
	const maxI64 = int64(^uint64(0) >> 1)
	const minI64 = -maxI64 - 1
	if _, ok := loweredPredicate(cmp(sql.ExprLessEqual, "x", maxI64)); ok {
		t.Errorf("x <= MaxInt64 must not lower (avoids overflow on +1)")
	}
	if _, ok := loweredPredicate(cmp(sql.ExprGreaterEqual, "x", minI64)); ok {
		t.Errorf("x >= MinInt64 must not lower (avoids underflow on -1)")
	}
}

func TestLoweredPredicate_Between(t *testing.T) {
	expr := sql.BoundExpr{
		Op: sql.ExprBetween,
		Args: []sql.BoundExpr{
			{Op: sql.ExprColumn, Column: "x"},
			{Op: sql.ExprLiteral, Literal: int64(10)},
			{Op: sql.ExprLiteral, Literal: int64(20)},
		},
	}
	got, ok := loweredPredicate(expr)
	if !ok {
		t.Fatal("expected lowering for BETWEEN")
	}
	// x BETWEEN 10 AND 20 -> And{x >= 10, x <= 20} -> And{Gt{9}, Lt{21}}
	want := storage.And{Children: []storage.Predicate{
		storage.GtInt64{Column: "x", Value: 9},
		storage.LtInt64{Column: "x", Value: 21},
	}}
	if a, ok := got.(storage.And); !ok || len(a.Children) != 2 || a.Children[0] != want.Children[0] || a.Children[1] != want.Children[1] {
		t.Errorf("got %#v, want %#v", got, want)
	}
}

func TestLoweredPredicate_In(t *testing.T) {
	expr := sql.BoundExpr{
		Op: sql.ExprIn,
		Args: []sql.BoundExpr{
			{Op: sql.ExprColumn, Column: "id"},
			{Op: sql.ExprLiteral, Literal: int64(1)},
			{Op: sql.ExprLiteral, Literal: int64(2)},
			{Op: sql.ExprLiteral, Literal: int64(3)},
		},
	}
	got, ok := loweredPredicate(expr)
	if !ok {
		t.Fatal("expected lowering for IN")
	}
	or, ok := got.(storage.Or)
	if !ok || len(or.Children) != 3 {
		t.Fatalf("got %#v, want Or with 3 children", got)
	}
	for i, want := range []int64{1, 2, 3} {
		eq, ok := or.Children[i].(storage.EqInt64)
		if !ok || eq.Value != want || eq.Column != "id" {
			t.Errorf("child %d = %#v, want EqInt64{id, %d}", i, or.Children[i], want)
		}
	}
}

func TestLoweredPredicate_NotIn(t *testing.T) {
	expr := sql.BoundExpr{
		Op:  sql.ExprIn,
		Not: true,
		Args: []sql.BoundExpr{
			{Op: sql.ExprColumn, Column: "id"},
			{Op: sql.ExprLiteral, Literal: int64(1)},
			{Op: sql.ExprLiteral, Literal: int64(2)},
		},
	}
	got, ok := loweredPredicate(expr)
	if !ok {
		t.Fatal("expected lowering for NOT IN")
	}
	not, ok := got.(storage.Not)
	if !ok {
		t.Fatalf("got %#v, want Not wrapper", got)
	}
	if _, ok := not.Child.(storage.Or); !ok {
		t.Errorf("Not.Child = %#v, want Or", not.Child)
	}
}

func TestLoweredPredicate_InSingleton(t *testing.T) {
	// IN with one literal collapses straight to Eq without an Or wrapper.
	expr := sql.BoundExpr{
		Op: sql.ExprIn,
		Args: []sql.BoundExpr{
			{Op: sql.ExprColumn, Column: "id"},
			{Op: sql.ExprLiteral, Literal: int64(42)},
		},
	}
	got, ok := loweredPredicate(expr)
	if !ok {
		t.Fatal("expected lowering for singleton IN")
	}
	want := storage.EqInt64{Column: "id", Value: 42}
	if got != want {
		t.Errorf("got %#v, want %#v", got, want)
	}
}

func TestLoweredPredicate_ReversedOperands(t *testing.T) {
	// 10 <= x should be rewritten as x >= 10, then lowered to GtInt64{value=9}.
	expr := sql.BoundExpr{
		Op: sql.ExprLessEqual,
		Args: []sql.BoundExpr{
			{Op: sql.ExprLiteral, Literal: int64(10)},
			{Op: sql.ExprColumn, Column: "x"},
		},
	}
	got, ok := loweredPredicate(expr)
	if !ok {
		t.Fatal("expected lowering for reversed operands")
	}
	want := storage.GtInt64{Column: "x", Value: 9}
	if got != want {
		t.Errorf("got %#v, want %#v", got, want)
	}
}
