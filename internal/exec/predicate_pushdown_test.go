// loweredPredicate maps binary comparison ops to storage.Pred with reversed-operand handling and overflow guards.
package exec

import (
	"bytes"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
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

func intLeaf(op storage.PredOp, col string, v int64) storage.Pred {
	return storage.Pred{Op: op, Col: col, Kind: vector.VecInt64, I64: v}
}

func predEq(a, b storage.Pred) bool {
	if a.Op != b.Op || a.Col != b.Col || a.Kind != b.Kind || a.I64 != b.I64 || a.F64 != b.F64 {
		return false
	}
	if !bytes.Equal(a.Bytes, b.Bytes) {
		return false
	}
	if len(a.Children) != len(b.Children) || len(a.Set) != len(b.Set) {
		return false
	}
	for i := range a.Children {
		if !predEq(a.Children[i], b.Children[i]) {
			return false
		}
	}
	for i := range a.Set {
		if !predEq(a.Set[i], b.Set[i]) {
			return false
		}
	}
	return true
}

func TestLoweredPredicate_IntComparisons(t *testing.T) {
	cases := []struct {
		name string
		op   sql.ExprOp
		want storage.Pred
	}{
		{"eq", sql.ExprEqual, intLeaf(storage.OpEq, "x", 10)},
		{"ne", sql.ExprNotEqual, storage.Pred{Op: storage.OpNot, Children: []storage.Pred{intLeaf(storage.OpEq, "x", 10)}}},
		{"lt", sql.ExprLess, intLeaf(storage.OpLt, "x", 10)},
		{"gt", sql.ExprGreater, intLeaf(storage.OpGt, "x", 10)},
		{"le", sql.ExprLessEqual, intLeaf(storage.OpLt, "x", 11)},
		{"ge", sql.ExprGreaterEqual, intLeaf(storage.OpGt, "x", 9)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := loweredComparison(cmp(tc.op, "x", 10))
			if !ok {
				t.Fatalf("expected lowering for %v", tc.op)
			}
			if !predEq(got, tc.want) {
				t.Errorf("got %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestLoweredPredicate_OverflowGuards(t *testing.T) {
	const maxI64 = int64(^uint64(0) >> 1)
	const minI64 = -maxI64 - 1
	if _, ok := loweredComparison(cmp(sql.ExprLessEqual, "x", maxI64)); ok {
		t.Errorf("x <= MaxInt64 must not lower because +1 overflows")
	}
	if _, ok := loweredComparison(cmp(sql.ExprGreaterEqual, "x", minI64)); ok {
		t.Errorf("x >= MinInt64 must not lower because -1 underflows")
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
	got, ok := loweredComparison(expr)
	if !ok {
		t.Fatal("expected lowering for BETWEEN")
	}
	want := storage.Pred{Op: storage.OpAnd, Children: []storage.Pred{
		intLeaf(storage.OpGt, "x", 9),
		intLeaf(storage.OpLt, "x", 21),
	}}
	if !predEq(got, want) {
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
	got, ok := loweredComparison(expr)
	if !ok {
		t.Fatal("expected lowering for IN")
	}
	if got.Op != storage.OpOr || len(got.Children) != 3 {
		t.Fatalf("got %#v, want Or with 3 children", got)
	}
	for i, want := range []int64{1, 2, 3} {
		child := got.Children[i]
		if child.Op != storage.OpEq || child.Col != "id" || child.I64 != want {
			t.Errorf("child %d = %#v, want OpEq id=%d", i, child, want)
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
	got, ok := loweredComparison(expr)
	if !ok {
		t.Fatal("expected lowering for NOT IN")
	}
	if got.Op != storage.OpNot || len(got.Children) != 1 {
		t.Fatalf("got %#v, want OpNot wrapper", got)
	}
	if got.Children[0].Op != storage.OpOr {
		t.Errorf("Not child = %#v, want OpOr", got.Children[0])
	}
}

func TestLoweredPredicate_InSingleton(t *testing.T) {
	expr := sql.BoundExpr{
		Op: sql.ExprIn,
		Args: []sql.BoundExpr{
			{Op: sql.ExprColumn, Column: "id"},
			{Op: sql.ExprLiteral, Literal: int64(42)},
		},
	}
	got, ok := loweredComparison(expr)
	if !ok {
		t.Fatal("expected lowering for singleton IN")
	}
	want := intLeaf(storage.OpEq, "id", 42)
	if !predEq(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}
}

// Mixed-WHERE contract pushes the lowerable conjunct and keeps the rest as residual instead of falling back to a full FilterOp.
func TestSplitWhere_MixedConjuncts(t *testing.T) {
	unlowerable := sql.BoundExpr{Op: sql.ExprAdd, Args: []sql.BoundExpr{
		{Op: sql.ExprColumn, Column: "y"},
		{Op: sql.ExprLiteral, Literal: int64(1)},
	}}
	expr := sql.BoundExpr{Op: sql.ExprAnd, Args: []sql.BoundExpr{
		cmp(sql.ExprEqual, "x", 7),
		unlowerable,
	}}
	push, res := splitWhere(expr)
	if push == nil {
		t.Fatal("expected push pred for x=7")
	}
	if !predEq(*push, intLeaf(storage.OpEq, "x", 7)) {
		t.Errorf("push got %#v", *push)
	}
	if res == nil || res.Op != sql.ExprAdd {
		t.Fatalf("residual got %#v", res)
	}
}

// All-pushable WHERE must leave residual nil so the PredicateOnly column-narrowing path stays active.
func TestSplitWhere_AllPushed(t *testing.T) {
	expr := sql.BoundExpr{Op: sql.ExprAnd, Args: []sql.BoundExpr{
		cmp(sql.ExprEqual, "x", 1),
		cmp(sql.ExprGreater, "x", 0),
	}}
	push, res := splitWhere(expr)
	if push == nil || push.Op != storage.OpAnd || len(push.Children) != 2 {
		t.Fatalf("push got %#v", push)
	}
	if res != nil {
		t.Fatalf("expected nil residual, got %#v", *res)
	}
}

// No pushable conjunct means residual carries the whole expression so FilterOp runs it unchanged.
func TestSplitWhere_NothingPushed(t *testing.T) {
	expr := sql.BoundExpr{Op: sql.ExprAdd, Args: []sql.BoundExpr{
		{Op: sql.ExprColumn, Column: "y"},
		{Op: sql.ExprLiteral, Literal: int64(1)},
	}}
	push, res := splitWhere(expr)
	if push != nil {
		t.Fatalf("expected nil push, got %#v", *push)
	}
	if res == nil || res.Op != sql.ExprAdd {
		t.Fatalf("residual got %#v", res)
	}
}

func TestLoweredPredicate_ReversedOperands(t *testing.T) {
	expr := sql.BoundExpr{
		Op: sql.ExprLessEqual,
		Args: []sql.BoundExpr{
			{Op: sql.ExprLiteral, Literal: int64(10)},
			{Op: sql.ExprColumn, Column: "x"},
		},
	}
	got, ok := loweredComparison(expr)
	if !ok {
		t.Fatal("expected lowering for reversed operands")
	}
	want := intLeaf(storage.OpGt, "x", 9)
	if !predEq(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}
}