package exec

import (
	"context"
	"testing"

	v3sql "github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestProjectBoundOutputsLiteralsAndArithmetic(t *testing.T) {
	valid := types.NewValidity(3)
	types.SetInvalid(valid, 2)
	batch := execInt64Batch(t, []int64{10, 20, 99}, valid)
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	downstream := &collectConsumer{}
	project := &Project{
		Exprs: []v3sql.BoundOutput{
			{Expr: boundColumn("amount", types.Int64)},
			{Alias: "label", Expr: boundLiteral(types.Text, "active")},
			{Alias: "ok", Expr: boundLiteral(types.Bool, true)},
			{Alias: "missing", Expr: boundLiteral(types.Type{}, nil)},
			{Alias: "next_amount", Expr: boundBinary(types.Int64, v3sql.BoundOpAdd, boundColumn("amount", types.Int64), boundLiteral(types.Int64, int64(1)))},
		},
		Downstream: downstream,
	}
	if err := project.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := project.Push(batch, sel); err != nil {
		t.Fatalf("Push: %v", err)
	}
	got := singleProjectedBatch(t, downstream)
	assertInt64Column(t, got, "amount", []int64{10, 20, 99})
	assertTextColumn(t, got, "label", []string{"active", "active", "active"})
	assertBoolColumn(t, got, "ok", []bool{true, true, true})
	assertInvalidRows(t, got, "missing", []int{0, 1, 2})
	assertInt64Column(t, got, "next_amount", []int64{11, 21, 0})
	assertInvalidRows(t, got, "next_amount", []int{2})
}

func TestProjectBoundOutputsTextExpressions(t *testing.T) {
	batch := execSortBatch(t, []int64{1, 2}, []string{"Signup", "CHECKOUT"}, nil)
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	downstream := &collectConsumer{}
	col := boundColumn("event_type", types.Text)
	project := &Project{
		Exprs: []v3sql.BoundOutput{
			{Alias: "kind", Expr: boundUnary(types.Text, v3sql.BoundOpLower, col)},
			{Alias: "loud", Expr: boundUnary(types.Text, v3sql.BoundOpUpper, col)},
			{Alias: "label", Expr: boundBinary(types.Text, v3sql.BoundOpConcat, col, boundLiteral(types.Text, ":done"))},
		},
		Downstream: downstream,
	}
	if err := project.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := project.Push(batch, sel); err != nil {
		t.Fatalf("Push: %v", err)
	}
	got := singleProjectedBatch(t, downstream)
	assertTextColumn(t, got, "kind", []string{"signup", "checkout"})
	assertTextColumn(t, got, "loud", []string{"SIGNUP", "CHECKOUT"})
	assertTextColumn(t, got, "label", []string{"Signup:done", "CHECKOUT:done"})
}

func TestProjectBoundOutputsEvaluatesSelectedRowsOnly(t *testing.T) {
	batch := execInt64Batch(t, []int64{10, 0}, nil)
	sel := types.NewSelectionMask(batch.Len)
	sel.Set(0)
	downstream := &collectConsumer{}
	project := &Project{
		Exprs: []v3sql.BoundOutput{
			{Alias: "quotient", Expr: boundBinary(types.Int64, v3sql.BoundOpDivide, boundLiteral(types.Int64, int64(100)), boundColumn("amount", types.Int64))},
		},
		Downstream: downstream,
	}
	if err := project.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := project.Push(batch, sel); err != nil {
		t.Fatalf("Push: %v", err)
	}
	got := singleProjectedBatch(t, downstream)
	assertInt64Column(t, got, "quotient", []int64{10, 0})
	assertInvalidRows(t, got, "quotient", []int{1})
}

func TestProjectBoundOutputsIntArithmeticFastPathOps(t *testing.T) {
	batch := execInt64Batch(t, []int64{10, 21}, nil)
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	downstream := &collectConsumer{}
	col := boundColumn("amount", types.Int64)
	project := &Project{
		Exprs: []v3sql.BoundOutput{
			{Alias: "double", Expr: boundBinary(types.Int64, v3sql.BoundOpMultiply, col, boundLiteral(types.Int64, int64(2)))},
			{Alias: "remainder", Expr: boundBinary(types.Int64, v3sql.BoundOpModulo, col, boundLiteral(types.Int64, int64(4)))},
			{Alias: "from_hundred", Expr: boundBinary(types.Int64, v3sql.BoundOpSubtract, boundLiteral(types.Int64, int64(100)), col)},
		},
		Downstream: downstream,
	}
	if err := project.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := project.Push(batch, sel); err != nil {
		t.Fatalf("Push: %v", err)
	}
	got := singleProjectedBatch(t, downstream)
	assertInt64Column(t, got, "double", []int64{20, 42})
	assertInt64Column(t, got, "remainder", []int64{2, 1})
	assertInt64Column(t, got, "from_hundred", []int64{90, 79})
}

func singleProjectedBatch(t *testing.T, downstream *collectConsumer) types.Batch {
	t.Helper()
	if len(downstream.batches) != 1 {
		t.Fatalf("downstream batches = %d, want 1", len(downstream.batches))
	}
	return downstream.batches[0]
}

func boundColumn(name string, typ types.Type) v3sql.BoundExpr {
	return v3sql.BoundExpr{Kind: v3sql.BoundExprColumn, Type: typ, Column: name}
}

func boundLiteral(typ types.Type, value any) v3sql.BoundExpr {
	return v3sql.BoundExpr{Kind: v3sql.BoundExprLiteral, Type: typ, Literal: value}
}

func boundBinary(typ types.Type, op v3sql.BoundOp, left v3sql.BoundExpr, right v3sql.BoundExpr) v3sql.BoundExpr {
	return v3sql.BoundExpr{Kind: v3sql.BoundExprBinary, Type: typ, Op: op, Left: &left, Right: &right}
}

func boundUnary(typ types.Type, op v3sql.BoundOp, child v3sql.BoundExpr) v3sql.BoundExpr {
	return v3sql.BoundExpr{Kind: v3sql.BoundExprUnary, Type: typ, Op: op, Left: &child}
}

func assertBoolColumn(t *testing.T, batch types.Batch, name string, want []bool) {
	t.Helper()
	col, ok := columnByName(batch, name)
	if !ok {
		t.Fatalf("missing column %q", name)
	}
	if col.V.Len != len(want) {
		t.Fatalf("%s len = %d, want %d", name, col.V.Len, len(want))
	}
	for row, value := range want {
		got := col.V.BoolBits[row>>6]&(uint64(1)<<uint(row&63)) != 0
		if got != value {
			t.Fatalf("%s[%d] = %v, want %v", name, row, got, value)
		}
	}
}

func assertInvalidRows(t *testing.T, batch types.Batch, name string, rows []int) {
	t.Helper()
	col, ok := columnByName(batch, name)
	if !ok {
		t.Fatalf("missing column %q", name)
	}
	for _, row := range rows {
		if types.IsValid(col.V.Valid, row) {
			t.Fatalf("%s[%d] is valid, want invalid", name, row)
		}
	}
}
