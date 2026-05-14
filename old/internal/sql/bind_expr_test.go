package sql

import (
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestBindExprColumnAndLiteral(t *testing.T) {
	expr, err := BindExpr(exprColumns(), &ColumnRef{Name: "tenant_id"})
	if err != nil {
		t.Fatalf("BindExpr column: %v", err)
	}
	if expr.Kind != BoundExprColumn || expr.Column != "tenant_id" || expr.ColumnID != 1 || expr.Type != types.Int64 {
		t.Fatalf("expr = %#v", expr)
	}

	lit, err := BindExpr(exprColumns(), &Literal{Value: Value{Kind: ValueString, String: "signup"}})
	if err != nil {
		t.Fatalf("BindExpr literal: %v", err)
	}
	if lit.Kind != BoundExprLiteral || lit.Type != types.Text || lit.Literal != "signup" {
		t.Fatalf("literal = %#v", lit)
	}
}

func TestBindExprArithmetic(t *testing.T) {
	expr, err := BindExpr(exprColumns(), &BinaryExpr{
		Left:  &ColumnRef{Name: "tenant_id"},
		Op:    BinaryAdd,
		Right: &Literal{Value: Value{Kind: ValueInt, Int: 7}},
	})
	if err != nil {
		t.Fatalf("BindExpr arithmetic: %v", err)
	}
	if expr.Kind != BoundExprBinary || expr.Op != BoundOpAdd || expr.Type != types.Int64 {
		t.Fatalf("expr = %#v", expr)
	}
}

func TestBindExprPromotesIntFloatArithmetic(t *testing.T) {
	expr, err := BindExpr(exprColumns(), &BinaryExpr{
		Left:  &ColumnRef{Name: "tenant_id"},
		Op:    BinaryDivide,
		Right: &Literal{Value: Value{Kind: ValueFloat, Float: 2.5}},
	})
	if err != nil {
		t.Fatalf("BindExpr arithmetic: %v", err)
	}
	if expr.Type != types.Float64 {
		t.Fatalf("type = %s, want float64", expr.Type)
	}
}

func TestBindExprTextFunctions(t *testing.T) {
	expr, err := BindExpr(exprColumns(), &FuncCall{Name: "concat", Args: []Expr{
		&FuncCall{Name: "lower", Args: []Expr{&ColumnRef{Name: "event_type"}}},
		&Literal{Value: Value{Kind: ValueString, String: "_x"}},
	}})
	if err != nil {
		t.Fatalf("BindExpr concat: %v", err)
	}
	if expr.Kind != BoundExprBinary || expr.Op != BoundOpConcat || expr.Type != types.Text || expr.Left.Op != BoundOpLower {
		t.Fatalf("expr = %#v", expr)
	}
}

func TestBindExprPredicateOperators(t *testing.T) {
	expr, err := BindExpr(exprColumns(), &AndExpr{
		Left: &BinaryExpr{
			Left:  &ColumnRef{Name: "tenant_id"},
			Op:    BinaryGreaterEqual,
			Right: &Literal{Value: Value{Kind: ValueInt, Int: 10}},
		},
		Right: &InExpr{
			Expr: &ColumnRef{Name: "event_type"},
			Values: []Expr{
				&Literal{Value: Value{Kind: ValueString, String: "signup"}},
				&Literal{Value: Value{Kind: ValueString, String: "checkout"}},
			},
		},
	})
	if err != nil {
		t.Fatalf("BindExpr predicate: %v", err)
	}
	if expr.Kind != BoundExprBinary || expr.Op != BoundOpAnd || expr.Left.Op != BoundOpGreaterEqual || expr.Right.Kind != BoundExprIn {
		t.Fatalf("expr = %#v", expr)
	}
}

func TestBindExprRejectsAggregateCall(t *testing.T) {
	_, err := BindExpr(exprColumns(), &FuncCall{Name: "count", Star: true})
	if err == nil || !strings.Contains(err.Error(), "aggregate count") {
		t.Fatalf("err = %v", err)
	}
}

func TestBindExprRejectsTextFunctionOnNonText(t *testing.T) {
	_, err := BindExpr(exprColumns(), &FuncCall{Name: "lower", Args: []Expr{&ColumnRef{Name: "tenant_id"}}})
	if err == nil || !strings.Contains(err.Error(), "text argument") {
		t.Fatalf("err = %v", err)
	}
}

func TestBindExprRejectsNonNumericArithmetic(t *testing.T) {
	_, err := BindExpr(exprColumns(), &BinaryExpr{
		Left:  &ColumnRef{Name: "event_type"},
		Op:    BinaryAdd,
		Right: &Literal{Value: Value{Kind: ValueInt, Int: 1}},
	})
	if err == nil || !strings.Contains(err.Error(), "numeric operands") {
		t.Fatalf("err = %v", err)
	}
}

func TestValidateWhereComparisonNormalizesTemporalAndUUID(t *testing.T) {
	ts := BoundExpr{Kind: BoundExprColumn, Type: types.Timestamp, Column: "created_at"}
	tsLit := BoundExpr{Kind: BoundExprLiteral, Type: types.Text, Literal: "2026-05-07T01:02:03Z"}
	if err := validateWhereComparison(ts, FilterEqual, tsLit); err != nil {
		t.Fatalf("timestamp comparison: %v", err)
	}
	normalizeComparison(&ts, &tsLit)
	if tsLit.Literal != "2026-05-07T01:02:03.000000000Z" {
		t.Fatalf("timestamp literal = %v", tsLit.Literal)
	}

	uuid := BoundExpr{Kind: BoundExprColumn, Type: types.UUID, Column: "id"}
	uuidLit := BoundExpr{Kind: BoundExprLiteral, Type: types.Text, Literal: "550E8400-E29B-41D4-A716-446655440000"}
	if err := validateWhereComparison(uuid, FilterEqual, uuidLit); err != nil {
		t.Fatalf("uuid comparison: %v", err)
	}
	normalizeComparison(&uuid, &uuidLit)
	if uuidLit.Literal != "550e8400-e29b-41d4-a716-446655440000" {
		t.Fatalf("uuid literal = %v", uuidLit.Literal)
	}
}

func exprColumns() []BoundColumnDef {
	return []BoundColumnDef{
		{ID: 1, Name: "tenant_id", Type: types.Int64},
		{ID: 2, Name: "event_type", Type: types.Text},
		{ID: 3, Name: "created_at", Type: types.Timestamp},
		{ID: 4, Name: "id", Type: types.UUID},
	}
}
