package explain

import (
	"reflect"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/sql/logical"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
)

func TestPlanFromLogicalCount(t *testing.T) {
	plan := logical.Query{
		Kind:  logical.QueryAggregate,
		Table: catalog.TableDef{Name: "events"},
		Aggregate: logical.AggregateCount,
	}
	got := PlanFromLogical(plan)
	if got.Op != "Count" {
		t.Errorf("root op = %q, want %q", got.Op, "Count")
	}
	if len(got.Children) != 1 {
		t.Fatalf("expected 1 child, got %d", len(got.Children))
	}
	if got.Children[0].Op != "ReadSegments" || got.Children[0].Table != "events" {
		t.Errorf("child = %+v, want ReadSegments(events)", got.Children[0])
	}
}

func TestPlanFromLogicalSum(t *testing.T) {
	plan := logical.Query{
		Kind:            logical.QueryAggregate,
		Table:           catalog.TableDef{Name: "events"},
		Aggregate:       logical.AggregateSum,
		AggregateColumn: "amount",
	}
	got := PlanFromLogical(plan)
	if got.Op != "Sum(amount)" {
		t.Errorf("root op = %q, want %q", got.Op, "Sum(amount)")
	}
}

func TestPlanFromLogicalGroupedCount(t *testing.T) {
	plan := logical.Query{
		Kind:        logical.QueryAggregate,
		Table:       catalog.TableDef{Name: "events"},
		Aggregate:   logical.AggregateCount,
		GroupColumn: "country",
	}
	got := PlanFromLogical(plan)
	want := "Group(country), Count"
	if got.Op != want {
		t.Errorf("root op = %q, want %q", got.Op, want)
	}
}

func TestPlanFromLogicalScanProject(t *testing.T) {
	plan := logical.Query{
		Kind:  logical.QueryScan,
		Table: catalog.TableDef{Name: "events"},
		SelectOutputs: []logical.OutputExpr{
			{Alias: "tenant_id"},
			{Alias: "event_type"},
		},
	}
	got := PlanFromLogical(plan)
	want := "Project(tenant_id, event_type)"
	if got.Op != want {
		t.Errorf("root op = %q, want %q", got.Op, want)
	}
}

func TestPredicatesFromExpr_FlattensAndChain(t *testing.T) {
	// tenant_id = 42 AND event_type = 'checkout'
	tenantEq := mkBinary(logical.OpEqual,
		mkColumn("tenant_id", sqltype.Int64),
		mkLiteral(int64(42), sqltype.Int64),
	)
	eventEq := mkBinary(logical.OpEqual,
		mkColumn("event_type", sqltype.Text),
		mkLiteral("checkout", sqltype.Text),
	)
	whereExpr := mkBinary(logical.OpAnd, tenantEq, eventEq)

	got := PredicatesFromExpr(whereExpr)
	want := []string{"tenant_id = 42", "event_type = 'checkout'"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}
}

func TestPredicatesFromExpr_LowerCall(t *testing.T) {
	// lower(event_type) = 'checkout'
	lowerCall := &logical.Expr{
		Kind: logical.ExprUnary,
		Type: sqltype.Text,
		Op:   logical.OpLower,
		Left: mkColumn("event_type", sqltype.Text),
	}
	whereExpr := mkBinary(logical.OpEqual, lowerCall, mkLiteral("checkout", sqltype.Text))
	got := PredicatesFromExpr(whereExpr)
	want := []string{"lower(event_type) = 'checkout'"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}
}

func TestPredicatesFromExpr_Between(t *testing.T) {
	// amount BETWEEN 10 AND 100
	between := &logical.Expr{
		Kind: logical.ExprBetween,
		Type: sqltype.Bool,
		Left: mkColumn("amount", sqltype.Int64),
		Args: []logical.Expr{
			*mkLiteral(int64(10), sqltype.Int64),
			*mkLiteral(int64(100), sqltype.Int64),
		},
	}
	got := PredicatesFromExpr(between)
	want := []string{"amount BETWEEN 10 AND 100"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}
}

func TestPredicatesFromExpr_In(t *testing.T) {
	in := &logical.Expr{
		Kind: logical.ExprIn,
		Type: sqltype.Bool,
		Left: mkColumn("country", sqltype.Text),
		Args: []logical.Expr{
			*mkLiteral("US", sqltype.Text),
			*mkLiteral("CA", sqltype.Text),
		},
	}
	got := PredicatesFromExpr(in)
	want := []string{"country IN ('US', 'CA')"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %#v, want %#v", got, want)
	}
}

func TestPredicatesFromExpr_Nil(t *testing.T) {
	if got := PredicatesFromExpr(nil); got != nil {
		t.Errorf("nil expr returned %v, want nil", got)
	}
}

func mkColumn(name string, typ sqltype.Type) *logical.Expr {
	return &logical.Expr{Kind: logical.ExprColumn, Type: typ, Column: name}
}

func mkLiteral(v any, typ sqltype.Type) *logical.Expr {
	return &logical.Expr{Kind: logical.ExprLiteral, Type: typ, Literal: v}
}

func mkBinary(op logical.Op, left, right *logical.Expr) *logical.Expr {
	return &logical.Expr{Kind: logical.ExprBinary, Type: sqltype.Bool, Op: op, Left: left, Right: right}
}
