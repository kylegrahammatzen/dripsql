package sql

import (
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestBindSelectScanPlan(t *testing.T) {
	limit := int64(10)
	offset := int64(2)
	plan, err := BindSelect(&SelectStmt{
		Table: "events",
		Select: []SelectExpr{
			{Expr: &ColumnRef{Name: "tenant_id"}},
			{Expr: &ColumnRef{Name: "event_type"}, Alias: "kind"},
		},
		Where: &BinaryExpr{Left: &ColumnRef{Name: "tenant_id"}, Op: BinaryEqual, Right: &Literal{Value: Value{Kind: ValueInt, Int: 7}}},
		OrderBy: []OrderExpr{
			{Name: "kind", Desc: true},
		},
		Limit:  &limit,
		Offset: &offset,
	}, eventsDef())
	if err != nil {
		t.Fatalf("BindSelect: %v", err)
	}

	limitPlan, ok := plan.(*LimitPlan)
	if !ok || limitPlan.N != 10 || limitPlan.Offset != 2 {
		t.Fatalf("limit plan = %#v", plan)
	}
	sortPlan, ok := limitPlan.Source.(*SortPlan)
	if !ok || len(sortPlan.Keys) != 1 || sortPlan.Keys[0].Name != "kind" || !sortPlan.Keys[0].Desc {
		t.Fatalf("sort plan = %#v", limitPlan.Source)
	}
	project, ok := sortPlan.Source.(*ProjectPlan)
	if !ok || len(project.Exprs) != 2 || project.Exprs[1].Alias != "kind" {
		t.Fatalf("project plan = %#v", sortPlan.Source)
	}
	scan, ok := project.Source.(*ScanPlan)
	if !ok || scan.Where == nil || scan.Where.Op != BoundOpEqual || scan.Where.Left.Column != "tenant_id" {
		t.Fatalf("scan plan = %#v", project.Source)
	}
}

func TestBindSelectStar(t *testing.T) {
	plan, err := BindSelect(&SelectStmt{Table: "events", Select: []SelectExpr{{Expr: &StarRef{}}}}, eventsDef())
	if err != nil {
		t.Fatalf("BindSelect star: %v", err)
	}
	project, ok := plan.(*ProjectPlan)
	if !ok || len(project.Exprs) != 2 || project.Exprs[0].Expr.Column != "tenant_id" || project.Exprs[1].Expr.Column != "event_type" {
		t.Fatalf("plan = %#v", plan)
	}
}

func TestBindSelectLiteralProjections(t *testing.T) {
	plan, err := BindSelect(&SelectStmt{
		Table: "events",
		Select: []SelectExpr{
			{Expr: &ColumnRef{Name: "tenant_id"}},
			{Expr: &Literal{Value: Value{Kind: ValueString, String: "active"}}, Alias: "label"},
			{Expr: &Literal{Value: Value{Kind: ValueInt, Int: 1}}, Alias: "version"},
		},
		OrderBy: []OrderExpr{{Name: "label"}},
	}, eventsDef())
	if err != nil {
		t.Fatalf("BindSelect literals: %v", err)
	}
	sortPlan := plan.(*SortPlan)
	project := sortPlan.Source.(*ProjectPlan)
	if len(project.Exprs) != 3 || project.Exprs[1].Expr.Literal != "active" || project.Exprs[2].Expr.Literal != int64(1) || sortPlan.Keys[0].Name != "label" {
		t.Fatalf("plan = %#v", plan)
	}

	_, err = BindSelect(&SelectStmt{Table: "events", Select: []SelectExpr{{Expr: &Literal{Value: Value{Kind: ValueString, String: "active"}}}}}, eventsDef())
	if err == nil || !strings.Contains(err.Error(), "literal expressions require an alias") {
		t.Fatalf("err = %v", err)
	}
}

func TestBindSelectLiteralOnlyProjection(t *testing.T) {
	plan, err := BindSelect(&SelectStmt{
		Table: "events",
		Select: []SelectExpr{
			{Expr: &Literal{Value: Value{Kind: ValueString, String: "active"}}, Alias: "label"},
			{Expr: &Literal{Value: Value{Kind: ValueInt, Int: 1}}, Alias: "version"},
		},
		OrderBy: []OrderExpr{{Name: "version", Desc: true}},
	}, eventsDef())
	if err != nil {
		t.Fatalf("BindSelect literal only: %v", err)
	}
	sortPlan := plan.(*SortPlan)
	project := sortPlan.Source.(*ProjectPlan)
	if project.Exprs[0].Expr.Literal != "active" || project.Exprs[1].Expr.Literal != int64(1) || sortPlan.Keys[0].Name != "version" || !sortPlan.Keys[0].Desc {
		t.Fatalf("plan = %#v", plan)
	}
}

func TestBindSelectComputedProjections(t *testing.T) {
	plan, err := BindSelect(&SelectStmt{
		Table: "events",
		Select: []SelectExpr{
			{Expr: &ColumnRef{Name: "tenant_id"}},
			{Expr: &BinaryExpr{Left: &ColumnRef{Name: "tenant_id"}, Op: BinaryAdd, Right: &Literal{Value: Value{Kind: ValueInt, Int: 1}}}, Alias: "next_tenant"},
			{Expr: &BinaryExpr{Left: &Literal{Value: Value{Kind: ValueInt, Int: 100}}, Op: BinarySubtract, Right: &ColumnRef{Name: "tenant_id"}}, Alias: "remaining"},
		},
		OrderBy: []OrderExpr{{Name: "next_tenant", Desc: true}},
	}, eventsDef())
	if err != nil {
		t.Fatalf("BindSelect computed: %v", err)
	}
	sortPlan := plan.(*SortPlan)
	project := sortPlan.Source.(*ProjectPlan)
	next := project.Exprs[1].Expr
	remaining := project.Exprs[2].Expr
	if next.Kind != BoundExprBinary || next.Left.Column != "tenant_id" || next.Op != BoundOpAdd || next.Right.Literal != int64(1) || remaining.Left.Literal != int64(100) || sortPlan.Keys[0].Name != "next_tenant" {
		t.Fatalf("plan = %#v", plan)
	}

	_, err = BindSelect(&SelectStmt{Table: "events", Select: []SelectExpr{{Expr: &BinaryExpr{Left: &ColumnRef{Name: "tenant_id"}, Op: BinaryAdd, Right: &Literal{Value: Value{Kind: ValueInt, Int: 1}}}}}}, eventsDef())
	if err == nil || !strings.Contains(err.Error(), "computed expressions require an alias") {
		t.Fatalf("err = %v", err)
	}
}

func TestBindSelectFloatExpressions(t *testing.T) {
	def := BoundTableDef{
		Name: "metrics",
		Columns: []BoundColumnDef{
			{ID: 1, Name: "id", Type: types.Int64},
			{ID: 2, Name: "f32", Type: types.Float32},
			{ID: 3, Name: "f64", Type: types.Float64},
		},
	}
	plan, err := BindSelect(&SelectStmt{
		Table: "metrics",
		Select: []SelectExpr{
			{Expr: &ColumnRef{Name: "id"}},
			{Expr: &BinaryExpr{Left: &ColumnRef{Name: "f32"}, Op: BinaryAdd, Right: &Literal{Value: Value{Kind: ValueFloat, Float: 1.5}}}, Alias: "adjusted"},
		},
		Where:   &BinaryExpr{Left: &ColumnRef{Name: "f64"}, Op: BinaryGreaterEqual, Right: &Literal{Value: Value{Kind: ValueFloat, Float: 2.25}}},
		OrderBy: []OrderExpr{{Expr: &BinaryExpr{Left: &ColumnRef{Name: "f64"}, Op: BinaryMultiply, Right: &Literal{Value: Value{Kind: ValueInt, Int: 2}}}, Desc: true}},
	}, def)
	if err != nil {
		t.Fatalf("BindSelect float: %v", err)
	}
	sortPlan := plan.(*SortPlan)
	project := sortPlan.Source.(*ProjectPlan)
	scan := project.Source.(*ScanPlan)
	adjusted := project.Exprs[1].Expr
	if adjusted.Type.Kind != types.KindFloat64 || adjusted.Left.Type.Kind != types.KindFloat32 || adjusted.Right.Literal != 1.5 {
		t.Fatalf("adjusted = %#v", adjusted)
	}
	if scan.Where == nil || scan.Where.Op != BoundOpGreaterEqual || scan.Where.Right.Literal != 2.25 {
		t.Fatalf("where = %#v", scan.Where)
	}
	if sortPlan.Keys[0].Expr.Type.Kind != types.KindFloat64 {
		t.Fatalf("sort = %#v", sortPlan.Keys[0])
	}
}

func TestBindSelectMultiOrderByScan(t *testing.T) {
	plan, err := BindSelect(&SelectStmt{
		Table: "events",
		Select: []SelectExpr{
			{Expr: &ColumnRef{Name: "tenant_id"}, Alias: "tenant"},
			{Expr: &ColumnRef{Name: "event_type"}},
			{Expr: &BinaryExpr{Left: &ColumnRef{Name: "tenant_id"}, Op: BinarySubtract, Right: &Literal{Value: Value{Kind: ValueInt, Int: 10}}}, Alias: "remaining"},
		},
		OrderBy: []OrderExpr{
			{Name: "tenant", Desc: true},
			{Expr: &BinaryExpr{Left: &ColumnRef{Name: "tenant_id"}, Op: BinaryAdd, Right: &Literal{Value: Value{Kind: ValueInt, Int: 1}}}},
			{Name: "event_type", Desc: true},
		},
	}, eventsDef())
	if err != nil {
		t.Fatalf("BindSelect multi order by: %v", err)
	}
	sortPlan := plan.(*SortPlan)
	if len(sortPlan.Keys) != 3 {
		t.Fatalf("sort keys = %#v", sortPlan.Keys)
	}
	if sortPlan.Keys[0].Name != "tenant" || !sortPlan.Keys[0].Desc || sortPlan.Keys[0].Expr.Kind != BoundExprColumn || sortPlan.Keys[0].Expr.Column != "tenant_id" {
		t.Fatalf("sort[0] = %#v", sortPlan.Keys[0])
	}
	if sortPlan.Keys[1].Desc || sortPlan.Keys[1].Name != "" || sortPlan.Keys[1].Expr.Kind != BoundExprBinary || sortPlan.Keys[1].Expr.Op != BoundOpAdd {
		t.Fatalf("sort[1] = %#v", sortPlan.Keys[1])
	}
	if sortPlan.Keys[2].Name != "event_type" || !sortPlan.Keys[2].Desc || sortPlan.Keys[2].Expr.Kind != BoundExprColumn || sortPlan.Keys[2].Expr.Column != "event_type" {
		t.Fatalf("sort[2] = %#v", sortPlan.Keys[2])
	}
}

func TestBindSelectAggregateMultiOrderBy(t *testing.T) {
	plan, err := BindSelect(&SelectStmt{
		Table: "events",
		Select: []SelectExpr{
			{Expr: &ColumnRef{Name: "event_type"}, Alias: "kind"},
			{Expr: &FuncCall{Name: "sum", Args: []Expr{&ColumnRef{Name: "tenant_id"}}}, Alias: "total"},
		},
		GroupBy: []Expr{&ColumnRef{Name: "event_type"}},
		OrderBy: []OrderExpr{
			{Name: "kind"},
			{Expr: &BinaryExpr{Left: &ColumnRef{Name: "tenant_id"}, Op: BinaryMultiply, Right: &Literal{Value: Value{Kind: ValueInt, Int: 2}}}, Desc: true},
			{Name: "total", Desc: true},
		},
	}, eventsDef())
	if err != nil {
		t.Fatalf("BindSelect aggregate multi order by: %v", err)
	}
	sortPlan := plan.(*SortPlan)
	if len(sortPlan.Keys) != 3 {
		t.Fatalf("sort keys = %#v", sortPlan.Keys)
	}
	if sortPlan.Keys[0].Name != "kind" || sortPlan.Keys[0].Expr.Kind != BoundExprColumn || sortPlan.Keys[0].Expr.Column != "event_type" {
		t.Fatalf("sort[0] = %#v", sortPlan.Keys[0])
	}
	if sortPlan.Keys[1].Expr.Kind != BoundExprBinary || sortPlan.Keys[1].Expr.Op != BoundOpMultiply || sortPlan.Keys[1].Expr.Right.Literal != int64(2) {
		t.Fatalf("sort[1] = %#v", sortPlan.Keys[1])
	}
	if !sortPlan.Keys[2].Desc || sortPlan.Keys[2].Name != "total" || sortPlan.Keys[2].Expr.Kind != BoundExprColumn || sortPlan.Keys[2].Expr.Column != "total" {
		t.Fatalf("sort[2] = %#v", sortPlan.Keys[2])
	}
}

func TestBindSelectRejectsMultiOrderByBadExpression(t *testing.T) {
	_, err := BindSelect(&SelectStmt{
		Table:  "events",
		Select: []SelectExpr{{Expr: &ColumnRef{Name: "tenant_id"}}},
		OrderBy: []OrderExpr{
			{Name: "tenant_id"},
			{Expr: &BinaryExpr{Left: &ColumnRef{Name: "missing"}, Op: BinaryAdd, Right: &Literal{Value: Value{Kind: ValueInt, Int: 1}}}, Desc: true},
		},
	}, eventsDef())
	if err == nil || !strings.Contains(err.Error(), "missing column \"missing\"") {
		t.Fatalf("err = %v", err)
	}
}

func TestBindSelectAggregatePlan(t *testing.T) {
	plan, err := BindSelect(&SelectStmt{
		Table:  "events",
		Select: []SelectExpr{{Expr: &FuncCall{Name: "sum", Args: []Expr{&ColumnRef{Name: "tenant_id"}}}, Alias: "total"}},
	}, eventsDef())
	if err != nil {
		t.Fatalf("BindSelect aggregate: %v", err)
	}
	project, ok := plan.(*ProjectPlan)
	if !ok || len(project.Exprs) != 1 || project.Exprs[0].Alias != "total" {
		t.Fatalf("project = %#v", plan)
	}
	agg, ok := project.Source.(*AggregatePlan)
	if !ok || len(agg.Aggregates) != 1 || agg.Aggregates[0].Func != AggregateSum || agg.Aggregates[0].ArgName != "tenant_id" || agg.Aggregates[0].ArgColumn != 1 {
		t.Fatalf("aggregate = %#v", project.Source)
	}
}

func TestBindSelectMultiAggregatePlan(t *testing.T) {
	plan, err := BindSelect(&SelectStmt{
		Table: "events",
		Select: []SelectExpr{
			{Expr: &FuncCall{Name: "count", Star: true}, Alias: "n"},
			{Expr: &FuncCall{Name: "sum", Args: []Expr{&ColumnRef{Name: "tenant_id"}}}, Alias: "total"},
		},
	}, eventsDef())
	if err != nil {
		t.Fatalf("BindSelect multi aggregate: %v", err)
	}
	project, ok := plan.(*ProjectPlan)
	if !ok || len(project.Exprs) != 2 || project.Exprs[0].Alias != "n" || project.Exprs[1].Alias != "total" {
		t.Fatalf("project = %#v", plan)
	}
	agg, ok := project.Source.(*AggregatePlan)
	if !ok || len(agg.Aggregates) != 2 || agg.Aggregates[0].Func != AggregateCount || !agg.Aggregates[0].Star || agg.Aggregates[1].Func != AggregateSum || agg.Aggregates[1].ArgName != "tenant_id" {
		t.Fatalf("aggregate = %#v", project.Source)
	}
}

func TestBindSelectGroupedAggregatePlan(t *testing.T) {
	plan, err := BindSelect(&SelectStmt{
		Table: "events",
		Select: []SelectExpr{
			{Expr: &ColumnRef{Name: "event_type"}, Alias: "kind"},
			{Expr: &FuncCall{Name: "count", Star: true}, Alias: "n"},
		},
		GroupBy: []Expr{&ColumnRef{Name: "event_type"}},
	}, eventsDef())
	if err != nil {
		t.Fatalf("BindSelect group: %v", err)
	}
	project, ok := plan.(*ProjectPlan)
	if !ok || len(project.Exprs) != 2 || project.Exprs[0].Alias != "kind" || project.Exprs[1].Alias != "n" {
		t.Fatalf("project = %#v", plan)
	}
	agg, ok := project.Source.(*AggregatePlan)
	if !ok || len(agg.GroupBy) != 1 || agg.GroupBy[0].Column != "event_type" || len(agg.Aggregates) != 1 || !agg.Aggregates[0].Star {
		t.Fatalf("aggregate = %#v", project.Source)
	}
}

func TestBindSelectGroupedMultiAggregatePlan(t *testing.T) {
	plan, err := BindSelect(&SelectStmt{
		Table: "events",
		Select: []SelectExpr{
			{Expr: &ColumnRef{Name: "event_type"}, Alias: "kind"},
			{Expr: &FuncCall{Name: "count", Star: true}, Alias: "n"},
			{Expr: &FuncCall{Name: "sum", Args: []Expr{&ColumnRef{Name: "tenant_id"}}}, Alias: "total"},
		},
		GroupBy: []Expr{&ColumnRef{Name: "event_type"}},
	}, eventsDef())
	if err != nil {
		t.Fatalf("BindSelect grouped multi aggregate: %v", err)
	}
	project, ok := plan.(*ProjectPlan)
	if !ok || len(project.Exprs) != 3 || project.Exprs[0].Alias != "kind" || project.Exprs[1].Alias != "n" || project.Exprs[2].Alias != "total" {
		t.Fatalf("project = %#v", plan)
	}
	agg, ok := project.Source.(*AggregatePlan)
	if !ok || len(agg.GroupBy) != 1 || len(agg.Aggregates) != 2 || agg.Aggregates[1].Func != AggregateSum {
		t.Fatalf("aggregate = %#v", project.Source)
	}
}

func TestBindSelectAggregateOrderLimitOffset(t *testing.T) {
	limit := int64(2)
	offset := int64(1)
	plan, err := BindSelect(&SelectStmt{
		Table: "events",
		Select: []SelectExpr{
			{Expr: &ColumnRef{Name: "event_type"}, Alias: "kind"},
			{Expr: &FuncCall{Name: "sum", Args: []Expr{&ColumnRef{Name: "tenant_id"}}}, Alias: "total"},
		},
		GroupBy: []Expr{&ColumnRef{Name: "event_type"}},
		OrderBy: []OrderExpr{{Name: "total", Desc: true}},
		Limit:   &limit,
		Offset:  &offset,
	}, eventsDef())
	if err != nil {
		t.Fatalf("BindSelect aggregate order/limit: %v", err)
	}
	limitPlan := plan.(*LimitPlan)
	if limitPlan.N != 2 || limitPlan.Offset != 1 {
		t.Fatalf("limit = %#v", limitPlan)
	}
	sortPlan := limitPlan.Source.(*SortPlan)
	if len(sortPlan.Keys) != 1 || sortPlan.Keys[0].Name != "total" || !sortPlan.Keys[0].Desc {
		t.Fatalf("sort = %#v", sortPlan)
	}
}

func TestBindSelectGroupBySupportedKinds(t *testing.T) {
	def := BoundTableDef{
		Name: "events",
		Columns: []BoundColumnDef{
			{ID: 1, Name: "bucket", Type: types.Int16},
			{ID: 2, Name: "created_at", Type: types.Timestamp},
			{ID: 3, Name: "event_date", Type: types.Date},
			{ID: 4, Name: "id", Type: types.UUID},
			{ID: 5, Name: "payload", Type: types.Bytes},
			{ID: 6, Name: "status", Type: types.Named("event_status"), Labels: []string{"new", "done"}},
			{ID: 7, Name: "score", Type: types.Int64},
		},
	}
	for _, name := range []string{"bucket", "created_at", "event_date", "id", "payload", "status"} {
		t.Run(name, func(t *testing.T) {
			plan, err := BindSelect(&SelectStmt{
				Table: "events",
				Select: []SelectExpr{
					{Expr: &ColumnRef{Name: name}},
					{Expr: &FuncCall{Name: "sum", Args: []Expr{&ColumnRef{Name: "score"}}}},
				},
				GroupBy: []Expr{&ColumnRef{Name: name}},
			}, def)
			if err != nil {
				t.Fatalf("BindSelect group %s: %v", name, err)
			}
			project := plan.(*ProjectPlan)
			agg := project.Source.(*AggregatePlan)
			if len(agg.GroupBy) != 1 || agg.GroupBy[0].Column != name {
				t.Fatalf("group = %#v", agg.GroupBy)
			}
		})
	}
}

func TestBindSelectTemporalAndUUIDHaving(t *testing.T) {
	def := BoundTableDef{
		Name: "events",
		Columns: []BoundColumnDef{
			{ID: 1, Name: "created_at", Type: types.Timestamp},
			{ID: 2, Name: "event_date", Type: types.Date},
			{ID: 3, Name: "id", Type: types.UUID},
		},
	}
	plan, err := BindSelect(&SelectStmt{
		Table: "events",
		Select: []SelectExpr{
			{Expr: &ColumnRef{Name: "created_at"}},
			{Expr: &FuncCall{Name: "count", Star: true}},
		},
		GroupBy: []Expr{&ColumnRef{Name: "created_at"}},
		Having:  &InExpr{Expr: &ColumnRef{Name: "created_at"}, Values: []Expr{&Literal{Value: Value{Kind: ValueString, String: "2026-05-07T12:30:00.000000123Z"}}, &Literal{Value: Value{Kind: ValueString, String: "2026-05-08T00:00:00Z"}}}},
	}, def)
	if err != nil {
		t.Fatalf("BindSelect timestamp having: %v", err)
	}
	project := plan.(*ProjectPlan)
	agg := project.Source.(*AggregatePlan)
	if agg.Having == nil || agg.Having.Args[1].Literal != "2026-05-08T00:00:00.000000000Z" {
		t.Fatalf("having = %#v", agg.Having)
	}

	_, err = BindSelect(&SelectStmt{
		Table: "events",
		Select: []SelectExpr{
			{Expr: &ColumnRef{Name: "id"}},
			{Expr: &FuncCall{Name: "count", Star: true}},
		},
		GroupBy: []Expr{&ColumnRef{Name: "id"}},
		Having:  &BinaryExpr{Left: &ColumnRef{Name: "id"}, Op: BinaryEqual, Right: &Literal{Value: Value{Kind: ValueString, String: "bad"}}},
	}, def)
	if err == nil || !strings.Contains(err.Error(), "invalid uuid literal") {
		t.Fatalf("err = %v", err)
	}
}

func TestBindSelectHavingSelectedAggregateAlias(t *testing.T) {
	plan, err := BindSelect(&SelectStmt{
		Table:  "events",
		Select: []SelectExpr{{Expr: &FuncCall{Name: "sum", Args: []Expr{&ColumnRef{Name: "tenant_id"}}}, Alias: "total"}},
		Having: &BinaryExpr{Left: &ColumnRef{Name: "total"}, Op: BinaryGreaterEqual, Right: &Literal{Value: Value{Kind: ValueInt, Int: 10}}},
	}, eventsDef())
	if err != nil {
		t.Fatalf("BindSelect having alias: %v", err)
	}
	project := plan.(*ProjectPlan)
	agg := project.Source.(*AggregatePlan)
	if agg.Having == nil || agg.Having.Op != BoundOpGreaterEqual || agg.Having.Left.Column != "total" || agg.Having.Right.Literal != int64(10) {
		t.Fatalf("having = %#v", agg.Having)
	}
}

func TestBindSelectHavingSelectedAggregateCall(t *testing.T) {
	plan, err := BindSelect(&SelectStmt{
		Table:  "events",
		Select: []SelectExpr{{Expr: &FuncCall{Name: "count", Star: true}}},
		Having: &BinaryExpr{Left: &FuncCall{Name: "count", Star: true}, Op: BinaryGreater, Right: &Literal{Value: Value{Kind: ValueInt, Int: 0}}},
	}, eventsDef())
	if err != nil {
		t.Fatalf("BindSelect having count(*): %v", err)
	}
	project := plan.(*ProjectPlan)
	agg := project.Source.(*AggregatePlan)
	if agg.Having == nil || agg.Having.Left.Column != "count" || agg.Having.Right.Literal != int64(0) {
		t.Fatalf("having = %#v", agg.Having)
	}
}

func TestBindSelectHavingGroupOutputPredicates(t *testing.T) {
	plan, err := BindSelect(&SelectStmt{
		Table: "events",
		Select: []SelectExpr{
			{Expr: &ColumnRef{Name: "event_type"}, Alias: "kind"},
			{Expr: &FuncCall{Name: "count", Star: true}},
		},
		GroupBy: []Expr{&ColumnRef{Name: "event_type"}},
		Having: &AndExpr{
			Left:  &InExpr{Expr: &ColumnRef{Name: "kind"}, Values: []Expr{&Literal{Value: Value{Kind: ValueString, String: "signup"}}, &Literal{Value: Value{Kind: ValueString, String: "checkout"}}}},
			Right: &BinaryExpr{Left: &FuncCall{Name: "lower", Args: []Expr{&ColumnRef{Name: "kind"}}}, Op: BinaryNotEqual, Right: &Literal{Value: Value{Kind: ValueString, String: "login"}}},
		},
	}, eventsDef())
	if err != nil {
		t.Fatalf("BindSelect having group: %v", err)
	}
	project := plan.(*ProjectPlan)
	agg := project.Source.(*AggregatePlan)
	if agg.Having == nil || agg.Having.Op != BoundOpAnd || agg.Having.Left.Kind != BoundExprIn || agg.Having.Right.Left.Op != BoundOpLower {
		t.Fatalf("having = %#v", agg.Having)
	}
}

func TestBindSelectHavingAddsHiddenAggregate(t *testing.T) {
	plan, err := BindSelect(&SelectStmt{
		Table:  "events",
		Select: []SelectExpr{{Expr: &FuncCall{Name: "count", Star: true}}},
		Having: &BinaryExpr{Left: &FuncCall{Name: "sum", Args: []Expr{&ColumnRef{Name: "tenant_id"}}}, Op: BinaryGreater, Right: &Literal{Value: Value{Kind: ValueInt, Int: 1}}},
	}, eventsDef())
	if err != nil {
		t.Fatalf("BindSelect hidden having: %v", err)
	}
	project := plan.(*ProjectPlan)
	agg := project.Source.(*AggregatePlan)
	if len(agg.Hidden) != 1 || agg.Hidden[0].Func != AggregateSum || agg.Hidden[0].ArgName != "tenant_id" || agg.Hidden[0].Alias != "__having_sum_tenant_id" {
		t.Fatalf("hidden = %#v", agg.Hidden)
	}
	if agg.Having == nil || agg.Having.Left.Column != "__having_sum_tenant_id" {
		t.Fatalf("having = %#v", agg.Having)
	}
}

func TestBindSelectHavingDeduplicatesHiddenAggregate(t *testing.T) {
	plan, err := BindSelect(&SelectStmt{
		Table:  "events",
		Select: []SelectExpr{{Expr: &FuncCall{Name: "count", Star: true}}},
		Having: &AndExpr{
			Left:  &BinaryExpr{Left: &FuncCall{Name: "sum", Args: []Expr{&ColumnRef{Name: "tenant_id"}}}, Op: BinaryGreater, Right: &Literal{Value: Value{Kind: ValueInt, Int: 1}}},
			Right: &BinaryExpr{Left: &FuncCall{Name: "sum", Args: []Expr{&ColumnRef{Name: "tenant_id"}}}, Op: BinaryLess, Right: &Literal{Value: Value{Kind: ValueInt, Int: 10}}},
		},
	}, eventsDef())
	if err != nil {
		t.Fatalf("BindSelect hidden having: %v", err)
	}
	project := plan.(*ProjectPlan)
	agg := project.Source.(*AggregatePlan)
	if len(agg.Hidden) != 1 {
		t.Fatalf("hidden = %#v", agg.Hidden)
	}
}

func TestBindSelectRejectsBadAggregateColumn(t *testing.T) {
	_, err := BindSelect(&SelectStmt{
		Table:  "events",
		Select: []SelectExpr{{Expr: &FuncCall{Name: "sum", Args: []Expr{&ColumnRef{Name: "event_type"}}}}},
	}, eventsDef())
	if err == nil || !strings.Contains(err.Error(), "want int32 or int64") {
		t.Fatalf("err = %v", err)
	}
}

func TestBindSelectRejectsBadOrderBy(t *testing.T) {
	_, err := BindSelect(&SelectStmt{
		Table:   "events",
		Select:  []SelectExpr{{Expr: &ColumnRef{Name: "tenant_id"}}},
		OrderBy: []OrderExpr{{Name: "event_type"}},
	}, eventsDef())
	if err == nil || !strings.Contains(err.Error(), "selected output column") {
		t.Fatalf("err = %v", err)
	}
}

func TestBindExplainWrapsSelect(t *testing.T) {
	plan, err := BindExplain(&ExplainStmt{Analyze: true, Inner: &SelectStmt{Table: "events", Select: []SelectExpr{{Expr: &StarRef{}}}}}, eventsDef())
	if err != nil {
		t.Fatalf("BindExplain: %v", err)
	}
	if !plan.Analyze || plan.Inner == nil {
		t.Fatalf("plan = %#v", plan)
	}
}

func TestBindSelectWhereNormalizesUUID(t *testing.T) {
	def := BoundTableDef{Name: "ids", Columns: []BoundColumnDef{{ID: 1, Name: "id", Type: types.UUID}}}
	plan, err := BindSelect(&SelectStmt{
		Table:  "ids",
		Select: []SelectExpr{{Expr: &ColumnRef{Name: "id"}}},
		Where:  &BinaryExpr{Left: &ColumnRef{Name: "id"}, Op: BinaryEqual, Right: &Literal{Value: Value{Kind: ValueString, String: "550E8400-E29B-41D4-A716-446655440000"}}},
	}, def)
	if err != nil {
		t.Fatalf("BindSelect: %v", err)
	}
	project := plan.(*ProjectPlan)
	scan := project.Source.(*ScanPlan)
	if scan.Where.Right.Literal != "550e8400-e29b-41d4-a716-446655440000" {
		t.Fatalf("where = %#v", scan.Where)
	}
}
