package binder

import (
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/sql/ast"
	"github.com/kylegrahammatzen/dripsql/internal/sql/logical"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
)

func i64(v int64) ast.Value   { return ast.Value{Kind: ast.ValueInt, Int: v} }
func f64(v float64) ast.Value { return ast.Value{Kind: ast.ValueFloat, Float: v} }
func str(v string) ast.Value  { return ast.Value{Kind: ast.ValueString, String: v} }
func boolv(v bool) ast.Value  { return ast.Value{Kind: ast.ValueBool, Bool: v} }
func ptr64(v int64) *int64    { return &v }

// inExpr builds an IN/NOT IN ast for a column with values; slice plumbing is annoying enough to justify the helper.
func inExpr(column string, not bool, values ...ast.Value) ast.Expr {
	exprs := make([]ast.Expr, 0, len(values))
	for _, v := range values {
		exprs = append(exprs, &ast.Literal{Value: v})
	}
	return &ast.InExpr{Expr: &ast.ColumnRef{Name: column}, Values: exprs, Not: not}
}

func mustBind(t *testing.T, stmt *ast.SelectStmt, def catalog.TableDef) logical.Query {
	t.Helper()
	plan, err := BindSelect(stmt, def)
	if err != nil {
		t.Fatalf("BindSelect: %v", err)
	}
	return plan
}

func expectAgg(t *testing.T, p logical.Query, agg logical.AggregateFunc, col string) {
	t.Helper()
	if p.Kind != logical.QueryAggregate || p.Aggregate != agg || p.AggregateColumn != col || p.HasFilter {
		t.Fatalf("plan = %#v", p)
	}
}

func expectAggWhere(t *testing.T, p logical.Query, agg logical.AggregateFunc, aggCol, col string, kind sqltype.Kind, op logical.Op, value any) {
	t.Helper()
	if p.Kind != logical.QueryAggregate || p.Aggregate != agg || p.AggregateColumn != aggCol || !p.HasFilter || !whereSimpleEqual(p.WhereExpr, col, kind, op, value) {
		t.Fatalf("plan = %#v", p)
	}
}

func expectGroupAgg(t *testing.T, p logical.Query, agg logical.AggregateFunc, aggCol, group string, hasFilter bool) {
	t.Helper()
	if p.Kind != logical.QueryAggregate || p.Aggregate != agg || p.AggregateColumn != aggCol || p.GroupColumn != group || p.HasFilter != hasFilter {
		t.Fatalf("plan = %#v", p)
	}
}

func whereSimpleEqual(expr *logical.Expr, col string, kind sqltype.Kind, op logical.Op, value any) bool {
	if expr == nil || expr.Kind != logical.ExprBinary || expr.Op != op {
		return false
	}
	if expr.Left == nil || expr.Left.Kind != logical.ExprColumn || expr.Left.Column != col || expr.Left.Type.Kind != kind {
		return false
	}
	if expr.Right == nil || expr.Right.Kind != logical.ExprLiteral || expr.Right.Literal != value {
		return false
	}
	return true
}

func whereSimpleBetween(expr *logical.Expr, col string, kind sqltype.Kind, lo, hi any) bool {
	if expr == nil || expr.Kind != logical.ExprBetween || expr.Left == nil || len(expr.Args) != 2 {
		return false
	}
	if expr.Left.Kind != logical.ExprColumn || expr.Left.Column != col || expr.Left.Type.Kind != kind {
		return false
	}
	return expr.Args[0].Literal == lo && expr.Args[1].Literal == hi
}

func whereSimpleIn(expr *logical.Expr, col string, kind sqltype.Kind, not bool, values []any) bool {
	if expr == nil || expr.Kind != logical.ExprIn || expr.Left == nil || expr.Not != not {
		return false
	}
	if expr.Left.Kind != logical.ExprColumn || expr.Left.Column != col || expr.Left.Type.Kind != kind {
		return false
	}
	if len(expr.Args) != len(values) {
		return false
	}
	for i, want := range values {
		if expr.Args[i].Literal != want {
			return false
		}
	}
	return true
}

func countStmt(table string, where ast.Expr) *ast.SelectStmt {
	return &ast.SelectStmt{
		Table:  table,
		Select: []ast.SelectExpr{{Expr: &ast.FuncCall{Name: "count", Star: true}}},
		Where:  where,
	}
}

func sumStmt(table, column string, where ast.Expr) *ast.SelectStmt {
	return aggregateStmt(table, "sum", column, where)
}

func aggregateStmt(table, name, column string, where ast.Expr) *ast.SelectStmt {
	return aliasStmt(table, name, column, "", where)
}

func aliasStmt(table, name, column, alias string, where ast.Expr) *ast.SelectStmt {
	return &ast.SelectStmt{
		Table:  table,
		Select: []ast.SelectExpr{{Expr: &ast.FuncCall{Name: name, Args: []ast.Expr{&ast.ColumnRef{Name: column}}}, Alias: alias}},
		Where:  where,
	}
}

func groupCountStmt(table, groupColumn string, where ast.Expr) *ast.SelectStmt {
	return &ast.SelectStmt{
		Table: table,
		Select: []ast.SelectExpr{
			{Expr: &ast.ColumnRef{Name: groupColumn}},
			{Expr: &ast.FuncCall{Name: "count", Star: true}},
		},
		Where:   where,
		GroupBy: []ast.Expr{&ast.ColumnRef{Name: groupColumn}},
	}
}

func groupCountColumnStmt(table, groupColumn, countColumn string, where ast.Expr) *ast.SelectStmt {
	return &ast.SelectStmt{
		Table: table,
		Select: []ast.SelectExpr{
			{Expr: &ast.ColumnRef{Name: groupColumn}},
			{Expr: &ast.FuncCall{Name: "count", Args: []ast.Expr{&ast.ColumnRef{Name: countColumn}}}},
		},
		Where:   where,
		GroupBy: []ast.Expr{&ast.ColumnRef{Name: groupColumn}},
	}
}

func groupSumStmt(table, groupColumn, sumColumn string, where ast.Expr) *ast.SelectStmt {
	return groupAggregateStmt(table, groupColumn, "sum", sumColumn, where)
}

func groupAggregateStmt(table, groupColumn, aggregate, aggColumn string, where ast.Expr) *ast.SelectStmt {
	return groupAliasStmt(table, groupColumn, "", aggregate, aggColumn, "", where)
}

func groupAliasStmt(table, groupColumn, groupAlias, aggregate, aggColumn, aggAlias string, where ast.Expr, orderBy ...ast.OrderExpr) *ast.SelectStmt {
	return &ast.SelectStmt{
		Table: table,
		Select: []ast.SelectExpr{
			{Expr: &ast.ColumnRef{Name: groupColumn}, Alias: groupAlias},
			{Expr: &ast.FuncCall{Name: aggregate, Args: []ast.Expr{&ast.ColumnRef{Name: aggColumn}}}, Alias: aggAlias},
		},
		Where:   where,
		GroupBy: []ast.Expr{&ast.ColumnRef{Name: groupColumn}},
		OrderBy: orderBy,
	}
}

func eventsBoolDef() catalog.TableDef {
	return catalog.TableDef{
		Name: "events",
		Columns: []catalog.ColumnDef{
			{Name: "active", Type: sqltype.Bool},
			{Name: "event_type", Type: sqltype.Text},
		},
	}
}

func eventsInt32Def() catalog.TableDef {
	return catalog.TableDef{
		Name: "events",
		Columns: []catalog.ColumnDef{
			{Name: "tenant_id", Type: sqltype.Int32},
			{Name: "event_type", Type: sqltype.Text},
		},
	}
}

func TestBindSelectAggregate(t *testing.T) {
	tests := []struct {
		name string
		stmt *ast.SelectStmt
		want logical.AggregateFunc
		col  string
	}{
		{"count star", countStmt("events", nil), logical.AggregateCount, ""},
		{"sum column", aggregateStmt("events", "sum", "tenant_id", nil), logical.AggregateSum, "tenant_id"},
		{"min column", aggregateStmt("events", "min", "tenant_id", nil), logical.AggregateMin, "tenant_id"},
		{"max column", aggregateStmt("events", "max", "tenant_id", nil), logical.AggregateMax, "tenant_id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expectAgg(t, mustBind(t, tt.stmt, eventsDef()), tt.want, tt.col)
		})
	}
}

func TestBindSelectAggregateAlias(t *testing.T) {
	p := mustBind(t, aliasStmt("events", "sum", "tenant_id", "total", nil), eventsDef())
	if p.AggregateAlias != "total" {
		t.Fatalf("alias = %q", p.AggregateAlias)
	}
}

func TestBindSelectWhere(t *testing.T) {
	col := func(name string) *ast.ColumnRef { return &ast.ColumnRef{Name: name} }
	lit := func(v ast.Value) *ast.Literal { return &ast.Literal{Value: v} }
	bin := func(c string, op ast.BinaryOp, v ast.Value) ast.Expr {
		return &ast.BinaryExpr{Left: col(c), Op: op, Right: lit(v)}
	}

	t.Run("eq int64", func(t *testing.T) {
		expectAggWhere(t, mustBind(t, countStmt("events", bin("tenant_id", ast.BinaryEqual, i64(7))), eventsDef()),
			logical.AggregateCount, "", "tenant_id", sqltype.KindInt64, logical.OpEqual, int64(7))
	})
	t.Run("ne int64", func(t *testing.T) {
		expectAggWhere(t, mustBind(t, countStmt("events", bin("tenant_id", ast.BinaryNotEqual, i64(7))), eventsDef()),
			logical.AggregateCount, "", "tenant_id", sqltype.KindInt64, logical.OpNotEqual, int64(7))
	})
	t.Run("gte int64", func(t *testing.T) {
		expectAggWhere(t, mustBind(t, countStmt("events", bin("tenant_id", ast.BinaryGreaterEqual, i64(7))), eventsDef()),
			logical.AggregateCount, "", "tenant_id", sqltype.KindInt64, logical.OpGreaterEqual, int64(7))
	})
	t.Run("eq int32 column", func(t *testing.T) {
		expectAggWhere(t, mustBind(t, countStmt("events", bin("tenant_id", ast.BinaryEqual, i64(7))), eventsInt32Def()),
			logical.AggregateCount, "", "tenant_id", sqltype.KindInt32, logical.OpEqual, int64(7))
	})
	t.Run("eq bool", func(t *testing.T) {
		expectAggWhere(t, mustBind(t, countStmt("events", bin("active", ast.BinaryEqual, boolv(true))), eventsBoolDef()),
			logical.AggregateCount, "", "active", sqltype.KindBool, logical.OpEqual, true)
	})
	t.Run("between int64", func(t *testing.T) {
		bw := &ast.BetweenExpr{Expr: col("tenant_id"), Low: lit(i64(2)), High: lit(i64(9))}
		p := mustBind(t, countStmt("events", bw), eventsDef())
		if !p.HasFilter || !whereSimpleBetween(p.WhereExpr, "tenant_id", sqltype.KindInt64, int64(2), int64(9)) {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("between int32", func(t *testing.T) {
		bw := &ast.BetweenExpr{Expr: col("tenant_id"), Low: lit(i64(2)), High: lit(i64(9))}
		p := mustBind(t, countStmt("events", bw), eventsInt32Def())
		if !p.HasFilter || !whereSimpleBetween(p.WhereExpr, "tenant_id", sqltype.KindInt32, int64(2), int64(9)) {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("in int64", func(t *testing.T) {
		p := mustBind(t, countStmt("events", inExpr("tenant_id", false, i64(1), i64(3))), eventsDef())
		if !p.HasFilter || !whereSimpleIn(p.WhereExpr, "tenant_id", sqltype.KindInt64, false, []any{int64(1), int64(3)}) {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("not in int64", func(t *testing.T) {
		p := mustBind(t, countStmt("events", inExpr("tenant_id", true, i64(1), i64(3))), eventsDef())
		if !p.HasFilter || !whereSimpleIn(p.WhereExpr, "tenant_id", sqltype.KindInt64, true, []any{int64(1), int64(3)}) {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("and", func(t *testing.T) {
		expr := &ast.AndExpr{
			Left:  bin("tenant_id", ast.BinaryEqual, i64(7)),
			Right: bin("event_type", ast.BinaryEqual, str("signup")),
		}
		p := mustBind(t, countStmt("events", expr), eventsDef())
		if !p.HasFilter || p.WhereExpr == nil || p.WhereExpr.Op != logical.OpAnd ||
			!whereSimpleEqual(p.WhereExpr.Left, "tenant_id", sqltype.KindInt64, logical.OpEqual, int64(7)) ||
			!whereSimpleEqual(p.WhereExpr.Right, "event_type", sqltype.KindText, logical.OpEqual, "signup") {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("arithmetic", func(t *testing.T) {
		add := &ast.BinaryExpr{Left: col("tenant_id"), Op: ast.BinaryAdd, Right: lit(i64(1))}
		gt := &ast.BinaryExpr{Left: add, Op: ast.BinaryGreater, Right: lit(i64(7))}
		p := mustBind(t, countStmt("events", gt), eventsDef())
		if !p.HasFilter || p.WhereExpr == nil || p.WhereExpr.Op != logical.OpGreater || p.WhereExpr.Left.Op != logical.OpAdd {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("aggregate with where", func(t *testing.T) {
		expectAggWhere(t, mustBind(t, sumStmt("events", "tenant_id", bin("tenant_id", ast.BinaryEqual, i64(1))), eventsDef()),
			logical.AggregateSum, "tenant_id", "tenant_id", sqltype.KindInt64, logical.OpEqual, int64(1))
	})
}

func TestBindSelectGroup(t *testing.T) {
	bin := func(c string, op ast.BinaryOp, v ast.Value) ast.Expr {
		return &ast.BinaryExpr{Left: &ast.ColumnRef{Name: c}, Op: op, Right: &ast.Literal{Value: v}}
	}

	t.Run("group count", func(t *testing.T) {
		expectGroupAgg(t, mustBind(t, groupCountStmt("events", "event_type", nil), eventsDef()),
			logical.AggregateCount, "", "event_type", false)
	})
	t.Run("group count column", func(t *testing.T) {
		expectGroupAgg(t, mustBind(t, groupCountColumnStmt("events", "event_type", "tenant_id", nil), eventsDef()),
			logical.AggregateCount, "tenant_id", "event_type", false)
	})
	t.Run("group sum", func(t *testing.T) {
		expectGroupAgg(t, mustBind(t, groupSumStmt("events", "event_type", "tenant_id", nil), eventsDef()),
			logical.AggregateSum, "tenant_id", "event_type", false)
	})
	t.Run("group min", func(t *testing.T) {
		expectGroupAgg(t, mustBind(t, groupAggregateStmt("events", "event_type", "min", "tenant_id", nil), eventsDef()),
			logical.AggregateMin, "tenant_id", "event_type", false)
	})
	t.Run("group with where", func(t *testing.T) {
		p := mustBind(t, groupCountStmt("events", "event_type", bin("tenant_id", ast.BinaryEqual, i64(1))), eventsDef())
		if p.GroupColumn != "event_type" || !p.HasFilter || !whereSimpleEqual(p.WhereExpr, "tenant_id", sqltype.KindInt64, logical.OpEqual, int64(1)) {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("group sum with where", func(t *testing.T) {
		p := mustBind(t, groupSumStmt("events", "event_type", "tenant_id", bin("tenant_id", ast.BinaryEqual, i64(1))), eventsDef())
		if p.Aggregate != logical.AggregateSum || p.AggregateColumn != "tenant_id" || p.GroupColumn != "event_type" || !whereSimpleEqual(p.WhereExpr, "tenant_id", sqltype.KindInt64, logical.OpEqual, int64(1)) {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("group alias", func(t *testing.T) {
		p := mustBind(t, groupAliasStmt("events", "event_type", "kind", "sum", "tenant_id", "total", nil), eventsDef())
		if p.GroupAlias != "kind" || p.AggregateAlias != "total" {
			t.Fatalf("plan = %#v", p)
		}
	})
}

func TestBindSelectHaving(t *testing.T) {
	col := func(name string) *ast.ColumnRef { return &ast.ColumnRef{Name: name} }
	lit := func(v ast.Value) *ast.Literal { return &ast.Literal{Value: v} }
	bin := func(c string, op ast.BinaryOp, v ast.Value) ast.Expr {
		return &ast.BinaryExpr{Left: col(c), Op: op, Right: lit(v)}
	}
	withGroupAlias := func(having ast.Expr) *ast.SelectStmt {
		s := groupAliasStmt("events", "event_type", "kind", "sum", "tenant_id", "total", nil)
		s.Having = having
		return s
	}
	withGroupCount := func(having ast.Expr) *ast.SelectStmt {
		s := groupCountStmt("events", "event_type", nil)
		s.Having = having
		return s
	}

	t.Run("scalar count(*)", func(t *testing.T) {
		s := countStmt("events", nil)
		s.Having = &ast.BinaryExpr{Left: &ast.FuncCall{Name: "count", Star: true}, Op: ast.BinaryGreater, Right: lit(i64(0))}
		p := mustBind(t, s, eventsDef())
		if !p.HasHaving || !whereSimpleEqual(p.HavingFilterExpr, "count", sqltype.KindInt64, logical.OpGreater, int64(0)) {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("scalar alias", func(t *testing.T) {
		s := aliasStmt("events", "sum", "tenant_id", "total", nil)
		s.Having = bin("total", ast.BinaryGreaterEqual, i64(10))
		p := mustBind(t, s, eventsDef())
		if !p.HasHaving || !whereSimpleEqual(p.HavingFilterExpr, "total", sqltype.KindInt64, logical.OpGreaterEqual, int64(10)) {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("alias gte", func(t *testing.T) {
		p := mustBind(t, withGroupAlias(bin("total", ast.BinaryGreaterEqual, i64(10))), eventsDef())
		if !p.HasHaving || !whereSimpleEqual(p.HavingFilterExpr, "total", sqltype.KindInt64, logical.OpGreaterEqual, int64(10)) {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("between int", func(t *testing.T) {
		bw := &ast.BetweenExpr{Expr: col("total"), Low: lit(i64(10)), High: lit(i64(20))}
		p := mustBind(t, withGroupAlias(bw), eventsDef())
		if !p.HasHaving || !whereSimpleBetween(p.HavingFilterExpr, "total", sqltype.KindInt64, int64(10), int64(20)) {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("count gt", func(t *testing.T) {
		p := mustBind(t, withGroupCount(bin("count", ast.BinaryGreater, i64(1))), eventsDef())
		if !p.HasHaving || !whereSimpleEqual(p.HavingFilterExpr, "count", sqltype.KindInt64, logical.OpGreater, int64(1)) {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("count(*) gt", func(t *testing.T) {
		expr := &ast.BinaryExpr{Left: &ast.FuncCall{Name: "count", Star: true}, Op: ast.BinaryGreater, Right: lit(i64(1))}
		p := mustBind(t, withGroupCount(expr), eventsDef())
		if !p.HasHaving || !whereSimpleEqual(p.HavingFilterExpr, "count", sqltype.KindInt64, logical.OpGreater, int64(1)) {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("sum(col) gte", func(t *testing.T) {
		expr := &ast.BinaryExpr{Left: &ast.FuncCall{Name: "sum", Args: []ast.Expr{col("tenant_id")}}, Op: ast.BinaryGreaterEqual, Right: lit(i64(10))}
		p := mustBind(t, withGroupAlias(expr), eventsDef())
		if !p.HasHaving || !whereSimpleEqual(p.HavingFilterExpr, "total", sqltype.KindInt64, logical.OpGreaterEqual, int64(10)) {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("group in", func(t *testing.T) {
		p := mustBind(t, withGroupAlias(inExpr("kind", false, str("signup"), str("login"))), eventsDef())
		if !p.HasHaving || !whereSimpleIn(p.HavingFilterExpr, "kind", sqltype.KindText, false, []any{"signup", "login"}) {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("group lower expr", func(t *testing.T) {
		expr := &ast.BinaryExpr{Left: &ast.FuncCall{Name: "lower", Args: []ast.Expr{col("kind")}}, Op: ast.BinaryEqual, Right: lit(str("signup"))}
		p := mustBind(t, withGroupAlias(expr), eventsDef())
		if !p.HasHaving || p.HavingFilterExpr == nil || p.HavingFilterExpr.Left == nil || p.HavingFilterExpr.Left.Op != logical.OpLower {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("group ordering text", func(t *testing.T) {
		p := mustBind(t, withGroupAlias(bin("kind", ast.BinaryGreater, str("login"))), eventsDef())
		if !p.HasHaving || !whereSimpleEqual(p.HavingFilterExpr, "kind", sqltype.KindText, logical.OpGreater, "login") {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("group between text", func(t *testing.T) {
		bw := &ast.BetweenExpr{Expr: col("kind"), Low: lit(str("checkout")), High: lit(str("signup"))}
		p := mustBind(t, withGroupAlias(bw), eventsDef())
		if !p.HasHaving || !whereSimpleBetween(p.HavingFilterExpr, "kind", sqltype.KindText, "checkout", "signup") {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("and", func(t *testing.T) {
		expr := &ast.AndExpr{
			Left:  bin("kind", ast.BinaryEqual, str("signup")),
			Right: bin("total", ast.BinaryGreaterEqual, i64(10)),
		}
		p := mustBind(t, withGroupAlias(expr), eventsDef())
		if !p.HasHaving || p.HavingFilterExpr == nil || p.HavingFilterExpr.Op != logical.OpAnd {
			t.Fatalf("plan = %#v", p)
		}
	})
}

func TestBindSelectOrderByLimitOffset(t *testing.T) {
	t.Run("order by alias desc", func(t *testing.T) {
		p := mustBind(t, groupAliasStmt("events", "event_type", "kind", "sum", "tenant_id", "total", nil, ast.OrderExpr{Name: "total", Desc: true}), eventsDef())
		if p.OrderColumn != "total" || !p.OrderDesc {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("limit + offset", func(t *testing.T) {
		s := groupAliasStmt("events", "event_type", "kind", "sum", "tenant_id", "total", nil, ast.OrderExpr{Name: "total", Desc: true})
		s.Limit = ptr64(2)
		s.Offset = ptr64(1)
		p := mustBind(t, s, eventsDef())
		if !p.HasLimit || p.Limit != 2 || !p.HasOffset || p.Offset != 1 {
			t.Fatalf("plan = %#v", p)
		}
	})
}

func TestBindScanSelect(t *testing.T) {
	col := func(name string) *ast.ColumnRef { return &ast.ColumnRef{Name: name} }
	lit := func(v ast.Value) *ast.Literal { return &ast.Literal{Value: v} }

	t.Run("columns + where + order + limit + offset", func(t *testing.T) {
		s := &ast.SelectStmt{
			Table:   "events",
			Select:  []ast.SelectExpr{{Expr: col("tenant_id")}, {Expr: col("event_type"), Alias: "kind"}},
			Where:   &ast.BinaryExpr{Left: col("tenant_id"), Op: ast.BinaryEqual, Right: lit(i64(7))},
			OrderBy: []ast.OrderExpr{{Name: "kind", Desc: true}},
			Limit:   ptr64(10), Offset: ptr64(2),
		}
		p := mustBind(t, s, eventsDef())
		if p.Kind != logical.QueryScan || len(p.SelectOutputs) != 2 || p.SelectOutputs[0].Expr.Column != "tenant_id" || p.SelectOutputs[1].Alias != "kind" || !whereSimpleEqual(p.WhereExpr, "tenant_id", sqltype.KindInt64, logical.OpEqual, int64(7)) || p.OrderColumn != "kind" || !p.OrderDesc || !p.HasLimit || p.Limit != 10 || !p.HasOffset || p.Offset != 2 {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("bad order column", func(t *testing.T) {
		s := &ast.SelectStmt{
			Table:   "events",
			Select:  []ast.SelectExpr{{Expr: col("tenant_id")}, {Expr: col("event_type"), Alias: "kind"}},
			OrderBy: []ast.OrderExpr{{Name: "active"}},
		}
		_, err := BindSelect(s, eventsDef())
		if err == nil || !strings.Contains(err.Error(), "selected output column") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("star", func(t *testing.T) {
		s := &ast.SelectStmt{Table: "events", Select: []ast.SelectExpr{{Expr: &ast.StarRef{}}}, OrderBy: []ast.OrderExpr{{Name: "event_type"}}}
		p := mustBind(t, s, eventsDef())
		if p.Kind != logical.QueryScan || len(p.SelectOutputs) != len(eventsDef().Columns) || p.SelectOutputs[0].Expr.Column != "tenant_id" || p.OrderColumn != "event_type" {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("literal projections require alias", func(t *testing.T) {
		s := &ast.SelectStmt{Table: "events",
			Select: []ast.SelectExpr{
				{Expr: col("tenant_id")},
				{Expr: lit(str("active")), Alias: "label"},
				{Expr: lit(i64(1)), Alias: "version"},
			},
			OrderBy: []ast.OrderExpr{{Name: "label"}}}
		p := mustBind(t, s, eventsDef())
		if len(p.SelectOutputs) != 3 || p.SelectOutputs[1].Expr.Literal != "active" || p.SelectOutputs[2].Expr.Literal != int64(1) || p.OrderColumn != "label" {
			t.Fatalf("plan = %#v", p)
		}

		bad := *s
		bad.Select[1].Alias = ""
		_, err := BindSelect(&bad, eventsDef())
		if err == nil || !strings.Contains(err.Error(), "literal expressions require an alias") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("literal only", func(t *testing.T) {
		s := &ast.SelectStmt{Table: "events",
			Select: []ast.SelectExpr{
				{Expr: lit(str("active")), Alias: "label"},
				{Expr: lit(i64(1)), Alias: "version"},
			},
			OrderBy: []ast.OrderExpr{{Name: "version", Desc: true}}}
		p := mustBind(t, s, eventsDef())
		if p.Kind != logical.QueryScan || p.SelectOutputs[0].Expr.Literal != "active" || p.SelectOutputs[1].Expr.Literal != int64(1) || p.OrderColumn != "version" || !p.OrderDesc {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("computed col + lit", func(t *testing.T) {
		s := &ast.SelectStmt{Table: "events",
			Select: []ast.SelectExpr{
				{Expr: col("tenant_id")},
				{Expr: &ast.BinaryExpr{Left: col("tenant_id"), Op: ast.BinaryAdd, Right: lit(i64(1))}, Alias: "next_tenant"},
			},
			OrderBy: []ast.OrderExpr{{Name: "next_tenant", Desc: true}}}
		p := mustBind(t, s, eventsDef())
		out := p.SelectOutputs[1].Expr
		if out.Kind != logical.ExprBinary || out.Left.Column != "tenant_id" || out.Op != logical.OpAdd || out.Right.Literal != int64(1) || p.OrderColumn != "next_tenant" {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("computed mixed", func(t *testing.T) {
		s := &ast.SelectStmt{Table: "events",
			Select: []ast.SelectExpr{
				{Expr: &ast.BinaryExpr{Left: lit(i64(100)), Op: ast.BinarySubtract, Right: col("tenant_id")}, Alias: "remaining"},
				{Expr: &ast.BinaryExpr{Left: col("tenant_id"), Op: ast.BinaryAdd, Right: col("tenant_id")}, Alias: "doubled"},
			}}
		p := mustBind(t, s, eventsDef())
		if p.SelectOutputs[0].Expr.Left.Literal != int64(100) || p.SelectOutputs[0].Expr.Right.Column != "tenant_id" || p.SelectOutputs[1].Expr.Left.Column != "tenant_id" {
			t.Fatalf("plan = %#v", p)
		}
	})
	t.Run("arithmetic projections", func(t *testing.T) {
		ar := func(l ast.Expr, op ast.BinaryOp, r ast.Expr) ast.Expr {
			return &ast.BinaryExpr{Left: l, Op: op, Right: r}
		}
		s := &ast.SelectStmt{
			Table: "events",
			Select: []ast.SelectExpr{
				{Expr: ar(ar(ar(lit(i64(5)), ast.BinaryMultiply, lit(i64(2))), ast.BinarySubtract, lit(i64(6))), ast.BinaryDivide, lit(i64(2))), Alias: "result"},
				{Expr: ar(lit(i64(11)), ast.BinaryModulo, lit(i64(5))), Alias: "remainder"},
				{Expr: ar(lit(i64(10)), ast.BinaryIntDivide, lit(i64(4))), Alias: "quotient"},
				{Expr: ar(col("tenant_id"), ast.BinaryMultiply, ar(col("tenant_id"), ast.BinaryAdd, lit(i64(2)))), Alias: "mixed"},
			},
		}
		p := mustBind(t, s, eventsDef())
		if p.Kind != logical.QueryScan || len(p.SelectOutputs) != 4 {
			t.Fatalf("plan = %#v", p)
		}
		if p.SelectOutputs[0].Expr.Op != logical.OpDivide || p.SelectOutputs[0].Expr.Left.Op != logical.OpSubtract || p.SelectOutputs[0].Expr.Left.Left.Op != logical.OpMultiply || p.SelectOutputs[0].Expr.Right.Literal != int64(2) {
			t.Fatalf("result = %#v", p.SelectOutputs[0].Expr)
		}
		if p.SelectOutputs[1].Expr.Op != logical.OpModulo {
			t.Fatalf("remainder = %#v", p.SelectOutputs[1].Expr)
		}
		if p.SelectOutputs[2].Expr.Op != logical.OpIntDivide {
			t.Fatalf("quotient = %#v", p.SelectOutputs[2].Expr)
		}
		if p.SelectOutputs[3].Expr.Op != logical.OpMultiply || p.SelectOutputs[3].Expr.Left.Column != "tenant_id" || p.SelectOutputs[3].Expr.Right.Op != logical.OpAdd {
			t.Fatalf("mixed = %#v", p.SelectOutputs[3].Expr)
		}
	})
}

func TestBindSelectRejectsUnsupportedShapes(t *testing.T) {
	col := func(name string) *ast.ColumnRef { return &ast.ColumnRef{Name: name} }
	lit := func(v ast.Value) *ast.Literal { return &ast.Literal{Value: v} }
	bin := func(c string, op ast.BinaryOp, v ast.Value) ast.Expr {
		return &ast.BinaryExpr{Left: col(c), Op: op, Right: lit(v)}
	}
	bw := func(c string, lo, hi ast.Value) ast.Expr {
		return &ast.BetweenExpr{Expr: col(c), Low: lit(lo), High: lit(hi)}
	}
	groupHaving := func(havingCol string, op ast.BinaryOp, v ast.Value) *ast.SelectStmt {
		s := groupCountStmt("events", "event_type", nil)
		s.Having = bin(havingCol, op, v)
		return s
	}
	groupHavingEq := func(havingCol string, v ast.Value) *ast.SelectStmt {
		return groupHaving(havingCol, ast.BinaryEqual, v)
	}
	groupOrder := func(orderCol string) *ast.SelectStmt {
		s := groupCountStmt("events", "event_type", nil)
		s.OrderBy = []ast.OrderExpr{{Name: orderCol}}
		return s
	}
	orderScalar := func() *ast.SelectStmt {
		s := countStmt("events", nil)
		s.OrderBy = []ast.OrderExpr{{Name: "count"}}
		return s
	}

	tests := []struct {
		name string
		stmt *ast.SelectStmt
		def  catalog.TableDef
		want string
	}{
		{"missing where column", countStmt("events", bin("missing", ast.BinaryEqual, i64(1))), eventsDef(), "missing WHERE column"},
		{"bad where literal", countStmt("events", bin("tenant_id", ast.BinaryEqual, str("bad"))), eventsDef(), "expects int64 literal"},
		{"text between", countStmt("events", bw("event_type", str("a"), str("z"))), eventsDef(), "BETWEEN is only supported for numeric and temporal expressions"},
		{"int32 out of range", countStmt("events", bin("tenant_id", ast.BinaryEqual, i64(2147483648))), eventsInt32Def(), "int32 literal out of range"},
		{"bad bool literal", countStmt("events", bin("active", ast.BinaryEqual, str("true"))), eventsBoolDef(), "expects bool literal"},
		{"bool between", countStmt("events", bw("active", boolv(false), boolv(true))), eventsBoolDef(), "BETWEEN is only supported for numeric and temporal expressions"},
		{"where group missing column", groupCountStmt("events", "event_type", bin("missing", ast.BinaryEqual, i64(1))), eventsDef(), "missing WHERE column"},
		{"order scalar", orderScalar(), eventsDef(), "ORDER BY is only supported for GROUP BY"},
		{"order missing output", groupOrder("missing"), eventsDef(), "must be a selected output column"},
		{"having missing output", groupHaving("missing", ast.BinaryGreater, i64(1)), eventsDef(), "must be a selected output column"},
		{"having group bad literal", groupHavingEq("event_type", i64(1)), eventsDef(), "expects string literal"},
		{"having bad literal", groupHaving("count", ast.BinaryGreater, str("1")), eventsDef(), "expects int literal"},
		{"having negative count", groupHaving("count", ast.BinaryGreater, i64(-1)), eventsDef(), "count literal must be non-negative"},
		{"sum text", sumStmt("events", "event_type", nil), eventsDef(), "want int32 or int64"},
		{"min text", aggregateStmt("events", "min", "event_type", nil), eventsDef(), "want int32 or int64"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := BindSelect(tt.stmt, tt.def)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestBindSelectFloatExpressions(t *testing.T) {
	def := catalog.TableDef{
		Name: "metrics",
		Columns: []catalog.ColumnDef{
			{Name: "id", Type: sqltype.Int64},
			{Name: "f32", Type: sqltype.Float32},
			{Name: "f64", Type: sqltype.Float64},
		},
	}
	col := func(name string) *ast.ColumnRef { return &ast.ColumnRef{Name: name} }
	lit := func(v ast.Value) *ast.Literal { return &ast.Literal{Value: v} }

	stmt := &ast.SelectStmt{
		Table: "metrics",
		Select: []ast.SelectExpr{
			{Expr: col("id")},
			{Expr: &ast.BinaryExpr{Left: col("f32"), Op: ast.BinaryAdd, Right: lit(f64(1.5))}, Alias: "adjusted"},
		},
		Where:   &ast.BinaryExpr{Left: col("f64"), Op: ast.BinaryGreaterEqual, Right: lit(f64(2.25))},
		OrderBy: []ast.OrderExpr{{Expr: &ast.BinaryExpr{Left: col("f64"), Op: ast.BinaryMultiply, Right: lit(i64(2))}, Desc: true}},
	}
	p := mustBind(t, stmt, def)
	if p.Kind != logical.QueryScan || len(p.SelectOutputs) != 2 {
		t.Fatalf("plan = %#v", p)
	}
	adjusted := p.SelectOutputs[1].Expr
	if adjusted.Kind != logical.ExprBinary || adjusted.Type.Kind != sqltype.KindFloat64 || adjusted.Left.Type.Kind != sqltype.KindFloat32 || adjusted.Right.Literal != 1.5 {
		t.Fatalf("adjusted = %#v", adjusted)
	}
	if !p.HasFilter || p.WhereExpr == nil || p.WhereExpr.Op != logical.OpGreaterEqual || p.WhereExpr.Right.Literal != 2.25 {
		t.Fatalf("where = %#v", p.WhereExpr)
	}
	if p.OrderExpr == nil || p.OrderExpr.Type.Kind != sqltype.KindFloat64 {
		t.Fatalf("order = %#v", p.OrderExpr)
	}
}

func TestBindSelectInt16GroupBy(t *testing.T) {
	def := catalog.TableDef{
		Name: "events",
		Columns: []catalog.ColumnDef{
			{Name: "bucket", Type: sqltype.Int16},
			{Name: "score", Type: sqltype.Int64},
		},
	}
	p := mustBind(t, groupSumStmt("events", "bucket", "score", nil), def)
	if p.Kind != logical.QueryAggregate || p.Aggregate != logical.AggregateSum || p.GroupColumn != "bucket" || p.GroupType != sqltype.KindInt16 {
		t.Fatalf("plan = %#v", p)
	}
}

func TestBindSelectTemporalGroupBy(t *testing.T) {
	def := catalog.TableDef{
		Name: "events",
		Columns: []catalog.ColumnDef{
			{Name: "created_at", Type: sqltype.Timestamp},
			{Name: "event_date", Type: sqltype.Date},
		},
	}
	col := func(name string) *ast.ColumnRef { return &ast.ColumnRef{Name: name} }
	lit := func(v ast.Value) *ast.Literal { return &ast.Literal{Value: v} }

	datePlan := mustBind(t, groupCountStmt("events", "event_date", nil), def)
	if datePlan.GroupType != sqltype.KindDate {
		t.Fatalf("date plan = %#v", datePlan)
	}
	tsPlan := mustBind(t, groupCountStmt("events", "created_at", nil), def)
	if tsPlan.GroupType != sqltype.KindTimestamp {
		t.Fatalf("timestamp plan = %#v", tsPlan)
	}

	dateHavingStmt := groupCountStmt("events", "event_date", nil)
	dateHavingStmt.Having = &ast.BetweenExpr{Expr: col("event_date"), Low: lit(str("2026-05-07")), High: lit(str("2026-05-08"))}
	dateHavingPlan := mustBind(t, dateHavingStmt, def)
	if !dateHavingPlan.HasHaving || !whereSimpleBetween(dateHavingPlan.HavingFilterExpr, "event_date", sqltype.KindDate, "2026-05-07", "2026-05-08") {
		t.Fatalf("date having plan = %#v", dateHavingPlan)
	}

	tsHavingStmt := groupCountStmt("events", "created_at", nil)
	tsHavingStmt.Having = inExpr("created_at", false, str("2026-05-07T12:30:00.000000123Z"), str("2026-05-08T00:00:00Z"))
	tsHavingPlan := mustBind(t, tsHavingStmt, def)
	if !tsHavingPlan.HasHaving || !whereSimpleIn(tsHavingPlan.HavingFilterExpr, "created_at", sqltype.KindTimestamp, false, []any{"2026-05-07T12:30:00.000000123Z", "2026-05-08T00:00:00.000000000Z"}) {
		t.Fatalf("ts having plan = %#v", tsHavingPlan)
	}

	tsHavingStmt.Having = &ast.BinaryExpr{Left: col("created_at"), Op: ast.BinaryEqual, Right: lit(str("bad"))}
	if _, err := BindSelect(tsHavingStmt, def); err == nil || !strings.Contains(err.Error(), "invalid timestamp literal") {
		t.Fatalf("err = %v", err)
	}
}

func TestBindSelectUUIDBytesGroupBy(t *testing.T) {
	def := catalog.TableDef{
		Name: "events",
		Columns: []catalog.ColumnDef{
			{Name: "id", Type: sqltype.UUID},
			{Name: "payload", Type: sqltype.Bytes},
			{Name: "score", Type: sqltype.Int64},
		},
	}
	col := func(name string) *ast.ColumnRef { return &ast.ColumnRef{Name: name} }
	lit := func(v ast.Value) *ast.Literal { return &ast.Literal{Value: v} }
	bin := func(c string, op ast.BinaryOp, v ast.Value) ast.Expr {
		return &ast.BinaryExpr{Left: col(c), Op: op, Right: lit(v)}
	}

	uuidPlan := mustBind(t, groupCountStmt("events", "id", nil), def)
	if uuidPlan.GroupType != sqltype.KindUUID {
		t.Fatalf("uuid plan = %#v", uuidPlan)
	}
	bytesPlan := mustBind(t, groupSumStmt("events", "payload", "score", nil), def)
	if bytesPlan.GroupType != sqltype.KindBytes {
		t.Fatalf("bytes plan = %#v", bytesPlan)
	}

	uuidHavingStmt := groupCountStmt("events", "id", nil)
	uuidHavingStmt.Having = bin("id", ast.BinaryEqual, str("550E8400-E29B-41D4-A716-446655440000"))
	p := mustBind(t, uuidHavingStmt, def)
	if !p.HasHaving || !whereSimpleEqual(p.HavingFilterExpr, "id", sqltype.KindUUID, logical.OpEqual, "550e8400-e29b-41d4-a716-446655440000") {
		t.Fatalf("uuid having plan = %#v", p)
	}

	uuidHavingStmt.Having = bin("id", ast.BinaryGreater, str("550e8400-e29b-41d4-a716-446655440000"))
	if _, err := BindSelect(uuidHavingStmt, def); err == nil || !strings.Contains(err.Error(), "uuid comparisons") {
		t.Fatalf("err = %v", err)
	}
	uuidHavingStmt.Having = bin("id", ast.BinaryEqual, str("bad"))
	if _, err := BindSelect(uuidHavingStmt, def); err == nil || !strings.Contains(err.Error(), "invalid uuid literal") {
		t.Fatalf("err = %v", err)
	}

	bytesHavingStmt := groupCountStmt("events", "payload", nil)
	bytesHavingStmt.Having = inExpr("payload", true, str("aa"))
	p = mustBind(t, bytesHavingStmt, def)
	if !p.HasHaving || !whereSimpleIn(p.HavingFilterExpr, "payload", sqltype.KindBytes, true, []any{"aa"}) {
		t.Fatalf("bytes having plan = %#v", p)
	}

	bytesHavingStmt.Having = bin("payload", ast.BinaryGreater, str("aa"))
	if _, err := BindSelect(bytesHavingStmt, def); err == nil || !strings.Contains(err.Error(), "bytes comparisons") {
		t.Fatalf("err = %v", err)
	}
}

func TestBindSelectUUIDPredicates(t *testing.T) {
	def := catalog.TableDef{
		Name: "events",
		Columns: []catalog.ColumnDef{
			{Name: "id", Type: sqltype.UUID},
			{Name: "event_type", Type: sqltype.Text},
		},
	}
	col := func(name string) *ast.ColumnRef { return &ast.ColumnRef{Name: name} }
	lit := func(v ast.Value) *ast.Literal { return &ast.Literal{Value: v} }
	bin := func(c string, op ast.BinaryOp, v ast.Value) ast.Expr {
		return &ast.BinaryExpr{Left: col(c), Op: op, Right: lit(v)}
	}

	stmt := &ast.SelectStmt{
		Table:  "events",
		Select: []ast.SelectExpr{{Expr: col("id")}},
		Where:  bin("id", ast.BinaryEqual, str("550e8400-e29b-41d4-a716-446655440000")),
	}
	p := mustBind(t, stmt, def)
	if p.Kind != logical.QueryScan || !p.HasFilter || p.WhereExpr == nil || p.WhereExpr.Op != logical.OpEqual {
		t.Fatalf("plan = %#v", p)
	}

	stmt.Where = bin("id", ast.BinaryGreater, str("550e8400-e29b-41d4-a716-446655440000"))
	if _, err := BindSelect(stmt, def); err == nil || !strings.Contains(err.Error(), "uuid comparisons") {
		t.Fatalf("err = %v", err)
	}
}

func TestBindSelectBytesPredicates(t *testing.T) {
	def := catalog.TableDef{
		Name: "files",
		Columns: []catalog.ColumnDef{
			{Name: "payload", Type: sqltype.Bytes},
			{Name: "name", Type: sqltype.Text},
		},
	}
	col := func(name string) *ast.ColumnRef { return &ast.ColumnRef{Name: name} }
	lit := func(v ast.Value) *ast.Literal { return &ast.Literal{Value: v} }

	stmt := &ast.SelectStmt{
		Table:  "files",
		Select: []ast.SelectExpr{{Expr: col("payload")}},
		Where:  inExpr("payload", false, str("aa"), str("bb")),
	}
	p := mustBind(t, stmt, def)
	if p.Kind != logical.QueryScan || !p.HasFilter || p.WhereExpr == nil || p.WhereExpr.Kind != logical.ExprIn {
		t.Fatalf("plan = %#v", p)
	}

	stmt.Where = &ast.BinaryExpr{Left: col("payload"), Op: ast.BinaryGreater, Right: lit(str("aa"))}
	if _, err := BindSelect(stmt, def); err == nil || !strings.Contains(err.Error(), "bytes comparisons") {
		t.Fatalf("err = %v", err)
	}
}

func TestBindSelectNamedEnumPredicates(t *testing.T) {
	def := catalog.TableDef{
		Name: "events",
		Columns: []catalog.ColumnDef{
			{Name: "status", Type: sqltype.Named("event_status"), Labels: []string{"new", "done"}},
			{Name: "event_type", Type: sqltype.Text},
			{Name: "score", Type: sqltype.Int64},
		},
	}
	col := func(name string) *ast.ColumnRef { return &ast.ColumnRef{Name: name} }
	lit := func(v ast.Value) *ast.Literal { return &ast.Literal{Value: v} }
	bin := func(c string, op ast.BinaryOp, v ast.Value) ast.Expr {
		return &ast.BinaryExpr{Left: col(c), Op: op, Right: lit(v)}
	}

	stmt := &ast.SelectStmt{
		Table:  "events",
		Select: []ast.SelectExpr{{Expr: col("status")}},
		Where:  inExpr("status", false, str("new"), str("done")),
	}
	p := mustBind(t, stmt, def)
	if p.Kind != logical.QueryScan || !p.HasFilter || p.WhereExpr == nil || p.WhereExpr.Kind != logical.ExprIn {
		t.Fatalf("plan = %#v", p)
	}

	groupPlan := mustBind(t, groupSumStmt("events", "status", "score", nil), def)
	if groupPlan.GroupType != sqltype.KindNamed {
		t.Fatalf("group plan = %#v", groupPlan)
	}

	groupHavingStmt := groupCountStmt("events", "status", nil)
	groupHavingStmt.Having = inExpr("status", false, str("new"), str("done"))
	groupHavingPlan := mustBind(t, groupHavingStmt, def)
	if !groupHavingPlan.HasHaving || !whereSimpleIn(groupHavingPlan.HavingFilterExpr, "status", sqltype.KindNamed, false, []any{"new", "done"}) {
		t.Fatalf("group having plan = %#v", groupHavingPlan)
	}

	groupHavingStmt.Having = bin("status", ast.BinaryEqual, str("missing"))
	if _, err := BindSelect(groupHavingStmt, def); err == nil || !strings.Contains(err.Error(), "invalid enum label") {
		t.Fatalf("err = %v", err)
	}

	groupHavingStmt.Having = bin("status", ast.BinaryGreater, str("new"))
	if _, err := BindSelect(groupHavingStmt, def); err == nil || !strings.Contains(err.Error(), "enum comparisons") {
		t.Fatalf("err = %v", err)
	}

	stmt.Where = bin("status", ast.BinaryGreater, str("new"))
	if _, err := BindSelect(stmt, def); err == nil || !strings.Contains(err.Error(), "enum comparisons") {
		t.Fatalf("err = %v", err)
	}
}
