package parser

import (
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/sql/ast"
)

func TestParseCreateType(t *testing.T) {
	stmt, err := ParseOne(`CREATE TYPE IF NOT EXISTS event_status AS ENUM ('new', 'done', 'it''s done')`)
	if err != nil {
		t.Fatalf("ParseOne: %v", err)
	}
	create, ok := stmt.(*ast.CreateTypeStmt)
	if !ok {
		t.Fatalf("stmt = %T, want *ast.CreateTypeStmt", stmt)
	}
	if create.Name != "event_status" || !create.IfNotExists {
		t.Fatalf("unexpected create type header: %#v", create)
	}
	want := []string{"new", "done", "it's done"}
	if len(create.EnumLabels) != len(want) {
		t.Fatalf("labels = %#v, want %#v", create.EnumLabels, want)
	}
	for i := range want {
		if create.EnumLabels[i] != want[i] {
			t.Fatalf("labels = %#v, want %#v", create.EnumLabels, want)
		}
	}
}

func TestParseCreateTable(t *testing.T) {
	stmt, err := ParseOne(`
		CREATE TABLE IF NOT EXISTS events (
			tenant_id INT64 NOT NULL,
			event_type TEXT NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE NOT NULL,
			id UUID,
			status event_status NOT NULL
		) WITH (
			storage = columnar,
			profile = event_analytics,
			segment_rows = 100000,
			compression = 'auto',
			column_store = true
		);
	`)
	if err != nil {
		t.Fatalf("ParseOne: %v", err)
	}
	create, ok := stmt.(*ast.CreateTableStmt)
	if !ok {
		t.Fatalf("stmt = %T, want *ast.CreateTableStmt", stmt)
	}
	if create.Name != "events" || !create.IfNotExists {
		t.Fatalf("unexpected create table header: %#v", create)
	}
	if len(create.Columns) != 5 {
		t.Fatalf("columns = %#v", create.Columns)
	}
	assertColumn(t, create.Columns[0], "tenant_id", "int64", true)
	assertColumn(t, create.Columns[1], "event_type", "text", true)
	assertColumn(t, create.Columns[2], "created_at", "timestamp with time zone", true)
	assertColumn(t, create.Columns[3], "id", "uuid", false)
	assertColumn(t, create.Columns[4], "status", "event_status", true)

	if len(create.Options) != 5 {
		t.Fatalf("options = %#v", create.Options)
	}
	assertOption(t, create.Options[0], "storage", ast.ValueIdent, "columnar", 0, false)
	assertOption(t, create.Options[1], "profile", ast.ValueIdent, "event_analytics", 0, false)
	assertOption(t, create.Options[2], "segment_rows", ast.ValueInt, "", 100000, false)
	assertOption(t, create.Options[3], "compression", ast.ValueString, "auto", 0, false)
	assertOption(t, create.Options[4], "column_store", ast.ValueBool, "", 0, true)
}

func TestParseMultipleStatements(t *testing.T) {
	stmts, err := Parse(`CREATE TYPE status AS ENUM ('new'); CREATE TABLE events (status status NOT NULL); INSERT INTO events VALUES ('new');`)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(stmts) != 3 {
		t.Fatalf("len(stmts) = %d, want 3", len(stmts))
	}
}

func TestParseInsertValues(t *testing.T) {
	stmt, err := ParseOne(`INSERT INTO events (event_type, tenant_id) VALUES ('signup', 1), ('checkout', 2)`)
	if err != nil {
		t.Fatalf("ParseOne: %v", err)
	}
	insert, ok := stmt.(*ast.InsertStmt)
	if !ok {
		t.Fatalf("stmt = %T, want *ast.InsertStmt", stmt)
	}
	if insert.Table != "events" || strings.Join(insert.Columns, ",") != "event_type,tenant_id" {
		t.Fatalf("unexpected insert target: %#v", insert)
	}
	if len(insert.Values) != 2 || len(insert.Values[0]) != 2 || len(insert.Values[1]) != 2 {
		t.Fatalf("values = %#v", insert.Values)
	}
	if insert.Values[0][0].Kind != ast.ValueString || insert.Values[0][0].String != "signup" {
		t.Fatalf("first string literal = %#v", insert.Values[0][0])
	}
	if insert.Values[0][1].Kind != ast.ValueInt || insert.Values[0][1].Int != 1 {
		t.Fatalf("first int literal = %#v", insert.Values[0][1])
	}
}

func TestParseFloatLiterals(t *testing.T) {
	stmt, err := ParseOne(`INSERT INTO metrics VALUES (1.5, 1e3, 1.5e-2)`)
	if err != nil {
		t.Fatalf("ParseOne insert: %v", err)
	}
	insert, ok := stmt.(*ast.InsertStmt)
	if !ok {
		t.Fatalf("stmt = %T, want *ast.InsertStmt", stmt)
	}
	if got := insert.Values[0][0]; got.Kind != ast.ValueFloat || got.Float != 1.5 {
		t.Fatalf("first float literal = %#v", got)
	}
	if got := insert.Values[0][1]; got.Kind != ast.ValueFloat || got.Float != 1000 {
		t.Fatalf("exponent float literal = %#v", got)
	}
	if got := insert.Values[0][2]; got.Kind != ast.ValueFloat || got.Float != 0.015 {
		t.Fatalf("fraction exponent float literal = %#v", got)
	}

	stmt, err = ParseOne(`SELECT 1.5 AS ratio FROM metrics WHERE score >= 2.25`)
	if err != nil {
		t.Fatalf("ParseOne select: %v", err)
	}
	selectStmt, ok := stmt.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("stmt = %T, want *ast.SelectStmt", stmt)
	}
	selectLit, ok := selectStmt.Select[0].Expr.(*ast.Literal)
	if !ok || selectLit.Value.Kind != ast.ValueFloat || selectLit.Value.Float != 1.5 {
		t.Fatalf("select float literal = %#v", selectStmt.Select[0].Expr)
	}
	where, ok := selectStmt.Where.(*ast.BinaryExpr)
	if !ok {
		t.Fatalf("where = %T, want *ast.BinaryExpr", selectStmt.Where)
	}
	whereLit, ok := where.Right.(*ast.Literal)
	if !ok || whereLit.Value.Kind != ast.ValueFloat || whereLit.Value.Float != 2.25 {
		t.Fatalf("where float literal = %#v", where.Right)
	}
}

func TestParseSelect(t *testing.T) {
	tests := []struct {
		sql           string
		table         string
		groupColumn   string
		whereColumn   string
		whereBetween  bool
		countColumn   string
		sumColumn     string
		minColumn     string
		maxColumn     string
		aggAlias      string
		groupAlias    string
		havingColumn  string
		havingFunc    string
		havingBetween bool
		orderColumn   string
		orderDesc     bool
		limit         *int64
		offset        *int64
	}{
		{sql: `SELECT count(*) FROM events`, table: "events"},
		{sql: `SELECT count(event_type) FROM events`, table: "events", countColumn: "event_type"},
		{sql: `SELECT sum(tenant_id) FROM events`, table: "events", sumColumn: "tenant_id"},
		{sql: `SELECT min(tenant_id) FROM events`, table: "events", minColumn: "tenant_id"},
		{sql: `SELECT max(tenant_id) FROM events`, table: "events", maxColumn: "tenant_id"},
		{sql: `SELECT sum(tenant_id) AS total FROM events`, table: "events", sumColumn: "tenant_id", aggAlias: "total"},
		{sql: `SELECT sum(tenant_id) total FROM events`, table: "events", sumColumn: "tenant_id", aggAlias: "total"},
		{sql: `SELECT count(*) FROM events HAVING count(*) > 0`, table: "events", havingFunc: "count"},
		{sql: `SELECT sum(tenant_id) total FROM events HAVING total >= 10`, table: "events", sumColumn: "tenant_id", aggAlias: "total", havingColumn: "total"},
		{sql: `SELECT count(*) FROM events WHERE tenant_id = 1`, table: "events", whereColumn: "tenant_id"},
		{sql: `SELECT count(*) FROM events WHERE tenant_id != 1`, table: "events", whereColumn: "tenant_id"},
		{sql: `SELECT count(*) FROM events WHERE tenant_id <> 1`, table: "events", whereColumn: "tenant_id"},
		{sql: `SELECT count(*) FROM events WHERE tenant_id < 3`, table: "events", whereColumn: "tenant_id"},
		{sql: `SELECT count(*) FROM events WHERE tenant_id <= 3`, table: "events", whereColumn: "tenant_id"},
		{sql: `SELECT count(*) FROM events WHERE tenant_id > 3`, table: "events", whereColumn: "tenant_id"},
		{sql: `SELECT count(*) FROM events WHERE tenant_id >= 3`, table: "events", whereColumn: "tenant_id"},
		{sql: `SELECT count(*) FROM events WHERE tenant_id BETWEEN 1 AND 3`, table: "events", whereColumn: "tenant_id", whereBetween: true},
		{sql: `SELECT count(*) FROM events WHERE tenant_id IN (1, 3)`, table: "events", whereColumn: "tenant_id"},
		{sql: `SELECT count(*) FROM events WHERE tenant_id NOT IN (1, 3)`, table: "events", whereColumn: "tenant_id"},
		{sql: `SELECT count(*) FROM events WHERE tenant_id = 1 AND event_type = 'signup'`, table: "events", whereColumn: "tenant_id"},
		{sql: `SELECT event_type, count(*) FROM events GROUP BY event_type`, table: "events", groupColumn: "event_type"},
		{sql: `SELECT event_type, count(tenant_id) FROM events GROUP BY event_type`, table: "events", groupColumn: "event_type", countColumn: "tenant_id"},
		{sql: `SELECT event_type, sum(tenant_id) FROM events GROUP BY event_type`, table: "events", groupColumn: "event_type", sumColumn: "tenant_id"},
		{sql: `SELECT event_type, min(tenant_id) FROM events GROUP BY event_type`, table: "events", groupColumn: "event_type", minColumn: "tenant_id"},
		{sql: `SELECT event_type, max(tenant_id) FROM events GROUP BY event_type`, table: "events", groupColumn: "event_type", maxColumn: "tenant_id"},
		{sql: `SELECT event_type AS kind, sum(tenant_id) AS total FROM events GROUP BY event_type`, table: "events", groupColumn: "event_type", sumColumn: "tenant_id", groupAlias: "kind", aggAlias: "total"},
		{sql: `SELECT event_type kind, sum(tenant_id) total FROM events GROUP BY event_type`, table: "events", groupColumn: "event_type", sumColumn: "tenant_id", groupAlias: "kind", aggAlias: "total"},
		{sql: `SELECT event_type AS kind, sum(tenant_id) AS total FROM events GROUP BY event_type HAVING total >= 10`, table: "events", groupColumn: "event_type", sumColumn: "tenant_id", groupAlias: "kind", aggAlias: "total", havingColumn: "total"},
		{sql: `SELECT event_type, sum(tenant_id) FROM events GROUP BY event_type HAVING sum BETWEEN 1 AND 10`, table: "events", groupColumn: "event_type", sumColumn: "tenant_id", havingColumn: "sum", havingBetween: true},
		{sql: `SELECT event_type, count(*) FROM events GROUP BY event_type HAVING count IN (1, 2)`, table: "events", groupColumn: "event_type", havingColumn: "count"},
		{sql: `SELECT event_type, count(*) FROM events GROUP BY event_type HAVING count(*) > 1`, table: "events", groupColumn: "event_type", havingFunc: "count"},
		{sql: `SELECT event_type, sum(tenant_id) FROM events GROUP BY event_type HAVING sum(tenant_id) >= 10`, table: "events", groupColumn: "event_type", sumColumn: "tenant_id", havingFunc: "sum"},
		{sql: `SELECT event_type AS kind, count(*) FROM events GROUP BY event_type HAVING kind IN ('signup', 'login')`, table: "events", groupColumn: "event_type", groupAlias: "kind", havingColumn: "kind"},
		{sql: `SELECT event_type AS kind, sum(tenant_id) AS total FROM events GROUP BY event_type ORDER BY total DESC`, table: "events", groupColumn: "event_type", sumColumn: "tenant_id", groupAlias: "kind", aggAlias: "total", orderColumn: "total", orderDesc: true},
		{sql: `SELECT event_type AS kind, sum(tenant_id) AS total FROM events GROUP BY event_type ORDER BY total DESC LIMIT 10`, table: "events", groupColumn: "event_type", sumColumn: "tenant_id", groupAlias: "kind", aggAlias: "total", orderColumn: "total", orderDesc: true, limit: int64Ptr(10)},
		{sql: `SELECT event_type AS kind, sum(tenant_id) AS total FROM events GROUP BY event_type ORDER BY total DESC LIMIT 0`, table: "events", groupColumn: "event_type", sumColumn: "tenant_id", groupAlias: "kind", aggAlias: "total", orderColumn: "total", orderDesc: true, limit: int64Ptr(0)},
		{sql: `SELECT event_type AS kind, sum(tenant_id) AS total FROM events GROUP BY event_type ORDER BY total DESC LIMIT 10 OFFSET 5`, table: "events", groupColumn: "event_type", sumColumn: "tenant_id", groupAlias: "kind", aggAlias: "total", orderColumn: "total", orderDesc: true, limit: int64Ptr(10), offset: int64Ptr(5)},
		{sql: `SELECT event_type AS kind, sum(tenant_id) AS total FROM events GROUP BY event_type ORDER BY total DESC OFFSET 5`, table: "events", groupColumn: "event_type", sumColumn: "tenant_id", groupAlias: "kind", aggAlias: "total", orderColumn: "total", orderDesc: true, offset: int64Ptr(5)},
		{sql: `SELECT event_type AS kind, sum(tenant_id) AS total FROM events GROUP BY event_type OFFSET 5`, table: "events", groupColumn: "event_type", sumColumn: "tenant_id", groupAlias: "kind", aggAlias: "total", offset: int64Ptr(5)},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			stmt, err := ParseOne(tt.sql)
			if err != nil {
				t.Fatalf("ParseOne: %v", err)
			}
			selectStmt, ok := stmt.(*ast.SelectStmt)
			if !ok {
				t.Fatalf("stmt = %T, want *ast.SelectStmt", stmt)
			}
			if selectStmt.Table != tt.table || countColumnName(selectStmt) != tt.countColumn || aggregateColumnName(selectStmt, "sum") != tt.sumColumn || aggregateColumnName(selectStmt, "min") != tt.minColumn || aggregateColumnName(selectStmt, "max") != tt.maxColumn || (tt.countColumn == "" && tt.sumColumn == "" && tt.minColumn == "" && tt.maxColumn == "" && !selectHasCountStar(selectStmt)) || groupColumnName(selectStmt) != tt.groupColumn {
				t.Fatalf("select = %#v", selectStmt)
			}
			if tt.aggAlias != "" && selectStmt.Select[len(selectStmt.Select)-1].Alias != tt.aggAlias {
				t.Fatalf("aggregate alias = %q, want %q", selectStmt.Select[len(selectStmt.Select)-1].Alias, tt.aggAlias)
			}
			if tt.groupAlias != "" && selectStmt.Select[0].Alias != tt.groupAlias {
				t.Fatalf("group alias = %q, want %q", selectStmt.Select[0].Alias, tt.groupAlias)
			}
			if tt.havingColumn == "" && tt.havingFunc == "" && selectStmt.Having != nil {
				t.Fatalf("having = %#v, want nil", selectStmt.Having)
			}
			if tt.havingColumn != "" && whereColumnName(selectStmt.Having) != tt.havingColumn {
				t.Fatalf("having = %#v, want column %q", selectStmt.Having, tt.havingColumn)
			}
			if tt.havingFunc != "" && havingFuncName(selectStmt.Having) != tt.havingFunc {
				t.Fatalf("having = %#v, want function %q", selectStmt.Having, tt.havingFunc)
			}
			if tt.havingColumn != "" && isBetweenExpr(selectStmt.Having) != tt.havingBetween {
				t.Fatalf("having between = %v, want %v", isBetweenExpr(selectStmt.Having), tt.havingBetween)
			}
			if tt.orderColumn != "" {
				if len(selectStmt.OrderBy) != 1 || selectStmt.OrderBy[0].Name != tt.orderColumn || selectStmt.OrderBy[0].Desc != tt.orderDesc {
					t.Fatalf("order by = %#v, want %s desc=%v", selectStmt.OrderBy, tt.orderColumn, tt.orderDesc)
				}
			}
			if (selectStmt.Limit == nil) != (tt.limit == nil) || (tt.limit != nil && *selectStmt.Limit != *tt.limit) {
				t.Fatalf("limit = %#v, want %#v", selectStmt.Limit, tt.limit)
			}
			if (selectStmt.Offset == nil) != (tt.offset == nil) || (tt.offset != nil && *selectStmt.Offset != *tt.offset) {
				t.Fatalf("offset = %#v, want %#v", selectStmt.Offset, tt.offset)
			}
			if tt.whereColumn == "" && selectStmt.Where != nil {
				t.Fatalf("where = %#v, want nil", selectStmt.Where)
			}
			if tt.whereColumn != "" && whereColumnName(selectStmt.Where) != tt.whereColumn {
				t.Fatalf("where = %#v, want column %q", selectStmt.Where, tt.whereColumn)
			}
			if tt.whereColumn != "" && isBetweenExpr(selectStmt.Where) != tt.whereBetween {
				t.Fatalf("where between = %v, want %v", isBetweenExpr(selectStmt.Where), tt.whereBetween)
			}
		})
	}
}

func TestParseSelectStar(t *testing.T) {
	stmt, err := ParseOne(`SELECT * FROM events WHERE tenant_id = 1 ORDER BY event_type DESC LIMIT 2`)
	if err != nil {
		t.Fatalf("ParseOne: %v", err)
	}
	selectStmt, ok := stmt.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("stmt = %T, want *ast.SelectStmt", stmt)
	}
	if selectStmt.Table != "events" || len(selectStmt.Select) != 1 {
		t.Fatalf("select = %#v", selectStmt)
	}
	if _, ok := selectStmt.Select[0].Expr.(*ast.StarRef); !ok {
		t.Fatalf("select expr = %T, want *ast.StarRef", selectStmt.Select[0].Expr)
	}
	if whereColumnName(selectStmt.Where) != "tenant_id" {
		t.Fatalf("where = %#v", selectStmt.Where)
	}
	if len(selectStmt.OrderBy) != 1 || selectStmt.OrderBy[0].Name != "event_type" || !selectStmt.OrderBy[0].Desc {
		t.Fatalf("order by = %#v", selectStmt.OrderBy)
	}
	if selectStmt.Limit == nil || *selectStmt.Limit != 2 {
		t.Fatalf("limit = %#v, want 2", selectStmt.Limit)
	}
}

func TestParseSelectLiterals(t *testing.T) {
	stmt, err := ParseOne(`SELECT tenant_id, 'active' AS label, 1 AS version, true AS ok, NULL AS missing FROM events`)
	if err != nil {
		t.Fatalf("ParseOne: %v", err)
	}
	selectStmt, ok := stmt.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("stmt = %T, want *ast.SelectStmt", stmt)
	}
	if len(selectStmt.Select) != 5 {
		t.Fatalf("select len = %d, want 5", len(selectStmt.Select))
	}
	if _, ok := selectStmt.Select[0].Expr.(*ast.ColumnRef); !ok {
		t.Fatalf("first expr = %T, want *ast.ColumnRef", selectStmt.Select[0].Expr)
	}
	for i, alias := range []string{"label", "version", "ok", "missing"} {
		lit, ok := selectStmt.Select[i+1].Expr.(*ast.Literal)
		if !ok {
			t.Fatalf("expr %d = %T, want *ast.Literal", i+1, selectStmt.Select[i+1].Expr)
		}
		if selectStmt.Select[i+1].Alias != alias {
			t.Fatalf("alias %d = %q, want %q", i+1, selectStmt.Select[i+1].Alias, alias)
		}
		if lit.Value.Kind == 0 && alias != "label" {
			t.Fatalf("literal %d = %#v", i+1, lit.Value)
		}
	}
}

func TestParseSelectComputedExpression(t *testing.T) {
	stmt, err := ParseOne(`SELECT tenant_id, score + 1 AS next_score, 100 - score AS remaining, score + tenant_id AS total FROM events`)
	if err != nil {
		t.Fatalf("ParseOne: %v", err)
	}
	selectStmt, ok := stmt.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("stmt = %T, want *ast.SelectStmt", stmt)
	}
	if len(selectStmt.Select) != 4 {
		t.Fatalf("select len = %d, want 4", len(selectStmt.Select))
	}
	add, ok := selectStmt.Select[1].Expr.(*ast.BinaryExpr)
	if !ok || add.Op != ast.BinaryAdd || selectStmt.Select[1].Alias != "next_score" {
		t.Fatalf("add expr = %#v alias=%q", selectStmt.Select[1].Expr, selectStmt.Select[1].Alias)
	}
	sub, ok := selectStmt.Select[2].Expr.(*ast.BinaryExpr)
	if !ok || sub.Op != ast.BinarySubtract || selectStmt.Select[2].Alias != "remaining" {
		t.Fatalf("sub expr = %#v alias=%q", selectStmt.Select[2].Expr, selectStmt.Select[2].Alias)
	}
	col, ok := selectStmt.Select[3].Expr.(*ast.BinaryExpr)
	if !ok || col.Op != ast.BinaryAdd || selectStmt.Select[3].Alias != "total" {
		t.Fatalf("column expr = %#v alias=%q", selectStmt.Select[3].Expr, selectStmt.Select[3].Alias)
	}
}

func TestParseSelectArithmeticPrecedence(t *testing.T) {
	stmt, err := ParseOne(`SELECT (5 * 2 - 6) / 2 AS result, 11 % 5 AS remainder, 11 MOD 5 AS modded, 10 DIV 4 AS quotient FROM events`)
	if err != nil {
		t.Fatalf("ParseOne: %v", err)
	}
	selectStmt, ok := stmt.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("stmt = %T, want *ast.SelectStmt", stmt)
	}
	if len(selectStmt.Select) != 4 {
		t.Fatalf("select len = %d, want 4", len(selectStmt.Select))
	}
	result, ok := selectStmt.Select[0].Expr.(*ast.BinaryExpr)
	if !ok || result.Op != ast.BinaryDivide || selectStmt.Select[0].Alias != "result" {
		t.Fatalf("result expr = %#v alias=%q", selectStmt.Select[0].Expr, selectStmt.Select[0].Alias)
	}
	left, ok := result.Left.(*ast.BinaryExpr)
	if !ok || left.Op != ast.BinarySubtract {
		t.Fatalf("result left = %#v, want subtraction", result.Left)
	}
	product, ok := left.Left.(*ast.BinaryExpr)
	if !ok || product.Op != ast.BinaryMultiply {
		t.Fatalf("subtraction left = %#v, want multiplication", left.Left)
	}
	remainder, ok := selectStmt.Select[1].Expr.(*ast.BinaryExpr)
	if !ok || remainder.Op != ast.BinaryModulo || selectStmt.Select[1].Alias != "remainder" {
		t.Fatalf("remainder expr = %#v alias=%q", selectStmt.Select[1].Expr, selectStmt.Select[1].Alias)
	}
	modded, ok := selectStmt.Select[2].Expr.(*ast.BinaryExpr)
	if !ok || modded.Op != ast.BinaryModulo || selectStmt.Select[2].Alias != "modded" {
		t.Fatalf("modded expr = %#v alias=%q", selectStmt.Select[2].Expr, selectStmt.Select[2].Alias)
	}
	quotient, ok := selectStmt.Select[3].Expr.(*ast.BinaryExpr)
	if !ok || quotient.Op != ast.BinaryIntDivide || selectStmt.Select[3].Alias != "quotient" {
		t.Fatalf("quotient expr = %#v alias=%q", selectStmt.Select[3].Expr, selectStmt.Select[3].Alias)
	}
}

func TestParseWhereArithmeticExpression(t *testing.T) {
	stmt, err := ParseOne(`SELECT count(*) FROM events WHERE (score * 2 + tenant_id) >= 21 AND 10 DIV tenant_id = 5`)
	if err != nil {
		t.Fatalf("ParseOne: %v", err)
	}
	selectStmt, ok := stmt.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("stmt = %T, want *ast.SelectStmt", stmt)
	}
	where, ok := selectStmt.Where.(*ast.AndExpr)
	if !ok {
		t.Fatalf("where = %T, want *ast.AndExpr", selectStmt.Where)
	}
	left, ok := where.Left.(*ast.BinaryExpr)
	if !ok || left.Op != ast.BinaryGreaterEqual {
		t.Fatalf("left where = %#v, want >=", where.Left)
	}
	leftExpr, ok := left.Left.(*ast.BinaryExpr)
	if !ok || leftExpr.Op != ast.BinaryAdd {
		t.Fatalf("left comparison operand = %#v, want addition", left.Left)
	}
	product, ok := leftExpr.Left.(*ast.BinaryExpr)
	if !ok || product.Op != ast.BinaryMultiply {
		t.Fatalf("addition left = %#v, want multiplication", leftExpr.Left)
	}
	right, ok := where.Right.(*ast.BinaryExpr)
	if !ok || right.Op != ast.BinaryEqual {
		t.Fatalf("right where = %#v, want equality", where.Right)
	}
	quotient, ok := right.Left.(*ast.BinaryExpr)
	if !ok || quotient.Op != ast.BinaryIntDivide {
		t.Fatalf("right comparison operand = %#v, want DIV", right.Left)
	}
}

func int64Ptr(value int64) *int64 { return &value }

func aggregateColumnName(stmt *ast.SelectStmt, name string) string {
	if len(stmt.Select) == 2 {
		call, ok := stmt.Select[1].Expr.(*ast.FuncCall)
		if !ok || call.Name != name || call.Star || len(call.Args) != 1 {
			return ""
		}
		col, ok := call.Args[0].(*ast.ColumnRef)
		if !ok {
			return ""
		}
		return col.Name
	}
	if len(stmt.Select) != 1 {
		return ""
	}
	call, ok := stmt.Select[0].Expr.(*ast.FuncCall)
	if !ok || call.Name != name || call.Star || len(call.Args) != 1 {
		return ""
	}
	col, ok := call.Args[0].(*ast.ColumnRef)
	if !ok {
		return ""
	}
	return col.Name
}

func countColumnName(stmt *ast.SelectStmt) string {
	if len(stmt.Select) == 2 {
		call, ok := stmt.Select[1].Expr.(*ast.FuncCall)
		if !ok || call.Name != "count" || call.Star || len(call.Args) != 1 {
			return ""
		}
		col, ok := call.Args[0].(*ast.ColumnRef)
		if !ok {
			return ""
		}
		return col.Name
	}
	if len(stmt.Select) != 1 {
		return ""
	}
	call, ok := stmt.Select[0].Expr.(*ast.FuncCall)
	if !ok || call.Name != "count" || call.Star || len(call.Args) != 1 {
		return ""
	}
	col, ok := call.Args[0].(*ast.ColumnRef)
	if !ok {
		return ""
	}
	return col.Name
}

func selectHasCountStar(stmt *ast.SelectStmt) bool {
	if len(stmt.Select) == 1 {
		call, ok := stmt.Select[0].Expr.(*ast.FuncCall)
		return ok && call.Name == "count" && call.Star
	}
	if len(stmt.Select) == 2 {
		call, ok := stmt.Select[1].Expr.(*ast.FuncCall)
		return ok && call.Name == "count" && call.Star
	}
	return false
}

func groupColumnName(stmt *ast.SelectStmt) string {
	if len(stmt.GroupBy) == 0 {
		return ""
	}
	col, ok := stmt.GroupBy[0].(*ast.ColumnRef)
	if !ok {
		return ""
	}
	return col.Name
}

func whereColumnName(expr ast.Expr) string {
	if and, ok := expr.(*ast.AndExpr); ok {
		return whereColumnName(and.Left)
	}
	switch expr := expr.(type) {
	case *ast.BinaryExpr:
		col, _ := expr.Left.(*ast.ColumnRef)
		if col == nil {
			return ""
		}
		return col.Name
	case *ast.BetweenExpr:
		col, _ := expr.Expr.(*ast.ColumnRef)
		if col == nil {
			return ""
		}
		return col.Name
	case *ast.InExpr:
		col, _ := expr.Expr.(*ast.ColumnRef)
		if col == nil {
			return ""
		}
		return col.Name
	default:
		return ""
	}
}

func havingFuncName(expr ast.Expr) string {
	if and, ok := expr.(*ast.AndExpr); ok {
		if name := havingFuncName(and.Left); name != "" {
			return name
		}
		return havingFuncName(and.Right)
	}
	switch expr := expr.(type) {
	case *ast.BinaryExpr:
		call, _ := expr.Left.(*ast.FuncCall)
		if call == nil {
			return ""
		}
		return call.Name
	case *ast.BetweenExpr:
		call, _ := expr.Expr.(*ast.FuncCall)
		if call == nil {
			return ""
		}
		return call.Name
	case *ast.InExpr:
		call, _ := expr.Expr.(*ast.FuncCall)
		if call == nil {
			return ""
		}
		return call.Name
	default:
		return ""
	}
}

func isBetweenExpr(expr ast.Expr) bool {
	_, ok := expr.(*ast.BetweenExpr)
	return ok
}

func TestParseRejectsUnsupportedDDL(t *testing.T) {
	tests := []struct {
		sql  string
		want string
	}{
		{sql: `CREATE TYPE bad AS (id INT64)`, want: "unsupported CREATE TYPE"},
		{sql: `CREATE TABLE events (id INT64 PRIMARY KEY)`, want: "unsupported column constraint"},
		{sql: `CREATE TABLE events (id INT64, PRIMARY KEY (id))`, want: "unsupported CREATE TABLE constraint"},
		{sql: `CREATE TABLE events (id INT64) USING columnar`, want: "expected statement end"},
		{sql: `INSERT INTO events SELECT 1`, want: "expected values"},
		{sql: `SELECT event_type, count(*) FROM events`, want: "requires GROUP BY"},
		{sql: `SELECT count(*) FROM events ORDER BY 1`, want: "expected identifier"},
		{sql: `SELECT count(*) FROM events LIMIT score`, want: "expected integer literal"},
		{sql: `SELECT count(*) FROM events LIMIT '10'`, want: "expected integer literal"},
		{sql: `SELECT count(*) FROM events LIMIT -1`, want: "expected integer literal"},
		{sql: `SELECT count(*) FROM events OFFSET score`, want: "expected integer literal"},
		{sql: `SELECT count(*) FROM events OFFSET '10'`, want: "expected integer literal"},
		{sql: `SELECT count(*) FROM events OFFSET -1`, want: "expected integer literal"},
	}
	for _, tt := range tests {
		_, err := ParseOne(tt.sql)
		if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tt.want)) {
			t.Fatalf("ParseOne(%q) error = %v, want containing %q", tt.sql, err, tt.want)
		}
	}
}

func assertColumn(t *testing.T, got ast.ColumnDef, name string, typ string, notNull bool) {
	t.Helper()
	if got.Name != name || got.Type != typ || got.NotNull != notNull {
		t.Fatalf("column = %#v, want name=%q type=%q notNull=%v", got, name, typ, notNull)
	}
}

func assertOption(t *testing.T, got ast.TableOption, name string, kind ast.ValueKind, value string, intValue int64, boolValue bool) {
	t.Helper()
	if got.Name != name || got.Value.Kind != kind || got.Value.String != value || got.Value.Int != intValue || got.Value.Bool != boolValue {
		t.Fatalf("option = %#v, want name=%q kind=%v value=%q int=%d bool=%v", got, name, kind, value, intValue, boolValue)
	}
}
