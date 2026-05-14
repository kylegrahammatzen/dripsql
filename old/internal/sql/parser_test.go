package sql

import (
	"strings"
	"testing"
)

func TestParseCreateTable(t *testing.T) {
	stmt, err := ParseOne("create table if not exists events (tenant_id int64 not null, event_type text, status event_status) with (storage=columnar, segment_rows=100000, compression='auto')")
	if err != nil {
		t.Fatalf("ParseOne CREATE TABLE: %v", err)
	}
	create, ok := stmt.(*CreateTableStmt)
	if !ok || create.Name != "events" || !create.IfNotExists || len(create.Columns) != 3 || len(create.Options) != 3 {
		t.Fatalf("stmt = %#v", stmt)
	}
	if create.Columns[0].Name != "tenant_id" || create.Columns[0].Type != "int64" || !create.Columns[0].NotNull {
		t.Fatalf("columns = %#v", create.Columns)
	}
}

func TestParseCreateType(t *testing.T) {
	stmt, err := ParseOne("create type status as enum ('new', 'done')")
	if err != nil {
		t.Fatalf("ParseOne CREATE TYPE: %v", err)
	}
	create, ok := stmt.(*CreateTypeStmt)
	if !ok || create.Name != "status" || strings.Join(create.EnumLabels, ",") != "new,done" {
		t.Fatalf("stmt = %#v", stmt)
	}
}

func TestParseInsert(t *testing.T) {
	stmt, err := ParseOne("insert into events (tenant_id, event_type, active) values (1, 'signup', true), (2, 'checkout', null)")
	if err != nil {
		t.Fatalf("ParseOne INSERT: %v", err)
	}
	insert, ok := stmt.(*InsertStmt)
	if !ok || insert.Table != "events" || len(insert.Columns) != 3 || len(insert.Values) != 2 {
		t.Fatalf("stmt = %#v", stmt)
	}
	if insert.Values[0][0].Int != 1 || insert.Values[0][1].String != "signup" || !insert.Values[0][2].Bool || insert.Values[1][2].Kind != ValueNull {
		t.Fatalf("values = %#v", insert.Values)
	}
}

func TestParseSelectScan(t *testing.T) {
	stmt, err := ParseOne("select tenant_id, lower(event_type) as kind from events where tenant_id + 1 >= 10 and event_type not in ('debug') order by kind desc limit 5 offset 2")
	if err != nil {
		t.Fatalf("ParseOne SELECT: %v", err)
	}
	selectStmt, ok := stmt.(*SelectStmt)
	if !ok || selectStmt.Table != "events" || len(selectStmt.Select) != 2 || len(selectStmt.OrderBy) != 1 || selectStmt.Limit == nil || selectStmt.Offset == nil {
		t.Fatalf("stmt = %#v", stmt)
	}
	if selectStmt.Select[1].Alias != "kind" || !selectStmt.OrderBy[0].Desc || *selectStmt.Limit != 5 || *selectStmt.Offset != 2 {
		t.Fatalf("stmt = %#v", selectStmt)
	}
	if _, ok := selectStmt.Where.(*AndExpr); !ok {
		t.Fatalf("where = %#v", selectStmt.Where)
	}
}

func TestParseSelectAggregateHaving(t *testing.T) {
	stmt, err := ParseOne("select event_type as kind, count(*) as n from events group by event_type having n > 1 order by n desc")
	if err != nil {
		t.Fatalf("ParseOne aggregate SELECT: %v", err)
	}
	selectStmt, ok := stmt.(*SelectStmt)
	if !ok || len(selectStmt.Select) != 2 || len(selectStmt.GroupBy) != 1 || selectStmt.Having == nil || len(selectStmt.OrderBy) != 1 {
		t.Fatalf("stmt = %#v", stmt)
	}
	if selectStmt.Select[0].Alias != "kind" || selectStmt.Select[1].Alias != "n" {
		t.Fatalf("select = %#v", selectStmt.Select)
	}
}

func TestParseSelectMultiAggregate(t *testing.T) {
	stmt, err := ParseOne("select count(*) as n, sum(amount) as total from events")
	if err != nil {
		t.Fatalf("ParseOne multi aggregate SELECT: %v", err)
	}
	selectStmt, ok := stmt.(*SelectStmt)
	if !ok || len(selectStmt.Select) != 2 || selectStmt.Select[0].Alias != "n" || selectStmt.Select[1].Alias != "total" {
		t.Fatalf("stmt = %#v", stmt)
	}

	stmt, err = ParseOne("select country, count(*) as n, sum(amount) as total from events group by country")
	if err != nil {
		t.Fatalf("ParseOne grouped multi aggregate SELECT: %v", err)
	}
	selectStmt, ok = stmt.(*SelectStmt)
	if !ok || len(selectStmt.Select) != 3 || len(selectStmt.GroupBy) != 1 || selectStmt.Select[2].Alias != "total" {
		t.Fatalf("stmt = %#v", stmt)
	}
}

func TestParseArithmeticPrecedence(t *testing.T) {
	stmt, err := ParseOne("select tenant_id + amount * 2 as score from events")
	if err != nil {
		t.Fatalf("ParseOne arithmetic: %v", err)
	}
	selectStmt := stmt.(*SelectStmt)
	add, ok := selectStmt.Select[0].Expr.(*BinaryExpr)
	if !ok || add.Op != BinaryAdd {
		t.Fatalf("expr = %#v, want addition", selectStmt.Select[0].Expr)
	}
	mul, ok := add.Right.(*BinaryExpr)
	if !ok || mul.Op != BinaryMultiply {
		t.Fatalf("right = %#v, want multiply", add.Right)
	}
}

func TestParseParenthesesOverrideArithmeticPrecedence(t *testing.T) {
	stmt, err := ParseOne("select (tenant_id + amount) * 2 as score from events")
	if err != nil {
		t.Fatalf("ParseOne parens: %v", err)
	}
	selectStmt := stmt.(*SelectStmt)
	mul, ok := selectStmt.Select[0].Expr.(*BinaryExpr)
	if !ok || mul.Op != BinaryMultiply {
		t.Fatalf("expr = %#v, want multiply", selectStmt.Select[0].Expr)
	}
	add, ok := mul.Left.(*BinaryExpr)
	if !ok || add.Op != BinaryAdd {
		t.Fatalf("left = %#v, want addition", mul.Left)
	}
}

func TestParseModDivPrecedence(t *testing.T) {
	stmt, err := ParseOne("select tenant_id mod 4 + amount div 2 as score from events")
	if err != nil {
		t.Fatalf("ParseOne mod/div: %v", err)
	}
	selectStmt := stmt.(*SelectStmt)
	add := selectStmt.Select[0].Expr.(*BinaryExpr)
	if add.Op != BinaryAdd {
		t.Fatalf("expr op = %v, want add", add.Op)
	}
	if left := add.Left.(*BinaryExpr); left.Op != BinaryModulo {
		t.Fatalf("left op = %v, want modulo", left.Op)
	}
	if right := add.Right.(*BinaryExpr); right.Op != BinaryIntDivide {
		t.Fatalf("right op = %v, want int divide", right.Op)
	}
}

func TestParseConcatLeftAssociative(t *testing.T) {
	stmt, err := ParseOne("select event_type || '_' || country as label from events")
	if err != nil {
		t.Fatalf("ParseOne concat: %v", err)
	}
	selectStmt := stmt.(*SelectStmt)
	concat := selectStmt.Select[0].Expr.(*BinaryExpr)
	if concat.Op != BinaryConcat {
		t.Fatalf("expr op = %v, want concat", concat.Op)
	}
	left, ok := concat.Left.(*BinaryExpr)
	if !ok || left.Op != BinaryConcat {
		t.Fatalf("left = %#v, want nested concat", concat.Left)
	}
}

func TestParseBooleanPrecedence(t *testing.T) {
	stmt, err := ParseOne("select count(*) from events where active = true or tenant_id = 1 and not event_type = 'debug'")
	if err != nil {
		t.Fatalf("ParseOne boolean precedence: %v", err)
	}
	selectStmt := stmt.(*SelectStmt)
	orExpr, ok := selectStmt.Where.(*OrExpr)
	if !ok {
		t.Fatalf("where = %#v, want OR", selectStmt.Where)
	}
	andExpr, ok := orExpr.Right.(*AndExpr)
	if !ok {
		t.Fatalf("right = %#v, want AND", orExpr.Right)
	}
	if _, ok := andExpr.Right.(*NotExpr); !ok {
		t.Fatalf("and right = %#v, want NOT", andExpr.Right)
	}
}

func TestParseExplainAnalyze(t *testing.T) {
	stmt, err := ParseOne("explain analyze select count(*) from events where tenant_id = 1")
	if err != nil {
		t.Fatalf("ParseOne EXPLAIN: %v", err)
	}
	explain, ok := stmt.(*ExplainStmt)
	if !ok || !explain.Analyze || explain.Inner == nil {
		t.Fatalf("stmt = %#v", stmt)
	}
}

func TestParseRejectsExplainInsert(t *testing.T) {
	_, err := ParseOne("explain insert into events values (1)")
	if err == nil || !strings.Contains(err.Error(), "SELECT only") {
		t.Fatalf("err = %v", err)
	}
}

func TestParseRejectsExplainCreate(t *testing.T) {
	_, err := ParseOne("explain create table events (id int64)")
	if err == nil || !strings.Contains(err.Error(), "SELECT only") {
		t.Fatalf("err = %v", err)
	}
}

func TestParseRejectsMalformedSelect(t *testing.T) {
	_, err := ParseOne("select tenant_id where tenant_id = 1")
	if err == nil || !strings.Contains(err.Error(), "expected from") {
		t.Fatalf("err = %v", err)
	}
}

func TestParseRejectsNotWithoutIn(t *testing.T) {
	_, err := ParseOne("select count(*) from events where event_type not ('debug')")
	if err == nil || !strings.Contains(err.Error(), "expected IN after NOT") {
		t.Fatalf("err = %v", err)
	}
}

func TestParseRejectsOrderByLiteral(t *testing.T) {
	_, err := ParseOne("select tenant_id from events order by 1")
	if err == nil || !strings.Contains(err.Error(), "expected identifier") {
		t.Fatalf("err = %v", err)
	}
}

func TestParseSelectWithMultiOrderBy(t *testing.T) {
	stmt, err := ParseOne("select tenant_id, event_type as kind, lower(country) from events order by tenant_id desc, lower(country), tenant_id + 1")
	if err != nil {
		t.Fatalf("ParseOne multi order by: %v", err)
	}
	selectStmt, ok := stmt.(*SelectStmt)
	if !ok || len(selectStmt.OrderBy) != 3 {
		t.Fatalf("stmt = %#v", stmt)
	}
	if selectStmt.OrderBy[0].Name != "tenant_id" || !selectStmt.OrderBy[0].Desc {
		t.Fatalf("first order expr = %#v", selectStmt.OrderBy[0])
	}
	if selectStmt.OrderBy[1].Name != "" || selectStmt.OrderBy[1].Desc {
		t.Fatalf("second order expr = %#v", selectStmt.OrderBy[1])
	}
	if _, ok := selectStmt.OrderBy[1].Expr.(*FuncCall); !ok {
		t.Fatalf("second order expr type = %#v", selectStmt.OrderBy[1].Expr)
	}
	if _, ok := selectStmt.OrderBy[2].Expr.(*BinaryExpr); !ok {
		t.Fatalf("third order expr type = %#v", selectStmt.OrderBy[2].Expr)
	}
}

func TestParseRejectsMultiOrderByLiteral(t *testing.T) {
	_, err := ParseOne("select tenant_id from events order by tenant_id, 1, event_type")
	if err == nil || !strings.Contains(err.Error(), "expected identifier") {
		t.Fatalf("err = %v", err)
	}
}

func TestParseRejectsAggregateWithoutGroupBy(t *testing.T) {
	_, err := ParseOne("select country, count(*) from events")
	if err == nil || !strings.Contains(err.Error(), "requires GROUP BY") {
		t.Fatalf("err = %v", err)
	}
}
