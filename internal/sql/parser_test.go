// Parser tests for every statement kind the lexer plus parser admit.
// SELECT projection plus WHERE plus GROUP BY plus ORDER BY plus LIMIT plus scalar precedence, INSERT VALUES shapes, CREATE TYPE plus CREATE TABLE plus options, EXPLAIN inner gating, WITH CTE binding shape.
package sql

import "testing"

func parseSelect(t *testing.T, src string) *SelectStmt {
	t.Helper()
	stmt, err := ParseOne(src)
	if err != nil {
		t.Fatalf("ParseOne(%q): %v", src, err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("ParseOne(%q) = %T, want *SelectStmt", src, stmt)
	}
	return sel
}

func parseInsert(t *testing.T, src string) *InsertStmt {
	t.Helper()
	stmt, err := ParseOne(src)
	if err != nil {
		t.Fatalf("ParseOne(%q): %v", src, err)
	}
	s, ok := stmt.(*InsertStmt)
	if !ok {
		t.Fatalf("got %T, want *InsertStmt", stmt)
	}
	return s
}

func parseCreateType(t *testing.T, src string) *CreateTypeStmt {
	t.Helper()
	stmt, err := ParseOne(src)
	if err != nil {
		t.Fatalf("ParseOne(%q): %v", src, err)
	}
	s, ok := stmt.(*CreateTypeStmt)
	if !ok {
		t.Fatalf("got %T, want *CreateTypeStmt", stmt)
	}
	return s
}

func parseCreateTable(t *testing.T, src string) *CreateTableStmt {
	t.Helper()
	stmt, err := ParseOne(src)
	if err != nil {
		t.Fatalf("ParseOne(%q): %v", src, err)
	}
	s, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("got %T, want *CreateTableStmt", stmt)
	}
	return s
}

func TestSelect_StarProjection(t *testing.T) {
	s := parseSelect(t, "SELECT * FROM t")
	tn, ok := s.From.(*TableName)
	if !ok || tn.Name != "t" {
		t.Fatalf("From = %+v, want TableName{t}", s.From)
	}
	if len(s.Select) != 1 {
		t.Fatalf("Select len = %d", len(s.Select))
	}
	if _, ok := s.Select[0].Expr.(*StarRef); !ok {
		t.Fatalf("Select[0] = %T", s.Select[0].Expr)
	}
}

func TestSelect_ColumnList_WithAlias(t *testing.T) {
	s := parseSelect(t, "SELECT id, name AS n, age years FROM users")
	if len(s.Select) != 3 {
		t.Fatalf("got %d cols", len(s.Select))
	}
	if r, _ := s.Select[0].Expr.(*ColumnRef); r == nil || r.Name != "id" || s.Select[0].Alias != "" {
		t.Fatalf("col 0: %+v alias=%q", s.Select[0].Expr, s.Select[0].Alias)
	}
	if s.Select[1].Alias != "n" {
		t.Fatalf("col 1 alias = %q, want n", s.Select[1].Alias)
	}
	if s.Select[2].Alias != "years" {
		t.Fatalf("col 2 alias = %q, want years", s.Select[2].Alias)
	}
}

func TestSelect_Where_AndOrPrecedence(t *testing.T) {
	s := parseSelect(t, "SELECT id FROM t WHERE a = 1 AND b = 2 OR c = 3")
	or, ok := s.Where.(*OrExpr)
	if !ok {
		t.Fatalf("WHERE root = %T, want *OrExpr (AND binds tighter)", s.Where)
	}
	if _, ok := or.Left.(*AndExpr); !ok {
		t.Fatalf("OR.Left = %T, want *AndExpr", or.Left)
	}
}

func TestSelect_Where_NotAndParens(t *testing.T) {
	s := parseSelect(t, "SELECT id FROM t WHERE NOT (a = 1 OR b = 2)")
	not, ok := s.Where.(*NotExpr)
	if !ok {
		t.Fatalf("root = %T, want *NotExpr", s.Where)
	}
	if _, ok := not.Expr.(*OrExpr); !ok {
		t.Fatalf("NOT child = %T, want *OrExpr", not.Expr)
	}
}

func TestSelect_Where_BetweenAndIn(t *testing.T) {
	s := parseSelect(t, "SELECT id FROM t WHERE id BETWEEN 1 AND 10")
	if _, ok := s.Where.(*BetweenExpr); !ok {
		t.Fatalf("Where = %T, want *BetweenExpr", s.Where)
	}
	s = parseSelect(t, "SELECT id FROM t WHERE id IN (1, 2, 3)")
	in, ok := s.Where.(*InExpr)
	if !ok || in.Not || len(in.Values) != 3 {
		t.Fatalf("In = %+v", s.Where)
	}
	s = parseSelect(t, "SELECT id FROM t WHERE id NOT IN (1)")
	in, _ = s.Where.(*InExpr)
	if in == nil || !in.Not {
		t.Fatalf("NOT IN: %+v", s.Where)
	}
}

func TestSelect_Comparisons(t *testing.T) {
	for src, want := range map[string]BinaryOp{
		"SELECT id FROM t WHERE a = 1":  BinaryEqual,
		"SELECT id FROM t WHERE a != 1": BinaryNotEqual,
		"SELECT id FROM t WHERE a <> 1": BinaryNotEqual,
		"SELECT id FROM t WHERE a < 1":  BinaryLess,
		"SELECT id FROM t WHERE a <= 1": BinaryLessEqual,
		"SELECT id FROM t WHERE a > 1":  BinaryGreater,
		"SELECT id FROM t WHERE a >= 1": BinaryGreaterEqual,
	} {
		s := parseSelect(t, src)
		b, ok := s.Where.(*BinaryExpr)
		if !ok || b.Op != want {
			t.Fatalf("%q -> %+v, want op %d", src, s.Where, want)
		}
	}
}

func TestSelect_GroupBy_ParsedWhenPresent(t *testing.T) {
	s := parseSelect(t, "SELECT category, sum(price) FROM sales GROUP BY category")
	if len(s.GroupBy) != 1 {
		t.Fatalf("GroupBy = %v", s.GroupBy)
	}
}

func TestSelect_AggregateMix_AcceptedAtParse(t *testing.T) {
	if _, err := ParseOne("SELECT category, sum(price) FROM sales"); err != nil {
		t.Fatalf("parser must not enforce GROUP BY (binder will): %v", err)
	}
	if _, err := ParseOne("SELECT sum(price), category FROM sales"); err != nil {
		t.Fatalf("aggregate/non-aggregate order must not matter at parse: %v", err)
	}
}

func TestSelect_GroupBy_MultipleKeys(t *testing.T) {
	s := parseSelect(t, "SELECT a, b, sum(c) FROM t GROUP BY a, b")
	if len(s.GroupBy) != 2 {
		t.Fatalf("GroupBy len = %d", len(s.GroupBy))
	}
}

func TestSelect_AggregateOverExpression(t *testing.T) {
	s := parseSelect(t, "SELECT sum(price * qty) FROM t")
	fc, ok := s.Select[0].Expr.(*FuncCall)
	if !ok || fc.Name != "sum" || len(fc.Args) != 1 {
		t.Fatalf("FuncCall = %+v", s.Select[0].Expr)
	}
	if _, ok := fc.Args[0].(*BinaryExpr); !ok {
		t.Fatalf("aggregate arg = %T, want *BinaryExpr", fc.Args[0])
	}
}

func TestSelect_QuotedTrueIsColumn(t *testing.T) {
	s := parseSelect(t, `SELECT "true" FROM t`)
	c, ok := s.Select[0].Expr.(*ColumnRef)
	if !ok || c.Name != "true" {
		t.Fatalf("expected ColumnRef{Name:\"true\"}, got %+v", s.Select[0].Expr)
	}
}

func TestSelect_NegativeLiteral_InWhere(t *testing.T) {
	s := parseSelect(t, "SELECT id FROM t WHERE id = -42")
	b, ok := s.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("Where = %T", s.Where)
	}
	lit, ok := b.Right.(*Literal)
	if !ok || lit.Value.Kind != ValueInt || lit.Value.Int != -42 {
		t.Fatalf("Right = %+v", b.Right)
	}
}

func TestSelect_Having(t *testing.T) {
	s := parseSelect(t, "SELECT category, sum(price) FROM sales GROUP BY category HAVING sum(price) > 100")
	if s.Having == nil {
		t.Fatal("Having missing")
	}
	if _, ok := s.Having.(*BinaryExpr); !ok {
		t.Fatalf("Having = %T", s.Having)
	}
}

func TestSelect_OrderBy_DescAndMulti(t *testing.T) {
	s := parseSelect(t, "SELECT id FROM t ORDER BY a DESC, b")
	if len(s.OrderBy) != 2 {
		t.Fatalf("OrderBy len = %d", len(s.OrderBy))
	}
	if !s.OrderBy[0].Desc {
		t.Fatal("OrderBy[0] not desc")
	}
	if s.OrderBy[1].Desc {
		t.Fatal("OrderBy[1] should default asc")
	}
}

func TestSelect_OrderBy_RejectsLiteralOnly(t *testing.T) {
	if _, err := ParseOne("SELECT id FROM t ORDER BY 1"); err == nil {
		t.Fatal("ORDER BY literal must error")
	}
}

func TestSelect_LimitOffset(t *testing.T) {
	s := parseSelect(t, "SELECT id FROM t LIMIT 10 OFFSET 20")
	if s.Limit == nil || *s.Limit != 10 {
		t.Fatalf("Limit = %v", s.Limit)
	}
	if s.Offset == nil || *s.Offset != 20 {
		t.Fatalf("Offset = %v", s.Offset)
	}
}

func TestSelect_Scalar_TermBindsTighterThanExpr(t *testing.T) {
	s := parseSelect(t, "SELECT a + b * c FROM t")
	root := s.Select[0].Expr.(*BinaryExpr)
	if root.Op != BinaryAdd {
		t.Fatalf("root op = %d, want Add", root.Op)
	}
	rightMul, ok := root.Right.(*BinaryExpr)
	if !ok || rightMul.Op != BinaryMultiply {
		t.Fatalf("root.Right = %+v, want b*c", root.Right)
	}
}

func TestSelect_Scalar_JSONPathLeftAssoc(t *testing.T) {
	s := parseSelect(t, "SELECT payload -> 'k' ->> 'inner' FROM t")
	root := s.Select[0].Expr.(*BinaryExpr)
	if root.Op != BinaryJSONGetText {
		t.Fatalf("root op = %d, want ->>", root.Op)
	}
	if left, ok := root.Left.(*BinaryExpr); !ok || left.Op != BinaryJSONGet {
		t.Fatalf("root.Left = %+v, want payload->'k'", root.Left)
	}
}

func TestSelect_Aggregates(t *testing.T) {
	s := parseSelect(t, "SELECT count(*), sum(price), min(x), max(x) FROM t")
	if len(s.Select) != 4 {
		t.Fatalf("got %d cols", len(s.Select))
	}
	c0 := s.Select[0].Expr.(*FuncCall)
	if c0.Name != "count" || !c0.Star {
		t.Fatalf("count: %+v", c0)
	}
	for i, want := range []string{"count", "sum", "min", "max"} {
		fc, ok := s.Select[i].Expr.(*FuncCall)
		if !ok || fc.Name != want {
			t.Fatalf("col %d = %+v, want %s", i, s.Select[i].Expr, want)
		}
	}
}

func TestSelect_StringConcat(t *testing.T) {
	s := parseSelect(t, "SELECT first || ' ' || last FROM users")
	root := s.Select[0].Expr.(*BinaryExpr)
	if root.Op != BinaryConcat {
		t.Fatalf("root op = %d", root.Op)
	}
}

func TestParse_MultipleStatements(t *testing.T) {
	stmts, err := Parse("SELECT a FROM t; SELECT b FROM u;")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(stmts) != 2 {
		t.Fatalf("got %d stmts", len(stmts))
	}
}

func TestParse_RejectsUnknownStatement(t *testing.T) {
	if _, err := Parse("VACUUM"); err == nil {
		t.Fatal("VACUUM is unknown and must error")
	}
}

func TestParseDelete_AcceptsWhere(t *testing.T) {
	stmts, err := Parse("DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	d, ok := stmts[0].(*DeleteStmt)
	if !ok {
		t.Fatalf("got %T, want *DeleteStmt", stmts[0])
	}
	if d.Table != "users" || d.Where == nil {
		t.Fatalf("DELETE shape = %+v", d)
	}
}

func TestParseOne_RejectsMultiple(t *testing.T) {
	if _, err := ParseOne("SELECT a FROM t; SELECT b FROM u"); err == nil {
		t.Fatal("ParseOne must reject multi-statement input")
	}
}

func TestSelect_QuotedColumnPreservesCase(t *testing.T) {
	s := parseSelect(t, `SELECT "MixedCase" FROM t`)
	c := s.Select[0].Expr.(*ColumnRef)
	if c.Name != "MixedCase" {
		t.Fatalf("quoted ident name = %q", c.Name)
	}
}

func TestInsert_WithColumnList_SingleRow(t *testing.T) {
	s := parseInsert(t, "INSERT INTO users (id, name) VALUES (1, 'alice')")
	if s.Table != "users" {
		t.Fatalf("Table = %q", s.Table)
	}
	if len(s.Columns) != 2 || s.Columns[0] != "id" || s.Columns[1] != "name" {
		t.Fatalf("Columns = %v", s.Columns)
	}
	if len(s.Values) != 1 || len(s.Values[0]) != 2 {
		t.Fatalf("Values shape = %+v", s.Values)
	}
	if s.Values[0][0].Kind != ValueInt || s.Values[0][0].Int != 1 {
		t.Fatalf("row 0 col 0 = %+v", s.Values[0][0])
	}
	if s.Values[0][1].Kind != ValueString || s.Values[0][1].String != "alice" {
		t.Fatalf("row 0 col 1 = %+v", s.Values[0][1])
	}
}

func TestInsert_NoColumnList(t *testing.T) {
	s := parseInsert(t, "INSERT INTO t VALUES (1)")
	if len(s.Columns) != 0 {
		t.Fatalf("Columns = %v", s.Columns)
	}
	if len(s.Values) != 1 || s.Values[0][0].Int != 1 {
		t.Fatalf("Values = %+v", s.Values)
	}
}

func TestInsert_MultiRow(t *testing.T) {
	s := parseInsert(t, "INSERT INTO t (id) VALUES (1), (2), (3)")
	if len(s.Values) != 3 {
		t.Fatalf("rows = %d", len(s.Values))
	}
	for i, want := range []int64{1, 2, 3} {
		if s.Values[i][0].Int != want {
			t.Fatalf("row %d = %d, want %d", i, s.Values[i][0].Int, want)
		}
	}
}

func TestInsert_LiteralKinds(t *testing.T) {
	s := parseInsert(t, "INSERT INTO t VALUES (1, 2.5, 'x', true, false, null)")
	row := s.Values[0]
	want := []ValueKind{ValueInt, ValueFloat, ValueString, ValueBool, ValueBool, ValueNull}
	for i, w := range want {
		if row[i].Kind != w {
			t.Fatalf("col %d kind = %d, want %d (%+v)", i, row[i].Kind, w, row[i])
		}
	}
	if !row[3].Bool || row[4].Bool {
		t.Fatalf("bool values: %+v %+v", row[3], row[4])
	}
}

func TestInsert_NegativeLiteral(t *testing.T) {
	s := parseInsert(t, "INSERT INTO t VALUES (-1, -2.5)")
	if s.Values[0][0].Kind != ValueInt || s.Values[0][0].Int != -1 {
		t.Fatalf("col 0 = %+v", s.Values[0][0])
	}
	if s.Values[0][1].Kind != ValueFloat || s.Values[0][1].Float != -2.5 {
		t.Fatalf("col 1 = %+v", s.Values[0][1])
	}
}

func TestInsert_RejectsNonLiteralValue(t *testing.T) {
	if _, err := Parse("INSERT INTO t VALUES (id)"); err == nil {
		t.Fatal("non-literal in VALUES must error")
	}
}

func TestCreateType_Enum_LabelsAndIfNotExists(t *testing.T) {
	s := parseCreateType(t, "CREATE TYPE event AS ENUM ('view', 'click', 'buy')")
	if s.Name != "event" || s.IfNotExists {
		t.Fatalf("Name=%q IfNotExists=%v", s.Name, s.IfNotExists)
	}
	if len(s.EnumLabels) != 3 || s.EnumLabels[0] != "view" || s.EnumLabels[2] != "buy" {
		t.Fatalf("EnumLabels = %v", s.EnumLabels)
	}
	s = parseCreateType(t, "CREATE TYPE IF NOT EXISTS event AS ENUM ('a')")
	if !s.IfNotExists {
		t.Fatal("IfNotExists not set")
	}
}

func TestCreateType_RejectsNonEnum(t *testing.T) {
	if _, err := Parse("CREATE TYPE x AS RANGE ('a', 'b')"); err == nil {
		t.Fatal("non-ENUM type must error")
	}
}

func TestCreateTable_BasicColumns(t *testing.T) {
	s := parseCreateTable(t, "CREATE TABLE users (id int64, name text, age int32)")
	if s.Name != "users" {
		t.Fatalf("Name = %q", s.Name)
	}
	if len(s.Columns) != 3 {
		t.Fatalf("cols = %d", len(s.Columns))
	}
	if s.Columns[0].Type != "int64" || s.Columns[1].Type != "text" || s.Columns[2].Type != "int32" {
		t.Fatalf("col types = %+v", s.Columns)
	}
}

func TestCreateTable_NotNull(t *testing.T) {
	s := parseCreateTable(t, "CREATE TABLE t (id int64 NOT NULL, name text)")
	if !s.Columns[0].NotNull {
		t.Fatal("col 0 NotNull missing")
	}
	if s.Columns[1].NotNull {
		t.Fatal("col 1 NotNull should be false")
	}
}

func TestCreateTable_MultiwordType(t *testing.T) {
	s := parseCreateTable(t, "CREATE TABLE t (ts timestamp with time zone)")
	if s.Columns[0].Type != "timestamp with time zone" {
		t.Fatalf("Type = %q", s.Columns[0].Type)
	}
}

func TestCreateTable_WithOptions(t *testing.T) {
	s := parseCreateTable(t, "CREATE TABLE t (id int64) WITH (segment_rows = 1000000, codec = 'zstd', mutable = true)")
	if len(s.Options) != 3 {
		t.Fatalf("Options len = %d", len(s.Options))
	}
	if s.Options[0].Value.Kind != ValueInt || s.Options[0].Value.Int != 1_000_000 {
		t.Fatalf("opt 0 = %+v", s.Options[0])
	}
	if s.Options[1].Value.Kind != ValueString || s.Options[1].Value.String != "zstd" {
		t.Fatalf("opt 1 = %+v", s.Options[1])
	}
	if s.Options[2].Value.Kind != ValueBool || !s.Options[2].Value.Bool {
		t.Fatalf("opt 2 = %+v", s.Options[2])
	}
}

func TestCreateTable_OptionIdentBareWord(t *testing.T) {
	s := parseCreateTable(t, "CREATE TABLE t (id int64) WITH (storage = mutable)")
	if s.Options[0].Value.Kind != ValueIdent || s.Options[0].Value.String != "mutable" {
		t.Fatalf("opt = %+v", s.Options[0])
	}
}

func TestCreateTable_RejectsUnsupportedConstraints(t *testing.T) {
	for _, src := range []string{
		"CREATE TABLE t (id int64 PRIMARY KEY)",
		"CREATE TABLE t (id int64 UNIQUE)",
		"CREATE TABLE t (id int64 CHECK (id > 0))",
		"CREATE TABLE t (id int64 REFERENCES other)",
		"CREATE TABLE t (PRIMARY KEY (id))",
		"CREATE TABLE t (id int64, FOREIGN KEY (id))",
	} {
		if _, err := Parse(src); err == nil {
			t.Fatalf("must reject %q", src)
		}
	}
}

func TestCreateTable_RejectsEmptyColumnList(t *testing.T) {
	if _, err := Parse("CREATE TABLE t ()"); err == nil {
		t.Fatal("empty column list must error")
	}
}

func TestCreateTable_RejectsTrailingComma(t *testing.T) {
	if _, err := Parse("CREATE TABLE t (id int64,)"); err == nil {
		t.Fatal("trailing comma must error")
	}
}

func TestCreateTable_IfNotExists(t *testing.T) {
	s := parseCreateTable(t, "CREATE TABLE IF NOT EXISTS t (id int64)")
	if !s.IfNotExists {
		t.Fatal("IfNotExists not set")
	}
}

func TestExplain_SelectInner(t *testing.T) {
	stmt, err := ParseOne("EXPLAIN SELECT id FROM t")
	if err != nil {
		t.Fatalf("ParseOne: %v", err)
	}
	e, ok := stmt.(*ExplainStmt)
	if !ok {
		t.Fatalf("got %T, want *ExplainStmt", stmt)
	}
	if e.Analyze {
		t.Fatal("Analyze should be false")
	}
	if _, ok := e.Inner.(*SelectStmt); !ok {
		t.Fatalf("Inner = %T, want *SelectStmt", e.Inner)
	}
}

func TestExplain_Analyze(t *testing.T) {
	stmt, _ := ParseOne("EXPLAIN ANALYZE SELECT id FROM t")
	e := stmt.(*ExplainStmt)
	if !e.Analyze {
		t.Fatal("Analyze not set")
	}
}

func TestExplain_RejectsNonSelectInner(t *testing.T) {
	for _, src := range []string{
		"EXPLAIN INSERT INTO t VALUES (1)",
		"EXPLAIN CREATE TABLE t (id int64)",
	} {
		if _, err := Parse(src); err == nil {
			t.Fatalf("must reject %q", src)
		}
	}
}

func TestParse_With_SingleCTE(t *testing.T) {
	stmts, err := Parse("WITH t AS (SELECT id FROM users) SELECT id FROM t")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(stmts) != 1 {
		t.Fatalf("got %d statements", len(stmts))
	}
	sel, ok := stmts[0].(*SelectStmt)
	if !ok {
		t.Fatalf("statement is %T not *SelectStmt", stmts[0])
	}
	if len(sel.With) != 1 {
		t.Fatalf("got %d CTEs, want 1", len(sel.With))
	}
	if sel.With[0].Name != "t" {
		t.Fatalf("CTE name = %q, want t", sel.With[0].Name)
	}
	if sel.With[0].Query == nil {
		t.Fatal("CTE query is nil")
	}
}

func TestParse_With_MultipleCTEs(t *testing.T) {
	stmts, err := Parse("WITH a AS (SELECT id FROM users), b AS (SELECT id FROM orders) SELECT id FROM a")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	sel := stmts[0].(*SelectStmt)
	if len(sel.With) != 2 {
		t.Fatalf("got %d CTEs, want 2", len(sel.With))
	}
	if sel.With[0].Name != "a" || sel.With[1].Name != "b" {
		t.Fatalf("CTE names = %q, %q", sel.With[0].Name, sel.With[1].Name)
	}
}

func TestParse_With_DuplicateNameRejected(t *testing.T) {
	_, err := Parse("WITH t AS (SELECT id FROM users), t AS (SELECT id FROM orders) SELECT * FROM t")
	if err == nil {
		t.Fatal("expected error for duplicate CTE name")
	}
}
