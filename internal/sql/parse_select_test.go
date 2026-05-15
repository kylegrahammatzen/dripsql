// SELECT parser tests: projection star/list/alias, WHERE precedence, GROUP BY/HAVING coupling,
// ORDER BY direction, LIMIT/OFFSET, scalar precedence (path/term/expr), JSON path left-assoc.
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
