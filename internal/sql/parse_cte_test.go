// CTE parse tests. Bind/exec for WITH clauses lands in follow-up slices.
package sql

import "testing"

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
