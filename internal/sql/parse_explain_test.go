// EXPLAIN parser tests: optional ANALYZE, SELECT-only inner, error on non-SELECT inner.
package sql

import "testing"

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
