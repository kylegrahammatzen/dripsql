package parser

import (
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/sql/ast"
)

func TestParseExplainSelect(t *testing.T) {
	stmt, err := ParseOne("EXPLAIN SELECT count(*) FROM events WHERE tenant_id = 42")
	if err != nil {
		t.Fatalf("ParseOne: %v", err)
	}
	exp, ok := stmt.(*ast.ExplainStmt)
	if !ok {
		t.Fatalf("got %T, want *ast.ExplainStmt", stmt)
	}
	if exp.Analyze {
		t.Errorf("Analyze = true, want false")
	}
	inner, ok := exp.Inner.(*ast.SelectStmt)
	if !ok {
		t.Fatalf("inner = %T, want *ast.SelectStmt", exp.Inner)
	}
	if inner.Table != "events" {
		t.Errorf("inner.Table = %q, want events", inner.Table)
	}
}

func TestParseExplainAnalyzeSelect(t *testing.T) {
	stmt, err := ParseOne("EXPLAIN ANALYZE SELECT count(*) FROM events")
	if err != nil {
		t.Fatalf("ParseOne: %v", err)
	}
	exp, ok := stmt.(*ast.ExplainStmt)
	if !ok {
		t.Fatalf("got %T, want *ast.ExplainStmt", stmt)
	}
	if !exp.Analyze {
		t.Errorf("Analyze = false, want true")
	}
	if _, ok := exp.Inner.(*ast.SelectStmt); !ok {
		t.Errorf("inner = %T, want *ast.SelectStmt", exp.Inner)
	}
}

func TestParseExplainRejectsNonSelect(t *testing.T) {
	cases := []struct {
		sql      string
		fragment string
	}{
		{"EXPLAIN INSERT INTO events VALUES (1, 'x')", "supports SELECT only"},
		{"EXPLAIN CREATE TABLE x (a INT64)", "supports SELECT only"},
	}
	for _, tc := range cases {
		_, err := ParseOne(tc.sql)
		if err == nil {
			t.Errorf("ParseOne(%q): expected error containing %q", tc.sql, tc.fragment)
			continue
		}
		if !strings.Contains(err.Error(), tc.fragment) {
			t.Errorf("ParseOne(%q): error %q, want fragment %q", tc.sql, err.Error(), tc.fragment)
		}
	}
}
