// CLI tests exercise repl()/runOne() against an in-memory pipe so the assertions are deterministic.
package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/engine"
)

func openTestDB(t *testing.T) *engine.DB {
	t.Helper()
	db, err := engine.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestCLI_ExecAndQuery(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	var out bytes.Buffer
	if err := runOne(ctx, db, "CREATE TABLE t (id int64 NOT NULL)", &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "ok") {
		t.Fatalf("CREATE output = %q", out.String())
	}
	out.Reset()
	if err := runOne(ctx, db, "INSERT INTO t (id) VALUES (1), (2), (3)", &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "3 rows affected") {
		t.Fatalf("INSERT output = %q", out.String())
	}
	out.Reset()
	if err := runOne(ctx, db, "SELECT id FROM t ORDER BY id", &out); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	if !strings.Contains(got, "id\n") || !strings.Contains(got, "1\n") || !strings.Contains(got, "(3 rows)") {
		t.Fatalf("SELECT output = %q", got)
	}
}

func TestCLI_REPLStreamsStatementsSeparatedBySemicolons(t *testing.T) {
	db := openTestDB(t)
	in := strings.NewReader("CREATE TABLE t (id int64 NOT NULL);\nINSERT INTO t (id) VALUES (7);\nSELECT id FROM t;\n")
	var out bytes.Buffer
	if err := repl(context.Background(), db, in, &out); err != nil {
		t.Fatalf("repl: %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "1 row affected") {
		t.Fatalf("INSERT not reported: %q", got)
	}
	if !strings.Contains(got, "7\n") {
		t.Fatalf("SELECT value missing: %q", got)
	}
}

func TestCLI_IsQuery(t *testing.T) {
	cases := []struct {
		sql  string
		want bool
	}{
		{"SELECT 1", true},
		{"  select id from t", true},
		{"EXPLAIN SELECT 1", true},
		{"CREATE TABLE t (id int64 NOT NULL)", false},
		{"INSERT INTO t VALUES (1)", false},
		{"", false},
	}
	for _, c := range cases {
		if got := isQuery(c.sql); got != c.want {
			t.Errorf("isQuery(%q) = %v, want %v", c.sql, got, c.want)
		}
	}
}

func TestCLI_FormatValue(t *testing.T) {
	cases := []struct {
		in   any
		want string
	}{
		{nil, "NULL"},
		{"hello", "hello"},
		{int64(42), "42"},
		{float64(1.5), "1.5"},
		{true, "true"},
		{false, "false"},
	}
	for _, c := range cases {
		if got := formatValue(c.in); got != c.want {
			t.Errorf("formatValue(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}
