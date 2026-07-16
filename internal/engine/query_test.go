// EXPLAIN driver smoke test covering plan text output and ANALYZE timing lines.
package engine

import (
	"context"
	"strings"
	"testing"
)

func TestEngine_ExplainSmoke(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE users (id int64, name text, age int64);"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO users VALUES (1,'a',10),(2,'b',20),(3,'c',30);"); err != nil {
		t.Fatal(err)
	}
	cases := []string{
		"EXPLAIN SELECT id, age FROM users WHERE age > 15 ORDER BY age DESC LIMIT 2",
		"EXPLAIN SELECT count(*) FROM users",
		"EXPLAIN ANALYZE SELECT id FROM users",
	}
	for _, q := range cases {
		rows, err := db.Query(ctx, q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if len(rows.Columns) != 1 || rows.Columns[0] != "plan" {
			t.Fatalf("cols=%v", rows.Columns)
		}
		var b strings.Builder
		for _, r := range rows.Values {
			b.WriteString(r[0].(string))
			b.WriteString("\n")
		}
		t.Logf("\n== %s\n%s", q, b.String())
		if strings.HasPrefix(q, "EXPLAIN ANALYZE") {
			if !strings.Contains(b.String(), "wall=") {
				t.Errorf("ANALYZE output missing per-operator timings: %s", b.String())
			}
		}
	}
}
