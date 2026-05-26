// CLI tests cover the rendering shapes per format and the statement-classification helpers.
// The shell loop and meta dispatch are exercised by runOne and runMeta directly without the readline TTY.
package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kylegrahammatzen/dripsql"
)

func openTestDB(t *testing.T) *dripsql.DB {
	t.Helper()
	db, err := dripsql.Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestCLI_RunOne_ExecAndQuery(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	var out bytes.Buffer
	if err := runOne(ctx, db, "CREATE TABLE t (id int64 NOT NULL)", formatTable, false, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "ok") {
		t.Fatalf("CREATE output = %q", out.String())
	}
	out.Reset()
	if err := runOne(ctx, db, "INSERT INTO t (id) VALUES (1), (2), (3)", formatTable, false, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "3 rows affected") {
		t.Fatalf("INSERT output = %q", out.String())
	}
	out.Reset()
	if err := runOne(ctx, db, "SELECT id FROM t ORDER BY id", formatTable, true, &out); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	if !strings.Contains(got, "id") || !strings.Contains(got, "(3 rows)") {
		t.Fatalf("SELECT output missing header or row count, got %q", got)
	}
	if !strings.Contains(got, "time ") {
		t.Fatalf("SELECT output should include timing footer, got %q", got)
	}
}

func TestCLI_RunOne_TimingOff(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	var out bytes.Buffer
	if err := runOne(context.Background(), db, "SELECT count(*) FROM t", formatTable, false, &out); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "time ") {
		t.Fatalf("timing should be suppressed, got %q", out.String())
	}
}

func TestCLI_RenderRows_TSV(t *testing.T) {
	var out bytes.Buffer
	if err := renderRows(&out, formatTSV, []string{"a", "b"}, [][]any{{int64(1), "hi"}, {int64(2), "lo"}}); err != nil {
		t.Fatal(err)
	}
	want := "a\tb\n1\thi\n2\tlo\n"
	if out.String() != want {
		t.Fatalf("tsv = %q, want %q", out.String(), want)
	}
}

func TestCLI_RenderRows_JSON(t *testing.T) {
	var out bytes.Buffer
	if err := renderRows(&out, formatJSON, []string{"a"}, [][]any{{int64(1)}, {int64(2)}}); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	if !strings.Contains(got, `"a": 1`) || !strings.Contains(got, `"a": 2`) {
		t.Fatalf("json output missing rows, got %q", got)
	}
}

func TestCLI_RenderRows_Table(t *testing.T) {
	var out bytes.Buffer
	if err := renderRows(&out, formatTable, []string{"id", "name"}, [][]any{{int64(1), "alice"}, {int64(2), "bob"}}); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	if !strings.Contains(got, "id") || !strings.Contains(got, "alice") {
		t.Fatalf("table output missing header or row, got %q", got)
	}
	if !strings.Contains(got, "--") {
		t.Fatalf("table output should include a separator row, got %q", got)
	}
}

func TestCLI_Meta_Tables(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE alpha (id int64 NOT NULL)")
	mustExec(t, db, "CREATE TABLE beta (id int64 NOT NULL)")
	mode := formatTable
	timing := false
	var out bytes.Buffer
	if quit, err := runMeta(db, ".tables", &mode, &timing, &out); err != nil || quit {
		t.Fatalf("runMeta .tables = quit=%v err=%v", quit, err)
	}
	got := out.String()
	if !strings.Contains(got, "alpha") || !strings.Contains(got, "beta") {
		t.Fatalf("tables output missing names, got %q", got)
	}
}

func TestCLI_Meta_Schema(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, name text)")
	mode := formatTable
	timing := false
	var out bytes.Buffer
	if _, err := runMeta(db, ".schema t", &mode, &timing, &out); err != nil {
		t.Fatalf("runMeta .schema: %v", err)
	}
	got := out.String()
	if !strings.Contains(got, "id") || !strings.Contains(got, "name") {
		t.Fatalf("schema output missing columns, got %q", got)
	}
	if !strings.Contains(got, "NOT NULL") || !strings.Contains(got, "NULL") {
		t.Fatalf("schema output missing nullability, got %q", got)
	}
}

func TestCLI_Meta_ModeAndTimerToggle(t *testing.T) {
	db := openTestDB(t)
	mode := formatTable
	timing := true
	var out bytes.Buffer
	if _, err := runMeta(db, ".mode tsv", &mode, &timing, &out); err != nil {
		t.Fatal(err)
	}
	if mode != formatTSV {
		t.Fatalf("mode = %v, want tsv", mode)
	}
	if _, err := runMeta(db, ".timer off", &mode, &timing, &out); err != nil {
		t.Fatal(err)
	}
	if timing {
		t.Fatalf("timing should be off")
	}
}

func TestCLI_Meta_Quit(t *testing.T) {
	db := openTestDB(t)
	mode := formatTable
	timing := false
	var out bytes.Buffer
	quit, err := runMeta(db, ".quit", &mode, &timing, &out)
	if err != nil || !quit {
		t.Fatalf("runMeta .quit = quit=%v err=%v", quit, err)
	}
}

func TestCLI_RunOne_DispatchesCTE(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1), (2), (3)")
	var out bytes.Buffer
	if err := runOne(context.Background(), db, "WITH s AS (SELECT id FROM t) SELECT id FROM s ORDER BY id", formatTable, false, &out); err != nil {
		t.Fatalf("CTE runOne: %v", err)
	}
	if !strings.Contains(out.String(), "(3 rows)") {
		t.Fatalf("CTE output missing row count, got %q", out.String())
	}
}

func TestCLI_TopLevelSemicolon(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"SELECT 1;", 8},
		{"SELECT 1", -1},
		{"INSERT INTO t VALUES ('a;b');", 28},
		{"INSERT INTO t VALUES ('it''s ok;');", 34},
		{"SELECT 1; SELECT 2;", 8},
		{"'unterminated", -1},
	}
	for _, c := range cases {
		if got := topLevelSemicolon(c.in); got != c.want {
			t.Errorf("topLevelSemicolon(%q) = %d, want %d", c.in, got, c.want)
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
		{[]byte("hello"), "hello"},
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

func TestCLI_FormatDuration(t *testing.T) {
	cases := []struct {
		in   time.Duration
		want string
	}{
		{0, "<1 ns"},
		{750 * time.Nanosecond, "750 ns"},
		{500 * time.Microsecond, "500 us"},
		{12340 * time.Microsecond, "12.34 ms"},
		{1500 * time.Millisecond, "1.500 s"},
	}
	for _, c := range cases {
		if got := formatDuration(c.in); got != c.want {
			t.Errorf("formatDuration(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

func mustExec(t *testing.T, db *dripsql.DB, sql string) {
	t.Helper()
	if _, err := db.Exec(context.Background(), sql); err != nil {
		t.Fatalf("Exec %q: %v", sql, err)
	}
}
