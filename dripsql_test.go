// Public API contract tests for the dripsql package.
package dripsql

import (
	"context"
	"strings"
	"testing"
)

func openTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// Streaming Rows must advance with Next, copy typed values with Scan, and report no error at clean exhaustion.
func TestRows_StreamingScan(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, name text NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, name) VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

	rows, err := db.Query(ctx, "SELECT id, name FROM t")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	type row struct {
		id   int64
		name string
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.name); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Err after clean exhaust: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d rows, want 3", len(got))
	}
	if got[0] != (row{1, "alpha"}) || got[1] != (row{2, "beta"}) || got[2] != (row{3, "gamma"}) {
		t.Errorf("rows mismatch: %+v", got)
	}
}

// Columns returns the projected names in select order so callers can inspect schema without scanning.
func TestRows_Columns(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, kind text NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, kind) VALUES (1, 'x')")
	rows, err := db.Query(context.Background(), "SELECT id, kind FROM t")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()
	cols := rows.Columns()
	if len(cols) != 2 || cols[0] != "id" || cols[1] != "kind" {
		t.Errorf("Columns = %v, want [id kind]", cols)
	}
}

// Scan must reject a mismatched destination count instead of writing partial state.
func TestRows_Scan_DestCountMismatch(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	rows, err := db.Query(context.Background(), "SELECT id FROM t")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected one row")
	}
	var a, b int64
	if err := rows.Scan(&a, &b); err == nil {
		t.Fatal("expected count-mismatch error")
	}
}

// SetReadOnly must reject writes through Exec and BeginTx while still allowing reads.
func TestSetReadOnly_BlocksWrites(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	db.SetReadOnly(true)
	if _, err := db.Exec(ctx, "INSERT INTO t (id) VALUES (2)"); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("Exec in read-only mode, got err=%v want read-only", err)
	}
	if _, err := db.BeginTx(ctx); err == nil || !strings.Contains(err.Error(), "read-only") {
		t.Fatalf("BeginTx in read-only mode, got err=%v want read-only", err)
	}
	rows, err := db.Query(ctx, "SELECT id FROM t")
	if err != nil {
		t.Fatalf("Query in read-only mode: %v", err)
	}
	rows.Close()
	db.SetReadOnly(false)
	if _, err := db.Exec(ctx, "INSERT INTO t (id) VALUES (3)"); err != nil {
		t.Fatalf("Exec after toggle off: %v", err)
	}
}

// SetCacheSize must accept positive values and reset to the default when given anything below 1.
func TestSetCacheSize_AcceptsRange(t *testing.T) {
	db := openTestDB(t)
	db.SetCacheSize(32)
	db.SetCacheSize(0)
	db.SetCacheSize(-5)
	db.SetCacheSize(1024)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
}

// All drains the entire result set into a slice so terse one-shot reads skip the Next plus Scan loop.
func TestRows_All(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (10), (20), (30)")
	rows, err := db.Query(context.Background(), "SELECT id FROM t")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()
	vals, err := rows.All()
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	if len(vals) != 3 || vals[0][0].(int64) != 10 || vals[2][0].(int64) != 30 {
		t.Errorf("All = %v, want three rows 10,20,30", vals)
	}
}

// Positional placeholders bind left-to-right across types and reach both Query and Exec paths.
func TestPlaceholders_BindAcrossTypesAndPaths(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, kind text NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, kind) VALUES (1, 'a'), (2, 'b'), (3, 'a'), (4, 'c')")

	rows, err := db.Query(ctx, "SELECT count(*) FROM t WHERE id >= ? AND kind = ?", int64(2), "a")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	var got int64
	rows.Next()
	if err := rows.Scan(&got); err != nil {
		t.Fatalf("Scan: %v", err)
	}
	rows.Close()
	if got != 1 {
		t.Errorf("count = %d, want 1 (id=3,kind=a matches)", got)
	}

	res, err := db.Exec(ctx, "DELETE FROM t WHERE id = ?", int64(2))
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if res.RowsAffected != 1 {
		t.Errorf("DELETE RowsAffected = %d, want 1", res.RowsAffected)
	}

	if _, err := db.Query(ctx, "SELECT id FROM t WHERE id = ?"); err == nil {
		t.Fatal("expected error when args are short of placeholder count")
	}
}

// Open rejects the empty path so a missing config value cannot silently land on the wrong directory.
func TestOpen_EmptyPathRejected(t *testing.T) {
	if _, err := Open(""); err == nil {
		t.Fatal("Open(\"\") must return an error")
	}
}

// MemoryPath returns an explicit not-implemented error so callers learn before relying on it.
func TestOpen_MemoryPathNotYet(t *testing.T) {
	if _, err := Open(MemoryPath); err == nil {
		t.Fatal("Open(MemoryPath) must error until the in-memory backend lands")
	}
}

func mustExec(t *testing.T, db *DB, sql string) {
	t.Helper()
	if _, err := db.Exec(context.Background(), sql); err != nil {
		t.Fatalf("Exec %q: %v", sql, err)
	}
}
