// Compact + Vacuum smoke tests: DELETE invalidates most of a segment, Compact rewrites
// it without changing query results, and Vacuum drops the orphaned .dv files.
package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEngine_Compact_HalvesLargelyDeletedSegment(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id int64 NOT NULL, label text NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'a'),(4,'a'),(5,'c'),(6,'b'),(7,'a'),(8,'c')"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "DELETE FROM t WHERE id < 7"); err != nil {
		t.Fatal(err)
	}
	rewritten, err := db.Compact(ctx, "t")
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if rewritten != 1 {
		t.Fatalf("rewritten = %d, want 1", rewritten)
	}
	rows, err := db.Query(ctx, "SELECT id, label FROM t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Values) != 2 {
		t.Fatalf("post-compact rows = %d, want 2 (got=%v)", len(rows.Values), rows.Values)
	}
	for _, r := range rows.Values {
		switch r[0].(int64) {
		case 7, 8:
		default:
			t.Errorf("unexpected post-compact id %v", r[0])
		}
	}
}

func TestEngine_Vacuum_RemovesUnreferencedDV(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id int64 NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t VALUES (1),(2),(3)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "UPDATE t SET id = 9 WHERE id = 1"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "UPDATE t SET id = 8 WHERE id = 2"); err != nil {
		t.Fatal(err)
	}
	tableDir := filepath.Join(dir, "tables", "0000000000000001")
	before, err := countMatching(tableDir, ".dv.")
	if err != nil {
		t.Fatal(err)
	}
	if before < 2 {
		t.Fatalf("expected at least 2 versioned dv files before vacuum, found %d", before)
	}
	removed, err := db.Vacuum()
	if err != nil {
		t.Fatalf("vacuum: %v", err)
	}
	after, err := countMatching(tableDir, ".dv.")
	if err != nil {
		t.Fatal(err)
	}
	if before-after != removed {
		t.Fatalf("removed = %d, but file count before=%d after=%d", removed, before, after)
	}
	if removed < 1 {
		t.Fatalf("vacuum removed = %d, want >= 1 unreferenced dv", removed)
	}
}

func countMatching(dir, infix string) (int, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.Contains(e.Name(), infix) {
			count++
		}
	}
	return count, nil
}
