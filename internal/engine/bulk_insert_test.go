// BulkInsert collapses many INSERT statements into one multi-page segment.
// Round-trips data unchanged and reduces segment count vs per-call inserts.
package engine

import (
	"context"
	"fmt"
	"testing"
)

func TestEngine_BulkInsert_OneSegmentManyPages(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	if _, err := db.Exec(ctx, "CREATE TABLE t (id int64, name text)"); err != nil {
		t.Fatal(err)
	}

	stmts := []string{
		"INSERT INTO t VALUES (1, 'a'), (2, 'b')",
		"INSERT INTO t VALUES (3, 'c'), (4, 'd')",
		"INSERT INTO t VALUES (5, 'e')",
	}
	n, err := db.BulkInsert(ctx, stmts)
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("rows: got %d want 5", n)
	}

	segs, err := db.openSegmentsForQuery("t")
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 1 {
		t.Fatalf("segments: got %d want 1", len(segs))
	}
	// Three INSERTs became three pages in one segment.
	if got := len(segs[0].Cols[0].Pages); got != 3 {
		t.Fatalf("pages: got %d want 3", got)
	}

	rows, err := db.Query(ctx, "SELECT id, name FROM t ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Values) != 5 {
		t.Fatalf("query rows: got %d want 5", len(rows.Values))
	}
	for i, want := range []struct {
		id   int64
		name string
	}{{1, "a"}, {2, "b"}, {3, "c"}, {4, "d"}, {5, "e"}} {
		gotID, _ := rows.Values[i][0].(int64)
		gotName, _ := rows.Values[i][1].(string)
		if gotID != want.id || gotName != want.name {
			t.Fatalf("row %d: got (%d, %q) want (%d, %q)", i, gotID, gotName, want.id, want.name)
		}
	}
}

func TestEngine_BulkInsert_MismatchedTablesError(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	if _, err := db.Exec(ctx, "CREATE TABLE a (id int64)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "CREATE TABLE b (id int64)"); err != nil {
		t.Fatal(err)
	}

	stmts := []string{
		"INSERT INTO a VALUES (1)",
		"INSERT INTO b VALUES (2)",
	}
	if _, err := db.BulkInsert(ctx, stmts); err == nil {
		t.Fatal("expected error for cross-table BulkInsert, got nil")
	}
}

func TestEngine_BulkInsert_RejectsNonInsert(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()

	if _, err := db.Exec(ctx, "CREATE TABLE t (id int64)"); err != nil {
		t.Fatal(err)
	}

	stmts := []string{
		"INSERT INTO t VALUES (1)",
		"DELETE FROM t",
	}
	if _, err := db.BulkInsert(ctx, stmts); err == nil {
		t.Fatal("expected error for non-INSERT in BulkInsert, got nil")
	}
	_ = fmt.Sprintf
}
