// Relocation contract tests. A database directory must survive a move, and legacy
// manifests holding absolute segment paths must keep opening in place.
package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

func TestDB_RelocatedDirectoryStaysUsable(t *testing.T) {
	base := t.TempDir()
	oldPath := filepath.Join(base, "old")
	db, err := Open(oldPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1), (2)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (3), (4)")
	mustExec(t, db, "DELETE FROM t WHERE id = 2")
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	newPath := filepath.Join(base, "moved")
	if err := os.Rename(oldPath, newPath); err != nil {
		t.Fatalf("rename db dir: %v", err)
	}

	db2, err := Open(newPath)
	if err != nil {
		t.Fatalf("reopen after move: %v", err)
	}
	defer db2.Close()

	rows, err := db2.Query(context.Background(), "SELECT id FROM t ORDER BY id")
	if err != nil {
		t.Fatalf("query after move: %v", err)
	}
	wantRows(t, rows.Values, [][]any{{int64(1)}, {int64(3)}, {int64(4)}})

	mustExec(t, db2, "INSERT INTO t (id) VALUES (5)")
	if got := mustValues(t, db2, "SELECT id FROM t"); len(got) != 4 {
		t.Fatalf("after insert got %d rows, want 4", len(got))
	}

	// The first segment is half deleted, so Compact must rewrite it at the new location.
	rewritten, err := db2.Compact(context.Background(), "t")
	if err != nil {
		t.Fatalf("compact after move: %v", err)
	}
	if rewritten == 0 {
		t.Fatal("expected the half-deleted segment to be rewritten after the move")
	}
	if _, err := db2.Vacuum(); err != nil {
		t.Fatalf("vacuum after move: %v", err)
	}
	if got := mustValues(t, db2, "SELECT id FROM t"); len(got) != 4 {
		t.Fatalf("after compact and vacuum got %d rows, want 4", len(got))
	}
}

func TestDB_LegacyAbsoluteManifestPathStillOpens(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	tableDir := db.tableDir("t")
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Hand-append a manifest record whose Path is a fully joined absolute path,
	// simulating a database written before relative manifest paths.
	legacyPath := filepath.Join(tableDir, "legacy.dsv4")
	writeSingleInt(t, legacyPath, 42)
	m, err := storage.OpenManifest(filepath.Join(tableDir, "manifest"))
	if err != nil {
		t.Fatalf("open manifest: %v", err)
	}
	if err := m.Commit(1, []storage.ManifestSegmentAdd{{Path: legacyPath, Rows: 1}}, nil); err != nil {
		m.Close()
		t.Fatalf("commit legacy entry: %v", err)
	}
	m.Close()

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen with legacy entry: %v", err)
	}
	defer db2.Close()
	wantRows(t, mustValues(t, db2, "SELECT id FROM t"), [][]any{{int64(42)}})

	// A DV write against the absolute-path segment must round-trip too.
	mustExec(t, db2, "DELETE FROM t WHERE id = 42")
	if got := mustValues(t, db2, "SELECT id FROM t"); len(got) != 0 {
		t.Fatalf("after delete got %d rows, want 0", len(got))
	}
}
