// Compact and Vacuum tests covering DELETE-driven rewrites, DROP COLUMN tombstone cleanup, legacy identity-less segment migration, and Vacuum of orphaned .dv files.
package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
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

func segmentByteSize(t *testing.T, dir string) int64 {
	t.Helper()
	var total int64
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".dsv4") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		total += info.Size()
	}
	return total
}

func TestEngine_Compact_RewritesAfterDropColumn(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id int64 NOT NULL, junk text NOT NULL, keep int64 NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	// A heavy junk column makes the size delta after dropping visible.
	junk := strings.Repeat("xyz", 50)
	for i := int64(0); i < 200; i++ {
		stmt := fmt.Sprintf("INSERT INTO t (id, junk, keep) VALUES (%d, '%s', %d)", i, junk, i*2)
		if _, err := db.Exec(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	tableDir := filepath.Join(dir, "tables", "0000000000000001")
	before := segmentByteSize(t, tableDir)

	if _, err := db.Exec(ctx, "ALTER TABLE t DROP COLUMN junk"); err != nil {
		t.Fatal(err)
	}
	rewritten, err := db.Compact(ctx, "t")
	if err != nil {
		t.Fatalf("compact: %v", err)
	}
	if rewritten == 0 {
		t.Fatal("expected at least one segment to be rewritten after DROP COLUMN")
	}

	bd, err := db.boundTableByName("t")
	if err != nil {
		t.Fatal(err)
	}
	activeIDs := make(map[uint64]struct{}, len(bd.Columns))
	for _, c := range bd.Columns {
		activeIDs[uint64(c.ID)] = struct{}{}
	}

	m, err := db.manifestFor("t")
	if err != nil {
		t.Fatal(err)
	}
	view := m.Snapshot()
	checked := 0
	for _, e := range view.Entries {
		if e.Path == "" || e.DeletionVectorPath != "" {
			continue
		}
		seg, err := storage.OpenSegment(e.Path)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range seg.Cols {
			if c.ColumnID == 0 {
				continue
			}
			if _, ok := activeIDs[c.ColumnID]; !ok {
				seg.Close()
				t.Fatalf("active segment %s still carries dropped column id %d after compact", e.Path, c.ColumnID)
			}
		}
		seg.Close()
		checked++
	}
	if checked == 0 {
		t.Fatal("expected at least one active segment after compaction")
	}

	if _, err := db.VacuumRetention(^uint64(0)); err != nil {
		t.Fatalf("retention vacuum: %v", err)
	}
	after := segmentByteSize(t, tableDir)
	if after >= before {
		t.Fatalf("byte size did not shrink after dropping a heavy column then compacting: before=%d after=%d", before, after)
	}
	entries, err := os.ReadDir(tableDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".dsv4") {
			continue
		}
		seg, err := storage.OpenSegment(filepath.Join(tableDir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range seg.Cols {
			if c.ColumnID == 0 {
				continue
			}
			if _, ok := activeIDs[c.ColumnID]; !ok {
				seg.Close()
				t.Fatalf("segment %s still carries dropped column id %d after retention vacuum", e.Name(), c.ColumnID)
			}
		}
		seg.Close()
	}

	rows := mustValues(t, db, "SELECT id, keep FROM t WHERE id = 7")
	wantRows(t, rows, [][]any{{int64(7), int64(14)}})
}

func TestEngine_Compact_MigratesLegacySegments(t *testing.T) {
	dir := t.TempDir()
	tableDir := filepath.Join(dir, "segments", "users")
	if err := os.MkdirAll(tableDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "catalog.json"), []byte(v1UsersCatalog), 0o644); err != nil {
		t.Fatal(err)
	}

	// A true legacy segment without an identity sidecar that we then publish via the manifest.
	legacyPath := filepath.Join(tableDir, "999000.dsv4")
	idVec := vector.NewVec(vector.VecInt64, 3)
	for i, v := range []int64{1, 2, 3} {
		idVec.I64()[i] = v
	}
	nameVec := vector.NewVarVec(vector.VecText, 3, 0)
	nb := nameVec.Var()
	for i, s := range []string{"a", "b", "c"} {
		nb.AppendString(i, s)
	}
	batch, err := vector.NewBatch([]vector.Column{
		{Name: "id", Type: schema.Int64, V: idVec},
		{Name: "name", Type: schema.Text, V: nameVec},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := storage.WriteSegment(legacyPath, []vector.Batch{batch}, nil); err != nil {
		t.Fatal(err)
	}

	manifest, err := storage.OpenManifest(filepath.Join(tableDir, "manifest"))
	if err != nil {
		t.Fatal(err)
	}
	if err := manifest.Commit(1, []storage.ManifestSegmentAdd{{Path: legacyPath, Rows: 3}}, nil); err != nil {
		manifest.Close()
		t.Fatal(err)
	}
	manifest.Close()

	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()

	seg, err := storage.OpenSegment(legacyPath)
	if err != nil {
		t.Fatal(err)
	}
	if seg.TableID != 0 {
		seg.Close()
		t.Fatal("test setup expected a legacy segment with TableID == 0")
	}
	seg.Close()

	if _, err := db.Compact(ctx, "users"); err != nil {
		t.Fatalf("compact: %v", err)
	}

	m, err := db.manifestFor("users")
	if err != nil {
		t.Fatal(err)
	}
	migrated := 0
	for _, e := range m.Snapshot().Entries {
		if e.Path == "" || e.DeletionVectorPath != "" {
			continue
		}
		s, err := storage.OpenSegment(e.Path)
		if err != nil {
			t.Fatal(err)
		}
		if s.TableID == 0 {
			s.Close()
			t.Fatalf("active segment %s still legacy after compact", e.Path)
		}
		s.Close()
		migrated++
	}
	if migrated == 0 {
		t.Fatal("expected at least one active segment after compact")
	}

	rows := mustValues(t, db, "SELECT id, name FROM users ORDER BY id")
	wantRows(t, rows, [][]any{
		{int64(1), "a"},
		{int64(2), "b"},
		{int64(3), "c"},
	})
}
