// Compact rewrites segments that still carry tombstoned column bytes, so DROP COLUMN
// eventually frees disk once Compact runs.
package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

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
	// Make the junk column heavy so the size delta after dropping is visible.
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

	// The active segments produced by compaction must reference only active column ids.
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

	// VacuumRetention past the compact commit must physically delete the pre-compact segment file.
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
