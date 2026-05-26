// End-to-end checks that INSERT stamps the segment identity sidecar and the manifest
// entry's SchemaGeneration. Confirms the engine plumbing matches the storage contract.
package engine

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

func TestEngine_InsertStampsSegmentIdentity(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id int64 NOT NULL, name text NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b')"); err != nil {
		t.Fatal(err)
	}

	tab := db.tables["t"]
	if tab == nil {
		t.Fatal("table t missing from db.tables")
	}
	tableDir := filepath.Join(dir, "tables", "0000000000000001")
	segPath := ""
	entries, err := os.ReadDir(tableDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".dsv4") {
			segPath = filepath.Join(tableDir, e.Name())
			break
		}
	}
	if segPath == "" {
		t.Fatal("no .dsv4 segment produced by INSERT")
	}

	seg, err := storage.OpenSegment(segPath)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()
	if seg.TableID != uint64(tab.TableID) {
		t.Fatalf("segment.TableID = %d, want %d", seg.TableID, tab.TableID)
	}
	if seg.SchemaGeneration != uint64(db.catalog.Generation) {
		t.Fatalf("segment.SchemaGeneration = %d, want %d", seg.SchemaGeneration, db.catalog.Generation)
	}
	if len(seg.Cols) != 2 || seg.Cols[0].ColumnID != 1 || seg.Cols[1].ColumnID != 2 {
		t.Fatalf("column ids: got %+v, want [1 2]", []uint64{seg.Cols[0].ColumnID, seg.Cols[1].ColumnID})
	}
}

func TestEngine_InsertStampsManifestSchemaGeneration(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id int64 NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO t (id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	m, err := db.manifestFor("t")
	if err != nil {
		t.Fatal(err)
	}
	view := m.Snapshot()
	if len(view.Entries) == 0 {
		t.Fatal("no manifest entries after INSERT")
	}
	wantGen := uint64(db.catalog.Generation)
	for _, e := range view.Entries {
		if e.SchemaGeneration != wantGen {
			t.Fatalf("entry %s SchemaGeneration = %d, want %d", e.Path, e.SchemaGeneration, wantGen)
		}
	}
}
