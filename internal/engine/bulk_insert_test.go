// DML plumbing tests covering EXPLAIN smoke, segment identity sidecar stamping, and table-path routing for new vs migrated tables.
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

func TestEngine_NewTableUsesIDPath(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id int64 NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO t (id) VALUES (1), (2)"); err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(dir, "tables", "0000000000000001")
	entries, err := os.ReadDir(want)
	if err != nil {
		t.Fatalf("read id dir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatalf("expected at least manifest + 1 segment under %s, got empty", want)
	}
	if _, err := os.Stat(filepath.Join(dir, "segments", "t")); !os.IsNotExist(err) {
		t.Fatalf("legacy segments/t should not exist for new table (err=%v)", err)
	}
}

func TestEngine_MigratedTableKeepsLegacyPath(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "segments", "users"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "catalog.json"), []byte(v1UsersCatalog), 0o644); err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.Exec(context.Background(), "INSERT INTO users (id, name) VALUES (1, 'a')"); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Join(dir, "segments", "users"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("migrated table should keep writing under segments/users")
	}
	if _, err := os.Stat(filepath.Join(dir, "tables")); !os.IsNotExist(err) {
		t.Fatalf("tables/ must not appear for a legacy-path table (err=%v)", err)
	}
}

func TestEngine_MixedLegacyAndNewTables(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "segments", "users"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "catalog.json"), []byte(v1UsersCatalog), 0o644); err != nil {
		t.Fatal(err)
	}
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.Exec(context.Background(), "CREATE TABLE fresh (id int64 NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO users (id, name) VALUES (1, 'a')"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(context.Background(), "INSERT INTO fresh (id) VALUES (10)"); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(filepath.Join(dir, "segments", "users", "manifest")); err != nil {
		t.Fatalf("users manifest missing under legacy path: %v", err)
	}
	freshDir := filepath.Join(dir, "tables", fmt.Sprintf("%016x", 2))
	if _, err := os.Stat(filepath.Join(freshDir, "manifest")); err != nil {
		t.Fatalf("fresh manifest missing under id path %s: %v", freshDir, err)
	}
}
