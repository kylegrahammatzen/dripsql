// Tests that exercise the v2 catalog wiring end to end. Covers v1 -> v2 migration on
// Open, stable column IDs across reopen, and plan-cache invalidation on generation bump.
package engine

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
)

const v1UsersCatalog = `{
  "version": 1,
  "types": null,
  "tables": [
    {
      "id": 1,
      "spec": {
        "Name": "users",
        "IfNotExists": false,
        "Columns": [
          { "Name": "id",   "Type": { "Kind": 4, "Name": "" }, "Nullable": false, "Codec": 0 },
          { "Name": "name", "Type": { "Kind": 8, "Name": "" }, "Nullable": false, "Codec": 0 }
        ],
        "Options": {
          "Storage": 0, "Profile": 0,
          "SegmentRows": { "Auto": false, "Rows": 0 },
          "SortBy": null, "Compression": 0, "TimeColumn": ""
        }
      }
    }
  ]
}`

func TestEngine_CatalogV1Migration(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "segments", "users"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "catalog.json"), []byte(v1UsersCatalog), 0o644); err != nil {
		t.Fatal(err)
	}

	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	cols, err := db.TableSchema("users")
	if err != nil {
		t.Fatalf("TableSchema: %v", err)
	}
	if len(cols) != 2 || cols[0].Name != "id" || cols[0].Type != "int64" || cols[1].Name != "name" || cols[1].Type != "text" {
		t.Fatalf("unexpected schema: %+v", cols)
	}

	raw, err := os.ReadFile(filepath.Join(dir, "catalog.json"))
	if err != nil {
		t.Fatal(err)
	}
	var f catalog.File
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("post-migration catalog does not parse as v2: %v", err)
	}
	if f.FormatVersion != catalog.CurrentFormatVersion {
		t.Fatalf("FormatVersion = %d, want %d", f.FormatVersion, catalog.CurrentFormatVersion)
	}
	if len(f.Tables) != 1 || f.Tables[0].TableID != 1 || f.Tables[0].Columns[0].ColumnID != 1 {
		t.Fatalf("migrated tables: %+v", f.Tables)
	}
	if strings.Contains(string(raw), "IfNotExists") {
		t.Fatal("migrated catalog still contains IfNotExists")
	}
	if strings.Contains(string(raw), `"Kind": 4`) {
		t.Fatal("migrated catalog still contains integer enum")
	}
	if _, err := os.Stat(filepath.Join(dir, "catalog.json.bak")); err != nil {
		t.Fatalf("catalog.json.bak not preserved: %v", err)
	}
}

func TestEngine_ColumnIDStable(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := db.Exec(context.Background(), "CREATE TABLE t (id int64 NOT NULL, name text NOT NULL, age int64 NOT NULL)"); err != nil {
		t.Fatalf("CREATE: %v", err)
	}

	captured := captureColumnIDs(t, db)
	db.Close()

	db, err = Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	got := captureColumnIDs(t, db)
	if len(got) != len(captured) {
		t.Fatalf("len mismatch: got %d, want %d", len(got), len(captured))
	}
	for k, v := range captured {
		if got[k] != v {
			t.Fatalf("column %q id = %d, want %d", k, got[k], v)
		}
	}
}

func captureColumnIDs(t *testing.T, db *DB) map[string]sql.ColumnID {
	t.Helper()
	bd, err := db.boundTableByName("t")
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]sql.ColumnID, len(bd.Columns))
	for _, c := range bd.Columns {
		out[c.Name] = c.ID
	}
	return out
}

func TestEngine_PlanCacheInvalidationOnGenerationBump(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	gen0 := db.catalog.Generation
	if _, err := db.Exec(context.Background(), "CREATE TABLE a (id int64 NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	gen1 := db.catalog.Generation
	if gen1 <= gen0 {
		t.Fatalf("generation did not advance: %d -> %d", gen0, gen1)
	}
	if sql.SchemaVersion(gen1) != db.version {
		t.Fatalf("db.version %d != generation %d", db.version, gen1)
	}
}
