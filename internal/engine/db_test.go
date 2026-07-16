// Table path routing tests, new tables land under tables/<id> and migrated tables stay on segments/<name>.
package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

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
