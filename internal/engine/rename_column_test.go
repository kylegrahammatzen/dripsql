// End-to-end check that ALTER TABLE RENAME COLUMN is metadata only: segment files
// on disk keep the old column name, scans resolve by column id, and the new name is
// queryable immediately and after reopen.
package engine

import (
	"context"
	"testing"
)

func TestEngine_RenameColumn_MetadataOnly(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id int64 NOT NULL, name text NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b')"); err != nil {
		t.Fatal(err)
	}
	bd, err := db.boundTableByName("t")
	if err != nil {
		t.Fatal(err)
	}
	var renamedID uint64
	for _, c := range bd.Columns {
		if c.Name == "name" {
			renamedID = uint64(c.ID)
		}
	}
	if renamedID == 0 {
		t.Fatal("could not locate column id for 'name'")
	}

	if _, err := db.Exec(ctx, "ALTER TABLE t RENAME COLUMN name TO label"); err != nil {
		t.Fatalf("rename: %v", err)
	}

	rows := mustValues(t, db, "SELECT id, label FROM t ORDER BY id")
	wantRows(t, rows, [][]any{
		{int64(1), "a"},
		{int64(2), "b"},
	})

	bd2, err := db.boundTableByName("t")
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range bd2.Columns {
		if c.Name == "name" {
			t.Fatal("old name 'name' still visible after rename")
		}
		if c.Name == "label" && uint64(c.ID) != renamedID {
			t.Fatalf("renamed column id changed: %d -> %d", renamedID, c.ID)
		}
	}

	db.Close()
	db, err = Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	rows = mustValues(t, db, "SELECT id, label FROM t ORDER BY id")
	wantRows(t, rows, [][]any{
		{int64(1), "a"},
		{int64(2), "b"},
	})

	if _, err := db.Query(ctx, "SELECT name FROM t"); err == nil {
		t.Fatal("expected error querying by old column name")
	}
}

func TestEngine_RenameColumn_RejectsNameCollision(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id int64 NOT NULL, name text NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "ALTER TABLE t RENAME COLUMN name TO id"); err == nil {
		t.Fatal("expected error when renaming to an existing column name")
	}
}

func TestEngine_RenameColumn_RejectsUnknownColumn(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id int64 NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "ALTER TABLE t RENAME COLUMN missing TO present"); err == nil {
		t.Fatal("expected error when renaming a missing column")
	}
}
