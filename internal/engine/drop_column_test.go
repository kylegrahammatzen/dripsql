// ALTER TABLE DROP COLUMN tombstones the column in the catalog. Bytes stay on disk
// (compaction comes later); the binder/scan stop exposing it; the column id is
// retired and the freed name may be reused by ADD COLUMN.
package engine

import (
	"context"
	"testing"
)

func TestEngine_DropColumn_HidesFromSelect(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id int64 NOT NULL, name text NOT NULL, age int64 NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t (id, name, age) VALUES (1, 'a', 20), (2, 'b', 30)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "ALTER TABLE t DROP COLUMN age"); err != nil {
		t.Fatalf("drop: %v", err)
	}

	if _, err := db.Query(ctx, "SELECT age FROM t"); err == nil {
		t.Fatal("expected error referencing dropped column")
	}

	rows := mustValues(t, db, "SELECT id, name FROM t ORDER BY id")
	wantRows(t, rows, [][]any{
		{int64(1), "a"},
		{int64(2), "b"},
	})

	cols, err := db.TableSchema("t")
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != 2 {
		t.Fatalf("active columns = %d, want 2: %+v", len(cols), cols)
	}
}

func TestEngine_DropColumn_RejectsLastActiveColumn(t *testing.T) {
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
	if _, err := db.Exec(ctx, "ALTER TABLE t DROP COLUMN id"); err == nil {
		t.Fatal("expected error dropping the last active column")
	}
}

func TestEngine_DropColumn_FreesNameForAddColumn(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id int64 NOT NULL, note text NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t (id, note) VALUES (1, 'a')"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "ALTER TABLE t DROP COLUMN note"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "ALTER TABLE t ADD COLUMN note text"); err != nil {
		t.Fatalf("re-add: %v", err)
	}
	rows := mustValues(t, db, "SELECT id, note FROM t")
	wantRows(t, rows, [][]any{{int64(1), nil}})
}
