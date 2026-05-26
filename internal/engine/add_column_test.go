// ALTER TABLE ADD COLUMN appends a nullable column; pre-existing segments do not
// contain its column id and the scan path projects NULL for those rows.
package engine

import (
	"context"
	"testing"
)

func TestEngine_AddColumn_NullForOldRows(t *testing.T) {
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
	if _, err := db.Exec(ctx, "INSERT INTO t (id) VALUES (1), (2), (3)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "ALTER TABLE t ADD COLUMN note text"); err != nil {
		t.Fatalf("add column: %v", err)
	}

	rows := mustValues(t, db, "SELECT id, note FROM t ORDER BY id")
	wantRows(t, rows, [][]any{
		{int64(1), nil},
		{int64(2), nil},
		{int64(3), nil},
	})

	if _, err := db.Exec(ctx, "INSERT INTO t (id, note) VALUES (4, 'hi')"); err != nil {
		t.Fatal(err)
	}
	rows = mustValues(t, db, "SELECT id, note FROM t ORDER BY id")
	wantRows(t, rows, [][]any{
		{int64(1), nil},
		{int64(2), nil},
		{int64(3), nil},
		{int64(4), "hi"},
	})
}

func TestEngine_AddColumn_RejectsDuplicateName(t *testing.T) {
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
	if _, err := db.Exec(ctx, "ALTER TABLE t ADD COLUMN id text"); err == nil {
		t.Fatal("expected error for duplicate column name")
	}
}

func TestEngine_AddColumn_DefaultFillsOldRows(t *testing.T) {
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
	if _, err := db.Exec(ctx, "INSERT INTO t (id) VALUES (1), (2)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "ALTER TABLE t ADD COLUMN tag text NOT NULL DEFAULT 'unset'"); err != nil {
		t.Fatalf("add column: %v", err)
	}
	rows := mustValues(t, db, "SELECT id, tag FROM t ORDER BY id")
	wantRows(t, rows, [][]any{
		{int64(1), "unset"},
		{int64(2), "unset"},
	})
}

func TestEngine_AddColumn_NotNullRequiresDefault(t *testing.T) {
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
	if _, err := db.Exec(ctx, "ALTER TABLE t ADD COLUMN tag text NOT NULL"); err == nil {
		t.Fatal("expected error for NOT NULL ADD COLUMN without DEFAULT")
	}
}

func TestEngine_AddColumn_NumericDefault(t *testing.T) {
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
	if _, err := db.Exec(ctx, "INSERT INTO t (id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "ALTER TABLE t ADD COLUMN count int64 NOT NULL DEFAULT 0"); err != nil {
		t.Fatal(err)
	}
	rows := mustValues(t, db, "SELECT id, count FROM t")
	wantRows(t, rows, [][]any{{int64(1), int64(0)}})
}

func TestEngine_AddColumn_SurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id int64 NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t (id) VALUES (10)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "ALTER TABLE t ADD COLUMN note text"); err != nil {
		t.Fatal(err)
	}
	db.Close()
	db, err = Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	rows := mustValues(t, db, "SELECT id, note FROM t")
	wantRows(t, rows, [][]any{{int64(10), nil}})
}
