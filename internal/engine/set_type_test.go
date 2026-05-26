// ALTER COLUMN ... TYPE widens an existing column. Old segments still hold the narrow
// kind; the scan path casts on read. Narrowing or unrelated casts are rejected.
package engine

import (
	"context"
	"testing"
)

func TestEngine_SetType_WidenInt32ToInt64(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id int32 NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t (id) VALUES (1), (2), (3)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "ALTER TABLE t ALTER COLUMN id TYPE int64"); err != nil {
		t.Fatalf("alter: %v", err)
	}

	rows := mustValues(t, db, "SELECT id FROM t ORDER BY id")
	wantRows(t, rows, [][]any{
		{int64(1)},
		{int64(2)},
		{int64(3)},
	})

	if _, err := db.Exec(ctx, "INSERT INTO t (id) VALUES (4)"); err != nil {
		t.Fatal(err)
	}
	rows = mustValues(t, db, "SELECT id FROM t ORDER BY id")
	wantRows(t, rows, [][]any{
		{int64(1)},
		{int64(2)},
		{int64(3)},
		{int64(4)},
	})
}

func TestEngine_SetType_WidenFloat32ToFloat64(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id int64 NOT NULL, price float32 NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t (id, price) VALUES (1, 1.5), (2, 2.25)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "ALTER TABLE t ALTER COLUMN price TYPE float64"); err != nil {
		t.Fatalf("alter: %v", err)
	}
	rows := mustValues(t, db, "SELECT id, price FROM t ORDER BY id")
	wantRows(t, rows, [][]any{
		{int64(1), float64(1.5)},
		{int64(2), float64(2.25)},
	})
}

func TestEngine_SetType_WidenInt16ToInt64(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id int16 NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t (id) VALUES (1), (2), (3)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "ALTER TABLE t ALTER COLUMN id TYPE int64"); err != nil {
		t.Fatalf("alter: %v", err)
	}
	rows := mustValues(t, db, "SELECT id FROM t ORDER BY id")
	wantRows(t, rows, [][]any{
		{int64(1)}, {int64(2)}, {int64(3)},
	})
}

func TestEngine_SetType_WidenInt32ToFloat64(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id int32 NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t (id) VALUES (5), (10)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "ALTER TABLE t ALTER COLUMN id TYPE float64"); err != nil {
		t.Fatal(err)
	}
	rows := mustValues(t, db, "SELECT id FROM t ORDER BY id")
	wantRows(t, rows, [][]any{
		{float64(5)}, {float64(10)},
	})
}

func TestEngine_SetType_RejectsNarrowing(t *testing.T) {
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
	if _, err := db.Exec(ctx, "ALTER TABLE t ALTER COLUMN id TYPE int32"); err == nil {
		t.Fatal("expected error for narrowing cast")
	}
}

func TestEngine_SetType_RejectsUnrelatedCast(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id int32 NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "ALTER TABLE t ALTER COLUMN id TYPE text"); err == nil {
		t.Fatal("expected error for unsupported cast")
	}
}
