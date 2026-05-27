// Metadata-only ALTER plus per-column codec tests covering ADD, DROP, RENAME, ALTER COLUMN TYPE widening, and the user-declared codec override on CREATE TABLE.
package engine

import (
	"context"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
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

func TestEngine_RenameColumn_WhereStillResolves(t *testing.T) {
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
	if _, err := db.Exec(ctx, "INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "ALTER TABLE t RENAME COLUMN name TO label"); err != nil {
		t.Fatal(err)
	}

	rows := mustValues(t, db, "SELECT id FROM t WHERE label = 'b'")
	wantRows(t, rows, [][]any{{int64(2)}})

	rows = mustValues(t, db, "SELECT label FROM t WHERE id > 1 ORDER BY id")
	wantRows(t, rows, [][]any{{"b"}, {"c"}})
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

func TestEngine_UserCodec_PlainForcedOnDictionaryFriendlyColumn(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id int64, label text WITH (codec = 'plain'))"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t VALUES (1, 'alpha'),(2, 'alpha'),(3, 'beta')"); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Query(ctx, "SELECT id, label FROM t")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Values) != 3 {
		t.Fatalf("got %d rows want 3", len(rows.Values))
	}

	segs, err := db.openSegmentsForQuery("t")
	if err != nil {
		t.Fatal(err)
	}
	if len(segs) != 1 {
		t.Fatalf("got %d segments want 1", len(segs))
	}
	seg := segs[0]
	labelCol := -1
	for i := range seg.Cols {
		if seg.Cols[i].Name == "label" {
			labelCol = i
			break
		}
	}
	if labelCol < 0 {
		t.Fatal("label column not in segment")
	}
	page := seg.Cols[labelCol].Pages[0]
	got := schema.Encoding(page.Encoding)
	if !got.Valid() {
		t.Fatalf("page encoding %d unknown", page.Encoding)
	}
	if got != schema.EncPlain {
		t.Fatalf("label codec = %v, want EncPlain (plain)", got)
	}
	_ = storage.MagicLen
}

func TestEngine_UserCodec_UnknownCodecRejected(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	_, err = db.Exec(ctx, "CREATE TABLE t (id int64 WITH (codec = 'nonsense'))")
	if err == nil {
		t.Fatal("expected error on unknown codec, got nil")
	}
}
