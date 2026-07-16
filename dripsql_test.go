// Public API contract tests for the dripsql package.
package dripsql

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/engine"
	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func openTestDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// Rows cursor must advance with Next, copy typed values with Scan, and report no error at clean exhaustion.
func TestRows_CursorScan(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, name text NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, name) VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")

	rows, err := db.Query(ctx, "SELECT id, name FROM t")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()

	type row struct {
		id   int64
		name string
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.name); err != nil {
			t.Fatalf("Scan: %v", err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("Err after clean exhaust: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d rows, want 3", len(got))
	}
	if got[0] != (row{1, "alpha"}) || got[1] != (row{2, "beta"}) || got[2] != (row{3, "gamma"}) {
		t.Errorf("rows mismatch: %+v", got)
	}
}

// Columns returns the projected names in select order so callers can inspect schema without scanning.
func TestRows_Columns(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, kind text NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, kind) VALUES (1, 'x')")
	rows, err := db.Query(context.Background(), "SELECT id, kind FROM t")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()
	cols := rows.Columns()
	if len(cols) != 2 || cols[0] != "id" || cols[1] != "kind" {
		t.Errorf("Columns = %v, want [id kind]", cols)
	}
}

// Scan must reject a mismatched destination count instead of writing partial state.
func TestRows_Scan_DestCountMismatch(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	rows, err := db.Query(context.Background(), "SELECT id FROM t")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("expected one row")
	}
	var a, b int64
	if err := rows.Scan(&a, &b); err == nil {
		t.Fatal("expected count-mismatch error")
	}
}

// SetReadOnly must reject writes through Exec and BeginTx while still allowing reads.
func TestSetReadOnly_BlocksWrites(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	db.SetReadOnly(true)
	if _, err := db.Exec(ctx, "INSERT INTO t (id) VALUES (2)"); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Exec in read-only mode, got err=%v want ErrReadOnly", err)
	}
	if _, err := db.BeginTx(ctx); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("BeginTx in read-only mode, got err=%v want ErrReadOnly", err)
	}
	rows, err := db.Query(ctx, "SELECT id FROM t")
	if err != nil {
		t.Fatalf("Query in read-only mode: %v", err)
	}
	rows.Close()
	db.SetReadOnly(false)
	if _, err := db.Exec(ctx, "INSERT INTO t (id) VALUES (3)"); err != nil {
		t.Fatalf("Exec after toggle off: %v", err)
	}
}

// Positional placeholders bind left-to-right across types and reach both Query and Exec paths.
func TestPlaceholders_BindAcrossTypesAndPaths(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, kind text NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, kind) VALUES (1, 'a'), (2, 'b'), (3, 'a'), (4, 'c')")

	var got int64
	if err := db.QueryRow(ctx, "SELECT count(*) FROM t WHERE id >= ? AND kind = ?", int64(2), "a").Scan(&got); err != nil {
		t.Fatalf("QueryRow: %v", err)
	}
	if got != 1 {
		t.Errorf("count = %d, want 1 (id=3,kind=a matches)", got)
	}

	res, err := db.Exec(ctx, "DELETE FROM t WHERE id = ?", int64(2))
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if res.RowsAffected != 1 {
		t.Errorf("DELETE RowsAffected = %d, want 1", res.RowsAffected)
	}

	if _, err := db.Query(ctx, "SELECT id FROM t WHERE id = ?"); err == nil {
		t.Fatal("expected error when args are short of placeholder count")
	}
}

// Update commits the staged writes when fn returns nil and rolls them back when fn returns an error.
func TestUpdate_CommitsOnNilRollsBackOnError(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")

	if err := db.Update(ctx, func(tx *Tx) error {
		_, err := tx.Exec(ctx, "INSERT INTO t (id) VALUES (1), (2)")
		return err
	}); err != nil {
		t.Fatalf("Update commit: %v", err)
	}

	wantErr := fmt.Errorf("simulated failure")
	if err := db.Update(ctx, func(tx *Tx) error {
		if _, err := tx.Exec(ctx, "INSERT INTO t (id) VALUES (99)"); err != nil {
			return err
		}
		return wantErr
	}); err != wantErr {
		t.Fatalf("Update rollback err = %v, want simulated failure", err)
	}

	var n int64
	if err := db.QueryRow(ctx, "SELECT count(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("QueryRow: %v", err)
	}
	if n != 2 {
		t.Errorf("count = %d, want 2 (rolled-back insert must not survive)", n)
	}
}

// QueryRow succeeds on exactly one row and errors on zero or multiple rows so callers do not silently scan stale values.
func TestQueryRow_OneRowSucceedsZeroAndMultipleError(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1), (2)")

	var id int64
	if err := db.QueryRow(ctx, "SELECT id FROM t WHERE id = ?", int64(1)).Scan(&id); err != nil {
		t.Fatalf("one-row Scan: %v", err)
	}
	if id != 1 {
		t.Errorf("Scan = %d, want 1", id)
	}
	if err := db.QueryRow(ctx, "SELECT id FROM t WHERE id = ?", int64(999)).Scan(&id); !errors.Is(err, ErrNoRows) {
		t.Fatalf("got err=%v, want ErrNoRows", err)
	}
	if err := db.QueryRow(ctx, "SELECT id FROM t").Scan(&id); !errors.Is(err, ErrTooManyRows) {
		t.Fatalf("got err=%v, want ErrTooManyRows", err)
	}
}

// Rows.All drains unread rows on a fresh cursor, skips the row a prior Next consumed, returns empty after exhaustion, and Columns hands back a copy so caller mutation cannot corrupt engine metadata.
func TestRows_All_AndColumnsCopy(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1), (2), (3)")

	fresh, err := db.Query(ctx, "SELECT id FROM t")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	vals, err := fresh.All()
	if err != nil {
		t.Fatalf("fresh All: %v", err)
	}
	if len(vals) != 3 || vals[0][0].(int64) != 1 || vals[2][0].(int64) != 3 {
		t.Errorf("fresh All = %v, want three rows", vals)
	}
	fresh.Close()

	post, err := db.Query(ctx, "SELECT id FROM t")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	defer post.Close()
	if !post.Next() {
		t.Fatal("expected first row")
	}
	rest, err := post.All()
	if err != nil {
		t.Fatalf("post-Next All: %v", err)
	}
	if len(rest) != 2 || rest[0][0].(int64) != 2 || rest[1][0].(int64) != 3 {
		t.Errorf("post-Next All = %v, want rows 2 and 3", rest)
	}
	more, err := post.All()
	if err != nil {
		t.Fatalf("post-exhaustion All: %v", err)
	}
	if len(more) != 0 {
		t.Errorf("post-exhaustion All = %v, want empty", more)
	}
	cols := post.Columns()
	cols[0] = "stomped"
	if post.Columns()[0] != "id" {
		t.Error("Columns must return a copy")
	}
}

// QueryRow must not mutate dst when it returns ErrTooManyRows so caller state stays consistent on the error path.
func TestQueryRow_TooManyRowsDoesNotMutateDestination(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1), (2)")

	id := int64(99)
	if err := db.QueryRow(ctx, "SELECT id FROM t").Scan(&id); !errors.Is(err, ErrTooManyRows) {
		t.Fatalf("got err=%v, want ErrTooManyRows", err)
	}
	if id != 99 {
		t.Fatalf("destination mutated on ErrTooManyRows: got %d want 99", id)
	}
}

// Tx operations after Commit or Rollback return ErrTxDone so callers can detect reuse without string matching.
func TestTx_DoneAfterCommitOrRollback(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")

	tx, err := db.BeginTx(ctx)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO t (id) VALUES (1)"); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO t (id) VALUES (2)"); !errors.Is(err, ErrTxDone) {
		t.Fatalf("Exec after Commit err=%v, want ErrTxDone", err)
	}
}

// View pins a snapshot, surfaces fn's error to the caller, and stays permitted when SetReadOnly is on since it cannot stage writes.
func TestView_ReadsErrorAndReadOnly(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1), (2), (3)")

	var got int64
	if err := db.View(ctx, func(rtx *ReadTx) error {
		return rtx.QueryRow(ctx, "SELECT count(*) FROM t").Scan(&got)
	}); err != nil {
		t.Fatalf("View: %v", err)
	}
	if got != 3 {
		t.Errorf("count = %d, want 3", got)
	}

	wantErr := fmt.Errorf("read failure")
	if err := db.View(ctx, func(rtx *ReadTx) error { return wantErr }); err != wantErr {
		t.Fatalf("View error propagation: %v, want %v", err, wantErr)
	}

	db.SetReadOnly(true)
	if err := db.View(ctx, func(rtx *ReadTx) error {
		return rtx.QueryRow(ctx, "SELECT count(*) FROM t").Scan(&got)
	}); err != nil {
		t.Fatalf("View while read-only: %v", err)
	}
}

// Update must roll back staged writes when fn panics so an unhandled error path cannot leak partial state.
func TestUpdate_RollsBackOnPanic(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic to propagate out of Update")
			}
		}()
		_ = db.Update(ctx, func(tx *Tx) error {
			if _, err := tx.Exec(ctx, "INSERT INTO t (id) VALUES (1)"); err != nil {
				return err
			}
			panic("boom")
		})
	}()

	var n int64
	if err := db.QueryRow(ctx, "SELECT count(*) FROM t").Scan(&n); err != nil {
		t.Fatalf("QueryRow: %v", err)
	}
	if n != 0 {
		t.Fatalf("count after panic = %d, want 0 (rollback)", n)
	}
}

// Open rejects the empty path so a missing config value cannot silently land on the wrong directory.
func TestOpen_EmptyPathRejected(t *testing.T) {
	if _, err := Open(""); err == nil {
		t.Fatal("Open(\"\") must return an error")
	}
}

// MemoryPath returns an explicit not-implemented error so callers learn before relying on it.
func TestOpen_MemoryPathNotYet(t *testing.T) {
	if _, err := Open(MemoryPath); err == nil {
		t.Fatal("Open(MemoryPath) must error until the in-memory backend lands")
	}
}

// Chunk narrow accessors expose int32, int16, bool, and date columns with NULLs visible through IsNull and wrong kinds returning nil.
func TestQueryChunks_NarrowAccessors(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, a int32, b int16, c bool, d date)")

	// SQL INSERT cannot yet bind date literals end to end, so the rows arrive through the engine ingest path.
	valid := vector.NewValidity(3)
	valid.SetInvalid(2)
	idv := vector.NewVec(vector.VecInt64, 3)
	copy(idv.I64(), []int64{1, 2, 3})
	av := vector.NewVec(vector.VecInt32, 3)
	copy(av.I32(), []int32{7, -2, 0})
	av.Valid = valid
	bv := vector.NewVec(vector.VecInt16, 3)
	copy(bv.I16(), []int16{3, -4, 0})
	bv.Valid = valid
	cv := vector.NewVec(vector.VecBool, 3)
	cv.BoolBits()[0] = 0b001
	cv.Valid = valid
	dv := vector.NewVec(vector.VecDate, 3)
	copy(dv.I32(), []int32{19000, 20000, 0})
	dv.Valid = valid
	batch, err := vector.NewBatch([]vector.Column{
		{Name: "id", Type: schema.Int64, V: idv},
		{Name: "a", Type: schema.Int32, V: av},
		{Name: "b", Type: schema.Int16, V: bv},
		{Name: "c", Type: schema.Bool, V: cv},
		{Name: "d", Type: schema.Date, V: dv},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	if _, err := db.e.Ingest(ctx, engine.IngestConfig{Table: "t", Batches: []vector.Batch{batch}}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	chunks, err := db.QueryChunks(ctx, "SELECT a, b, c, d FROM t")
	if err != nil {
		t.Fatalf("QueryChunks: %v", err)
	}
	if len(chunks) != 1 || chunks[0].Len() != 3 {
		t.Fatalf("got %d chunks, want one chunk of 3 rows", len(chunks))
	}
	ch := chunks[0]
	a := ch.Ints32("a")
	if len(a) != 3 || a[0] != 7 || a[1] != -2 {
		t.Errorf("Ints32 = %v, want 7 and -2 in the first two rows", a)
	}
	b := ch.Ints16("b")
	if len(b) != 3 || b[0] != 3 || b[1] != -4 {
		t.Errorf("Ints16 = %v, want 3 and -4 in the first two rows", b)
	}
	bo := ch.Bools("c")
	if len(bo) != 3 || !bo[0] || bo[1] {
		t.Errorf("Bools = %v, want true then false in the first two rows", bo)
	}
	d := ch.Dates("d")
	if len(d) != 3 || d[0] != 19000 || d[1] != 20000 {
		t.Errorf("Dates = %v, want 19000 and 20000 in the first two rows", d)
	}
	for _, col := range []string{"a", "b", "c", "d"} {
		if ch.IsNull(col, 0) {
			t.Errorf("IsNull(%s, 0) = true, want false", col)
		}
		if !ch.IsNull(col, 2) {
			t.Errorf("IsNull(%s, 2) = false, want true", col)
		}
	}
	if ch.Ints16("c") != nil || ch.Dates("a") != nil || ch.Bools("b") != nil || ch.Ints32("missing") != nil {
		t.Error("wrong-kind or absent accessors must return nil")
	}
}

func mustExec(t *testing.T, db *DB, sql string) {
	t.Helper()
	if _, err := db.Exec(context.Background(), sql); err != nil {
		t.Fatalf("Exec %q: %v", sql, err)
	}
}
