// PR-V3: BeginTx + Exec/Query/Commit/Rollback. Reads inside the txn see staged writes,
// reads outside don't. Commit lands every staged table under one commit_ts. Rollback
// discards files and leaves the manifest untouched.
package engine

import (
	"context"
	"os"
	"testing"
)

func TestTx_InsertCommit_VisibleAfter(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	tx, err := db.BeginTx(context.Background())
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.Exec(context.Background(), "INSERT INTO t (id) VALUES (1)"); err != nil {
		t.Fatalf("tx.Exec: %v", err)
	}
	if _, err := tx.Exec(context.Background(), "INSERT INTO t (id) VALUES (2)"); err != nil {
		t.Fatalf("tx.Exec 2: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	got := mustValues(t, db, "SELECT id FROM t")
	if len(got) != 2 {
		t.Fatalf("after commit got %d rows, want 2", len(got))
	}
}

func TestTx_InsertRollback_DiscardsRows(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	tx, err := db.BeginTx(context.Background())
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.Exec(context.Background(), "INSERT INTO t (id) VALUES (1)"); err != nil {
		t.Fatalf("tx.Exec: %v", err)
	}
	pendingFiles := append([]string(nil), tx.files...)
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	for _, f := range pendingFiles {
		if _, err := os.Stat(f); !os.IsNotExist(err) {
			t.Errorf("rollback left file %q (err=%v)", f, err)
		}
	}
	got := mustValues(t, db, "SELECT id FROM t")
	if len(got) != 0 {
		t.Fatalf("after rollback got %d rows, want 0", len(got))
	}
}

func TestTx_ReadYourOwnWrites(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	tx, err := db.BeginTx(context.Background())
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(context.Background(), "INSERT INTO t (id) VALUES (2)"); err != nil {
		t.Fatalf("tx.Exec: %v", err)
	}
	rows, err := tx.Query(context.Background(), "SELECT id FROM t")
	if err != nil {
		t.Fatalf("tx.Query: %v", err)
	}
	if len(rows.Values) != 2 {
		t.Fatalf("read-your-own-writes got %d rows, want 2", len(rows.Values))
	}
}

func TestTx_DeleteWithinTxn_VisibleInside_HiddenOutside(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")
	tx, err := db.BeginTx(context.Background())
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.Exec(context.Background(), "DELETE FROM t WHERE id = 1"); err != nil {
		t.Fatalf("tx.Exec DELETE: %v", err)
	}
	inside, err := tx.Query(context.Background(), "SELECT id FROM t")
	if err != nil {
		t.Fatalf("tx.Query: %v", err)
	}
	if len(inside.Values) != 1 {
		t.Fatalf("inside txn after DELETE got %d rows, want 1", len(inside.Values))
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	outside := mustValues(t, db, "SELECT id FROM t")
	if len(outside) != 2 {
		t.Fatalf("after rollback got %d rows, want 2 (delete must be discarded)", len(outside))
	}
}

func TestTx_MultiTableInsert(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE a (id int64 NOT NULL)")
	mustExec(t, db, "CREATE TABLE b (id int64 NOT NULL)")
	tx, err := db.BeginTx(context.Background())
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.Exec(context.Background(), "INSERT INTO a (id) VALUES (1)"); err != nil {
		t.Fatalf("tx.Exec a: %v", err)
	}
	if _, err := tx.Exec(context.Background(), "INSERT INTO b (id) VALUES (10)"); err != nil {
		t.Fatalf("tx.Exec b: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	aRows := mustValues(t, db, "SELECT id FROM a")
	bRows := mustValues(t, db, "SELECT id FROM b")
	if len(aRows) != 1 || len(bRows) != 1 {
		t.Fatalf("multi-table commit: a=%d rows b=%d rows, want 1 each", len(aRows), len(bRows))
	}
}

func TestTx_MultiTableRollback(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE a (id int64 NOT NULL)")
	mustExec(t, db, "CREATE TABLE b (id int64 NOT NULL)")
	tx, err := db.BeginTx(context.Background())
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.Exec(context.Background(), "INSERT INTO a (id) VALUES (1)"); err != nil {
		t.Fatalf("tx.Exec a: %v", err)
	}
	if _, err := tx.Exec(context.Background(), "INSERT INTO b (id) VALUES (10)"); err != nil {
		t.Fatalf("tx.Exec b: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if len(mustValues(t, db, "SELECT id FROM a")) != 0 {
		t.Fatal("after rollback a should be empty")
	}
	if len(mustValues(t, db, "SELECT id FROM b")) != 0 {
		t.Fatal("after rollback b should be empty")
	}
}

func TestTx_AssignsSingleCommitTs(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	beforeTs := db.nextCommitTs.Load()
	tx, err := db.BeginTx(context.Background())
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if _, err := tx.Exec(context.Background(), "INSERT INTO t (id) VALUES (1)"); err != nil {
		t.Fatalf("tx.Exec: %v", err)
	}
	if _, err := tx.Exec(context.Background(), "INSERT INTO t (id) VALUES (2)"); err != nil {
		t.Fatalf("tx.Exec 2: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	afterTs := db.nextCommitTs.Load()
	if afterTs <= beforeTs {
		t.Fatalf("nextCommitTs did not advance: before=%d after=%d", beforeTs, afterTs)
	}
}
