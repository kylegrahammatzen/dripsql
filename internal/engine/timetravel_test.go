// PR-V4: QueryAt runs against a historical snapshot pinned to a commit_ts. AS OF the
// pre-INSERT timestamp sees only earlier writes. AS OF the latest sees everything.
package engine

import (
	"context"
	"testing"
)

func TestQueryAt_HistoricalSnapshot(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	tsAfterFirst := db.nextCommitTs.Load()
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (3)")

	rows, err := db.QueryAt(context.Background(), "SELECT id FROM t", tsAfterFirst)
	if err != nil {
		t.Fatalf("QueryAt(tsAfterFirst): %v", err)
	}
	if len(rows.Values) != 1 {
		t.Fatalf("QueryAt(tsAfterFirst) got %d rows, want 1", len(rows.Values))
	}

	latest := mustValues(t, db, "SELECT id FROM t")
	if len(latest) != 3 {
		t.Fatalf("latest got %d rows, want 3", len(latest))
	}
}

func TestQueryAt_TsZeroReturnsEmpty(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	rows, err := db.QueryAt(context.Background(), "SELECT id FROM t", 0)
	if err != nil {
		t.Fatalf("QueryAt(0): %v", err)
	}
	if len(rows.Values) != 0 {
		t.Fatalf("QueryAt(0) got %d rows, want 0 (pre-history snapshot)", len(rows.Values))
	}
}

func TestQueryAt_DeletePreservedAtEarlierTs(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")
	tsBeforeDelete := db.nextCommitTs.Load()
	mustExec(t, db, "DELETE FROM t WHERE id = 1")

	preDelete, err := db.QueryAt(context.Background(), "SELECT id FROM t", tsBeforeDelete)
	if err != nil {
		t.Fatalf("QueryAt(tsBeforeDelete): %v", err)
	}
	if len(preDelete.Values) != 2 {
		t.Fatalf("AS OF before delete got %d rows, want 2", len(preDelete.Values))
	}

	postDelete := mustValues(t, db, "SELECT id FROM t")
	if len(postDelete) != 1 {
		t.Fatalf("latest after delete got %d rows, want 1", len(postDelete))
	}
}
