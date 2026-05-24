// SELECT DISTINCT lowers to GROUP BY of the same expression. Multi-column DISTINCT
// rejected loudly until composite-key GROUP BY lands.
package engine

import (
	"context"
	"testing"
)

func TestDistinct_DedupsRows(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (3)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")

	rows := mustValues(t, db, "SELECT DISTINCT id FROM t ORDER BY id")
	if len(rows) != 3 {
		t.Fatalf("DISTINCT got %d rows, want 3", len(rows))
	}
	got := []int64{rows[0][0].(int64), rows[1][0].(int64), rows[2][0].(int64)}
	want := []int64{1, 2, 3}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d: got %d, want %d", i, got[i], want[i])
		}
	}
}

func TestDistinct_MultiColumn(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (a int64 NOT NULL, b int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (a, b) VALUES (1, 10)")
	mustExec(t, db, "INSERT INTO t (a, b) VALUES (1, 10)")
	mustExec(t, db, "INSERT INTO t (a, b) VALUES (1, 20)")
	mustExec(t, db, "INSERT INTO t (a, b) VALUES (2, 10)")
	mustExec(t, db, "INSERT INTO t (a, b) VALUES (2, 10)")
	rows := mustValues(t, db, "SELECT DISTINCT a, b FROM t")
	if len(rows) != 3 {
		t.Fatalf("multi-col DISTINCT got %d rows, want 3 (distinct pairs)", len(rows))
	}
}

func TestDistinct_StarRejected(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	if _, err := db.Query(context.Background(), "SELECT DISTINCT * FROM t"); err == nil {
		t.Fatal("SELECT DISTINCT * should error")
	}
}
