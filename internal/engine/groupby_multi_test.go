// Multi-column GROUP BY via the composite-key aggregate path. Mix of int + text keys,
// composes with COUNT(*). Same shape as DISTINCT but with explicit aggregates.
package engine

import (
	"testing"
)

func TestGroupBy_TwoIntColumns(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (a int64 NOT NULL, b int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (a, b) VALUES (1, 10)")
	mustExec(t, db, "INSERT INTO t (a, b) VALUES (1, 10)")
	mustExec(t, db, "INSERT INTO t (a, b) VALUES (1, 20)")
	mustExec(t, db, "INSERT INTO t (a, b) VALUES (2, 10)")

	rows := mustValues(t, db, "SELECT a, b, count(*) FROM t GROUP BY a, b")
	if len(rows) != 3 {
		t.Fatalf("got %d groups, want 3", len(rows))
	}
	// Build a (a,b) -> count map from rows.
	counts := map[[2]int64]int64{}
	for _, r := range rows {
		key := [2]int64{r[0].(int64), r[1].(int64)}
		counts[key] = r[2].(int64)
	}
	want := map[[2]int64]int64{
		{1, 10}: 2,
		{1, 20}: 1,
		{2, 10}: 1,
	}
	for k, v := range want {
		if counts[k] != v {
			t.Errorf("group %v: count=%d, want %d", k, counts[k], v)
		}
	}
}

func TestGroupBy_IntPlusText(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, cat text NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, cat) VALUES (1, 'red')")
	mustExec(t, db, "INSERT INTO t (id, cat) VALUES (1, 'red')")
	mustExec(t, db, "INSERT INTO t (id, cat) VALUES (1, 'blue')")
	mustExec(t, db, "INSERT INTO t (id, cat) VALUES (2, 'red')")
	rows := mustValues(t, db, "SELECT id, cat, count(*) FROM t GROUP BY id, cat")
	if len(rows) != 3 {
		t.Fatalf("got %d groups, want 3", len(rows))
	}
	type key struct {
		id  int64
		cat string
	}
	counts := map[key]int64{}
	for _, r := range rows {
		counts[key{r[0].(int64), r[1].(string)}] = r[2].(int64)
	}
	if counts[key{1, "red"}] != 2 {
		t.Errorf("(1,red): got %d, want 2", counts[key{1, "red"}])
	}
	if counts[key{1, "blue"}] != 1 {
		t.Errorf("(1,blue): got %d, want 1", counts[key{1, "blue"}])
	}
	if counts[key{2, "red"}] != 1 {
		t.Errorf("(2,red): got %d, want 1", counts[key{2, "red"}])
	}
}

func TestGroupBy_SingleColumnStillWorks(t *testing.T) {
	// Regression: single-col fast path (int key) still uses intIdx, not the
	// composite path. This fires on the typed paths only.
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")
	rows := mustValues(t, db, "SELECT id, count(*) FROM t GROUP BY id")
	if len(rows) != 2 {
		t.Fatalf("single-col GROUP BY got %d, want 2", len(rows))
	}
}
