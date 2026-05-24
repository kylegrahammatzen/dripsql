// AVG aggregate end-to-end: returns Float64 via sum/count, NULL when no rows match.
package engine

import "testing"

func TestAggregateAvg_NoGroup(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30)")
	got := mustValues(t, db, "SELECT avg(v) FROM t")
	if len(got) != 1 || len(got[0]) != 1 {
		t.Fatalf("expected 1 row 1 col, got %v", got)
	}
	if f, ok := got[0][0].(float64); !ok || f != 20.0 {
		t.Fatalf("avg = %v (%T), want 20.0 (float64)", got[0][0], got[0][0])
	}
}

func TestAggregateAvg_GroupBy(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (k text NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (k, v) VALUES ('a', 10), ('a', 30), ('b', 5), ('b', 15), ('b', 25)")
	got := mustValues(t, db, "SELECT k, avg(v) FROM t GROUP BY k ORDER BY k")
	want := [][]any{
		{"a", 20.0},
		{"b", 15.0},
	}
	wantRows(t, got, want)
}

func TestAggregateAvg_EmptyIsNull(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (v int64 NOT NULL)")
	got := mustValues(t, db, "SELECT avg(v) FROM t")
	if len(got) != 1 || len(got[0]) != 1 {
		t.Fatalf("expected 1 row 1 col, got %v", got)
	}
	if got[0][0] != nil {
		t.Fatalf("avg of empty = %v, want nil", got[0][0])
	}
}

func TestAggregateAvg_CombinedWithCountAndSum(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (k int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (k, v) VALUES (1, 10), (1, 20), (1, 30)")
	got := mustValues(t, db, "SELECT count(*), sum(v), avg(v) FROM t")
	if len(got) != 1 || len(got[0]) != 3 {
		t.Fatalf("expected 1 row 3 cols, got %v", got)
	}
	if got[0][0] != int64(3) {
		t.Errorf("count = %v, want 3", got[0][0])
	}
	if got[0][1] != int64(60) {
		t.Errorf("sum = %v, want 60", got[0][1])
	}
	if f, ok := got[0][2].(float64); !ok || f != 20.0 {
		t.Errorf("avg = %v (%T), want 20.0 (float64)", got[0][2], got[0][2])
	}
}
