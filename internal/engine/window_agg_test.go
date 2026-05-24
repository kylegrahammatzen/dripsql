// Aggregate-as-window: SUM/COUNT/MIN/MAX/AVG OVER (PARTITION BY ... ORDER BY ...).
// With ORDER BY: running aggregate. Without: full-partition aggregate.
package engine

import "testing"

func TestWindowAgg_SumPartitionRunning(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (k text NOT NULL, v int64 NOT NULL, ord int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (k, v, ord) VALUES ('a', 1, 1), ('a', 2, 2), ('a', 3, 3), ('b', 10, 1)")
	got := mustValues(t, db, "SELECT k, ord, sum(v) OVER (PARTITION BY k ORDER BY ord) AS rs FROM t ORDER BY k, ord")
	want := [][]any{
		{"a", int64(1), int64(1)},
		{"a", int64(2), int64(3)},
		{"a", int64(3), int64(6)},
		{"b", int64(1), int64(10)},
	}
	wantRows(t, got, want)
}

func TestWindowAgg_SumNoOrderBy(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (k text NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (k, v) VALUES ('a', 1), ('a', 2), ('a', 3), ('b', 10)")
	got := mustValues(t, db, "SELECT k, v, sum(v) OVER (PARTITION BY k) AS s FROM t ORDER BY k, v")
	want := [][]any{
		{"a", int64(1), int64(6)},
		{"a", int64(2), int64(6)},
		{"a", int64(3), int64(6)},
		{"b", int64(10), int64(10)},
	}
	wantRows(t, got, want)
}

func TestWindowAgg_CountStar(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (k text NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (k, v) VALUES ('a', 1), ('a', 2), ('a', 3), ('b', 10), ('b', 20)")
	got := mustValues(t, db, "SELECT k, v, count(*) OVER (PARTITION BY k) AS c FROM t ORDER BY k, v")
	want := [][]any{
		{"a", int64(1), int64(3)},
		{"a", int64(2), int64(3)},
		{"a", int64(3), int64(3)},
		{"b", int64(10), int64(2)},
		{"b", int64(20), int64(2)},
	}
	wantRows(t, got, want)
}

func TestWindowAgg_AvgRunning(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30)")
	got := mustValues(t, db, "SELECT id, avg(v) OVER (ORDER BY id) AS a FROM t ORDER BY id")
	want := [][]any{
		{int64(1), 10.0},
		{int64(2), 15.0},
		{int64(3), 20.0},
	}
	wantRows(t, got, want)
}

func TestWindowAgg_MinMax(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (k text NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (k, v) VALUES ('a', 5), ('a', 2), ('a', 8), ('b', 100)")
	got := mustValues(t, db, "SELECT k, min(v) OVER (PARTITION BY k) AS mn, max(v) OVER (PARTITION BY k) AS mx FROM t ORDER BY k, v")
	want := [][]any{
		{"a", int64(2), int64(8)},
		{"a", int64(2), int64(8)},
		{"a", int64(2), int64(8)},
		{"b", int64(100), int64(100)},
	}
	wantRows(t, got, want)
}
