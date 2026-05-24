// Window frame clauses: ROWS BETWEEN n PRECEDING/CURRENT ROW AND n FOLLOWING/CURRENT ROW.
package engine

import "testing"

func TestWindowFrame_RowsPrecedingFollowing(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
	got := mustValues(t, db, "SELECT id, sum(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s FROM t ORDER BY id")
	want := [][]any{
		{int64(1), int64(30)},
		{int64(2), int64(60)},
		{int64(3), int64(90)},
		{int64(4), int64(120)},
		{int64(5), int64(90)},
	}
	wantRows(t, got, want)
}

func TestWindowFrame_RowsUnboundedPrecedingCurrentRow(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30)")
	got := mustValues(t, db, "SELECT id, sum(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rs FROM t ORDER BY id")
	want := [][]any{
		{int64(1), int64(10)},
		{int64(2), int64(30)},
		{int64(3), int64(60)},
	}
	wantRows(t, got, want)
}

func TestWindowFrame_RowsCurrentRowUnboundedFollowing(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30)")
	got := mustValues(t, db, "SELECT id, sum(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS rs FROM t ORDER BY id")
	want := [][]any{
		{int64(1), int64(60)},
		{int64(2), int64(50)},
		{int64(3), int64(30)},
	}
	wantRows(t, got, want)
}

func TestWindowFrame_PartitionScoped(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (k text NOT NULL, ord int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (k, ord, v) VALUES ('a', 1, 1), ('a', 2, 2), ('a', 3, 4), ('b', 1, 100)")
	got := mustValues(t, db, "SELECT k, ord, sum(v) OVER (PARTITION BY k ORDER BY ord ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM t ORDER BY k, ord")
	want := [][]any{
		{"a", int64(1), int64(1)},
		{"a", int64(2), int64(3)},
		{"a", int64(3), int64(6)},
		{"b", int64(1), int64(100)},
	}
	wantRows(t, got, want)
}
