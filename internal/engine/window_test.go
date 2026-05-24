// Window function end-to-end: ROW_NUMBER, RANK, DENSE_RANK with PARTITION/ORDER BY.
package engine

import "testing"

func TestWindow_RowNumberOrderBy(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 30), (2, 10), (3, 20)")
	got := mustValues(t, db, "SELECT id, row_number() OVER (ORDER BY v) AS rn FROM t ORDER BY id")
	want := [][]any{
		{int64(1), int64(3)},
		{int64(2), int64(1)},
		{int64(3), int64(2)},
	}
	wantRows(t, got, want)
}

func TestWindow_RowNumberPartitionByOrderBy(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (k text NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (k, v) VALUES ('a', 10), ('a', 20), ('a', 5), ('b', 100), ('b', 50)")
	got := mustValues(t, db, "SELECT k, v, row_number() OVER (PARTITION BY k ORDER BY v) AS rn FROM t ORDER BY k, v")
	want := [][]any{
		{"a", int64(5), int64(1)},
		{"a", int64(10), int64(2)},
		{"a", int64(20), int64(3)},
		{"b", int64(50), int64(1)},
		{"b", int64(100), int64(2)},
	}
	wantRows(t, got, want)
}

func TestWindow_RankWithTies(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 20), (4, 30)")
	got := mustValues(t, db, "SELECT id, rank() OVER (ORDER BY v) AS r FROM t ORDER BY id")
	want := [][]any{
		{int64(1), int64(1)},
		{int64(2), int64(2)},
		{int64(3), int64(2)},
		{int64(4), int64(4)},
	}
	wantRows(t, got, want)
}

func TestWindow_DenseRankWithTies(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 20), (4, 30)")
	got := mustValues(t, db, "SELECT id, dense_rank() OVER (ORDER BY v) AS r FROM t ORDER BY id")
	want := [][]any{
		{int64(1), int64(1)},
		{int64(2), int64(2)},
		{int64(3), int64(2)},
		{int64(4), int64(3)},
	}
	wantRows(t, got, want)
}
