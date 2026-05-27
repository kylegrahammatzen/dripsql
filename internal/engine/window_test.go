// Window function tests covering ranking functions (ROW_NUMBER, RANK, DENSE_RANK), aggregate windows running and unframed, ROWS frames, and RANGE frames over int keys.
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

func TestWindowRange_PrecedingFollowing(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 100), (3, 200), (5, 300), (10, 400)")
	got := mustValues(t, db, "SELECT id, sum(v) OVER (ORDER BY id RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS s FROM t ORDER BY id")
	want := [][]any{
		{int64(1), int64(300)},
		{int64(3), int64(600)},
		{int64(5), int64(500)},
		{int64(10), int64(400)},
	}
	wantRows(t, got, want)
}

func TestWindowRange_UnboundedPrecedingCurrentRow(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (2, 30), (3, 40)")
	got := mustValues(t, db, "SELECT id, v, sum(v) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rs FROM t ORDER BY id, v")
	want := [][]any{
		{int64(1), int64(10), int64(10)},
		{int64(2), int64(20), int64(60)},
		{int64(2), int64(30), int64(60)},
		{int64(3), int64(40), int64(100)},
	}
	wantRows(t, got, want)
}

func TestWindowRange_DescOrder(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (10, 1), (5, 2), (3, 4)")
	got := mustValues(t, db, "SELECT id, sum(v) OVER (ORDER BY id DESC RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS s FROM t ORDER BY id DESC")
	want := [][]any{
		{int64(10), int64(1)},
		{int64(5), int64(6)},
		{int64(3), int64(6)},
	}
	wantRows(t, got, want)
}
