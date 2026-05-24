// RANGE-based window frames. Value-distance arithmetic on the single ORDER BY key.
package engine

import "testing"

func TestWindowRange_PrecedingFollowing(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 100), (3, 200), (5, 300), (10, 400)")
	got := mustValues(t, db, "SELECT id, sum(v) OVER (ORDER BY id RANGE BETWEEN 2 PRECEDING AND 2 FOLLOWING) AS s FROM t ORDER BY id")
	// id=1: range [-1, 3] catches id=1, id=3 => 100+200 = 300
	// id=3: range [1, 5] catches id=1, id=3, id=5 => 600
	// id=5: range [3, 7] catches id=3, id=5 => 500
	// id=10: range [8, 12] catches id=10 => 400
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
	// Equal order keys group together: id=2 and id=2 share the frame at CURRENT ROW.
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (2, 30), (3, 40)")
	got := mustValues(t, db, "SELECT id, v, sum(v) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rs FROM t ORDER BY id, v")
	// id=1: just 10
	// id=2 peers (both rows): 10 + 20 + 30 = 60 each
	// id=3: 10 + 20 + 30 + 40 = 100
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
	// DESC: pivot id=10, range catches values in [8, 12] => only id=10 => v=1
	// pivot id=5, range catches [3, 7] => id=5, id=3 => 2+4 = 6
	// pivot id=3, range catches [1, 5] => id=5, id=3 => 2+4 = 6
	want := [][]any{
		{int64(10), int64(1)},
		{int64(5), int64(6)},
		{int64(3), int64(6)},
	}
	wantRows(t, got, want)
}
