// CTE end-to-end: WITH binds a named subquery, outer SELECT reads from it.
// First slice supports unqualified column refs only.
package engine

import "testing"

func TestCTE_Simple(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30)")
	got := mustValues(t, db, "WITH big AS (SELECT id, v FROM t WHERE v >= 20) SELECT id, v FROM big ORDER BY id")
	want := [][]any{
		{int64(2), int64(20)},
		{int64(3), int64(30)},
	}
	wantRows(t, got, want)
}

func TestCTE_AggregateOverCTE(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (k text NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (k, v) VALUES ('a', 1), ('a', 2), ('b', 3), ('b', 4), ('c', 5)")
	got := mustValues(t, db, "WITH high AS (SELECT k, v FROM t WHERE v >= 3) SELECT k, sum(v) FROM high GROUP BY k ORDER BY k")
	want := [][]any{
		{"b", int64(7)},
		{"c", int64(5)},
	}
	wantRows(t, got, want)
}

func TestCTE_MultipleNames(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20)")
	got := mustValues(t, db, "WITH a AS (SELECT id, v FROM t WHERE id = 1), b AS (SELECT id, v FROM t WHERE id = 2) SELECT id, v FROM a")
	want := [][]any{{int64(1), int64(10)}}
	wantRows(t, got, want)
}
