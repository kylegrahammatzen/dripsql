// IS NULL and IS NOT NULL flow from grammar through pushdown and IN misses against a NULL list stay unknown.
// The +0 arithmetic wrap defeats pushdown so the exec row-eval path is exercised too.
package engine

import "testing"

func TestEngine_NullPredicatesIsNull(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 5), (2, NULL), (3, 7), (4, NULL)")
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE v IS NULL ORDER BY id"), [][]any{{int64(2)}, {int64(4)}})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE v IS NOT NULL ORDER BY id"), [][]any{{int64(1)}, {int64(3)}})

	// A NOT NULL column has no validity bitmap so IS NULL must select nothing and IS NOT NULL everything.
	mustExec(t, db, "CREATE TABLE c (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO c (id, v) VALUES (1, 5), (2, 7)")
	wantRows(t, mustValues(t, db, "SELECT id FROM c WHERE v IS NULL ORDER BY id"), [][]any{})
	wantRows(t, mustValues(t, db, "SELECT id FROM c WHERE v IS NOT NULL ORDER BY id"), [][]any{{int64(1)}, {int64(2)}})
}

func TestEngine_NullPredicatesIsNullUnderNot(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 5), (2, NULL), (3, 7)")
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE NOT (v IS NULL) ORDER BY id"), [][]any{{int64(1)}, {int64(3)}})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE NOT (v IS NOT NULL) ORDER BY id"), [][]any{{int64(2)}})
}

func TestEngine_NullPredicatesInWithNullList(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 1), (2, 2), (3, NULL), (4, 5)")
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE v IN (1, NULL) ORDER BY id"), [][]any{{int64(1)}})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE v + 0 IN (1, NULL) ORDER BY id"), [][]any{{int64(1)}})
}

func TestEngine_NullPredicatesNotInWithNullList(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 1), (2, 2), (3, NULL), (4, 5)")
	// Every miss against a list containing NULL is unknown so NOT IN returns nothing.
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE v NOT IN (1, NULL) ORDER BY id"), [][]any{})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE v + 0 NOT IN (1, NULL) ORDER BY id"), [][]any{})
}

func TestEngine_NullPredicatesNotInWithoutNull(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 1), (2, 2), (3, NULL), (4, 5)")
	// A NULL probe stays unknown so id 3 is excluded while definite misses pass.
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE v NOT IN (1, 2) ORDER BY id"), [][]any{{int64(4)}})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE v + 0 NOT IN (1, 2) ORDER BY id"), [][]any{{int64(4)}})
}
