// NOT under WHERE follows SQL three-valued logic so rows where the child predicate is NULL stay excluded.
// The +0 arithmetic in some predicates defeats pushdown so the decoded exec filter path is exercised.
package engine

import "testing"

func TestEngine_NotThreeValuedRowEval(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 5), (2, 7), (3, NULL)")
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE NOT (v + 0 = 5) ORDER BY id"), [][]any{{int64(2)}})

	mustExec(t, db, "CREATE TABLE c (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO c (id, v) VALUES (1, 5), (2, 7)")
	wantRows(t, mustValues(t, db, "SELECT id FROM c WHERE NOT (v + 0 = 5) ORDER BY id"), [][]any{{int64(2)}})
}

func TestEngine_NotThreeValuedOverAnd(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, a int64 NOT NULL, b int64)")
	mustExec(t, db, "INSERT INTO t (id, a, b) VALUES (1, 1, 2), (2, 9, NULL), (3, 1, NULL), (4, 5, 7)")
	// NOT of false AND NULL is true so id 2 must survive while NOT of true AND NULL is unknown so id 3 stays out.
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE NOT (a = 1 AND b = 2) ORDER BY id"), [][]any{{int64(2)}, {int64(4)}})
}

func TestEngine_NotThreeValuedOverOr(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, a int64 NOT NULL, b int64)")
	mustExec(t, db, "INSERT INTO t (id, a, b) VALUES (1, 1, NULL), (2, 5, NULL), (3, 5, 7)")
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE NOT (a + 0 = 1 OR b = 2) ORDER BY id"), [][]any{{int64(3)}})
}

func TestEngine_NotThreeValuedDoubleNot(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 5), (2, 7), (3, NULL)")
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE NOT (NOT (v + 0 = 5)) ORDER BY id"), [][]any{{int64(1)}})
}

func TestEngine_NotThreeValuedPushdownAgrees(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 5), (2, 7), (3, NULL)")
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE NOT (v = 5) ORDER BY id"), [][]any{{int64(2)}})
	wantRows(t, mustValues(t, db, "SELECT id FROM t WHERE NOT (v + 0 = 5) ORDER BY id"), [][]any{{int64(2)}})
}
