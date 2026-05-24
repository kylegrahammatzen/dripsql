// CASE WHEN expression. Searched form only (CASE WHEN <pred> THEN ... END). All THEN
// branches plus optional ELSE must share a Kind. NULL when no branch matches and no ELSE.
package engine

import (
	"testing"
)

func TestCase_BasicWhenThenElse(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (x int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (x) VALUES (-5)")
	mustExec(t, db, "INSERT INTO t (x) VALUES (0)")
	mustExec(t, db, "INSERT INTO t (x) VALUES (7)")

	rows := mustValues(t, db, "SELECT CASE WHEN x > 0 THEN 'pos' WHEN x < 0 THEN 'neg' ELSE 'zero' END AS s FROM t ORDER BY x")
	if len(rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(rows))
	}
	got := []string{rows[0][0].(string), rows[1][0].(string), rows[2][0].(string)}
	want := []string{"neg", "zero", "pos"}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("row %d: got %q, want %q", i, got[i], w)
		}
	}
}

func TestCase_NoElseReturnsNull(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (x int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (x) VALUES (5)")
	rows := mustValues(t, db, "SELECT CASE WHEN x > 100 THEN 'big' END AS s FROM t")
	if len(rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows))
	}
	if rows[0][0] != nil {
		t.Fatalf("no-match no-ELSE should be NULL, got %v", rows[0][0])
	}
}

func TestCase_IntBranches(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (x int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (x) VALUES (10)")
	mustExec(t, db, "INSERT INTO t (x) VALUES (20)")
	rows := mustValues(t, db, "SELECT CASE WHEN x = 10 THEN 1 ELSE 2 END AS bucket FROM t ORDER BY x")
	if rows[0][0].(int64) != 1 || rows[1][0].(int64) != 2 {
		t.Fatalf("int CASE results wrong: %v", rows)
	}
}

func TestCase_TypeMismatchRejected(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (x int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (x) VALUES (1)")
	// THEN int, ELSE text -> binder should reject.
	_, err := db.Query(t.Context(), "SELECT CASE WHEN x = 1 THEN 1 ELSE 'b' END FROM t")
	if err == nil {
		t.Fatal("type mismatch across CASE branches should error")
	}
}
