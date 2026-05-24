// IN (subquery) and EXISTS (subquery) end-to-end. Uncorrelated only.
package engine

import "testing"

func TestInSubquery_Match(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
	got := mustValues(t, db, "SELECT id FROM t WHERE id IN (SELECT id FROM t WHERE v >= 30) ORDER BY id")
	want := [][]any{
		{int64(3)},
		{int64(4)},
	}
	wantRows(t, got, want)
}

func TestInSubquery_NotIn(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30)")
	got := mustValues(t, db, "SELECT id FROM t WHERE id NOT IN (SELECT id FROM t WHERE v >= 30) ORDER BY id")
	want := [][]any{
		{int64(1)},
		{int64(2)},
	}
	wantRows(t, got, want)
}

func TestExistsSubquery_True(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1), (2), (3)")
	got := mustValues(t, db, "SELECT id FROM t WHERE EXISTS (SELECT id FROM t WHERE id > 999) ORDER BY id")
	if len(got) != 0 {
		t.Fatalf("expected 0 rows (EXISTS false), got %d: %v", len(got), got)
	}
}

func TestExistsSubquery_TrueWithMatches(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1), (2)")
	got := mustValues(t, db, "SELECT id FROM t WHERE EXISTS (SELECT id FROM t WHERE id = 1) ORDER BY id")
	want := [][]any{
		{int64(1)},
		{int64(2)},
	}
	wantRows(t, got, want)
}

func TestNotExists(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1), (2)")
	got := mustValues(t, db, "SELECT id FROM t WHERE NOT EXISTS (SELECT id FROM t WHERE id > 999) ORDER BY id")
	want := [][]any{
		{int64(1)},
		{int64(2)},
	}
	wantRows(t, got, want)
}
