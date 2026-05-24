// abs() preserves numeric kind; nullif(a, b) returns NULL when a == b else a.
package engine

import "testing"

func TestAbs_Int64(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, -10), (2, 5), (3, -3)")
	got := mustValues(t, db, "SELECT abs(v) AS a FROM t ORDER BY id")
	want := [][]any{{int64(10)}, {int64(5)}, {int64(3)}}
	wantRows(t, got, want)
}

func TestAbs_RejectsNonNumeric(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (s text NOT NULL)")
	_, err := db.Query(t.Context(), "SELECT abs(s) FROM t")
	if err == nil {
		t.Fatal("abs(text) must error")
	}
}

func TestNullIf_ReturnsNullOnEqual(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 0), (2, 5), (3, 0), (4, 7)")
	got := mustValues(t, db, "SELECT nullif(v, 0) AS x FROM t ORDER BY id")
	want := [][]any{{nil}, {int64(5)}, {nil}, {int64(7)}}
	wantRows(t, got, want)
}

func TestNullIf_TypeMismatchRejected(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (s text NOT NULL, n int64 NOT NULL)")
	_, err := db.Query(t.Context(), "SELECT nullif(s, n) FROM t")
	if err == nil {
		t.Fatal("nullif with mismatched types must error")
	}
}
