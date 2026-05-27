// Aggregate and scalar-function tests covering AVG, CASE branches, SELECT DISTINCT, multi-column GROUP BY, abs and nullif.
package engine

import (
	"context"
	"testing"
)

func TestAggregateAvg_NoGroup(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30)")
	got := mustValues(t, db, "SELECT avg(v) FROM t")
	if len(got) != 1 || len(got[0]) != 1 {
		t.Fatalf("expected 1 row 1 col, got %v", got)
	}
	if f, ok := got[0][0].(float64); !ok || f != 20.0 {
		t.Fatalf("avg = %v (%T), want 20.0 (float64)", got[0][0], got[0][0])
	}
}

func TestAggregateAvg_GroupBy(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (k text NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (k, v) VALUES ('a', 10), ('a', 30), ('b', 5), ('b', 15), ('b', 25)")
	got := mustValues(t, db, "SELECT k, avg(v) FROM t GROUP BY k ORDER BY k")
	want := [][]any{
		{"a", 20.0},
		{"b", 15.0},
	}
	wantRows(t, got, want)
}

func TestAggregateAvg_EmptyIsNull(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (v int64 NOT NULL)")
	got := mustValues(t, db, "SELECT avg(v) FROM t")
	if len(got) != 1 || len(got[0]) != 1 {
		t.Fatalf("expected 1 row 1 col, got %v", got)
	}
	if got[0][0] != nil {
		t.Fatalf("avg of empty = %v, want nil", got[0][0])
	}
}

func TestAggregateAvg_CombinedWithCountAndSum(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (k int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (k, v) VALUES (1, 10), (1, 20), (1, 30)")
	got := mustValues(t, db, "SELECT count(*), sum(v), avg(v) FROM t")
	if len(got) != 1 || len(got[0]) != 3 {
		t.Fatalf("expected 1 row 3 cols, got %v", got)
	}
	if got[0][0] != int64(3) {
		t.Errorf("count = %v, want 3", got[0][0])
	}
	if got[0][1] != int64(60) {
		t.Errorf("sum = %v, want 60", got[0][1])
	}
	if f, ok := got[0][2].(float64); !ok || f != 20.0 {
		t.Errorf("avg = %v (%T), want 20.0 (float64)", got[0][2], got[0][2])
	}
}

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
	_, err := db.Query(t.Context(), "SELECT CASE WHEN x = 1 THEN 1 ELSE 'b' END FROM t")
	if err == nil {
		t.Fatal("type mismatch across CASE branches should error")
	}
}

func TestDistinct_DedupsRows(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (3)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")

	rows := mustValues(t, db, "SELECT DISTINCT id FROM t ORDER BY id")
	if len(rows) != 3 {
		t.Fatalf("DISTINCT got %d rows, want 3", len(rows))
	}
	got := []int64{rows[0][0].(int64), rows[1][0].(int64), rows[2][0].(int64)}
	want := []int64{1, 2, 3}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("row %d: got %d, want %d", i, got[i], want[i])
		}
	}
}

func TestDistinct_MultiColumn(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (a int64 NOT NULL, b int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (a, b) VALUES (1, 10)")
	mustExec(t, db, "INSERT INTO t (a, b) VALUES (1, 10)")
	mustExec(t, db, "INSERT INTO t (a, b) VALUES (1, 20)")
	mustExec(t, db, "INSERT INTO t (a, b) VALUES (2, 10)")
	mustExec(t, db, "INSERT INTO t (a, b) VALUES (2, 10)")
	rows := mustValues(t, db, "SELECT DISTINCT a, b FROM t")
	if len(rows) != 3 {
		t.Fatalf("multi-col DISTINCT got %d rows, want 3 (distinct pairs)", len(rows))
	}
}

func TestDistinct_StarRejected(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	if _, err := db.Query(context.Background(), "SELECT DISTINCT * FROM t"); err == nil {
		t.Fatal("SELECT DISTINCT * should error")
	}
}

func TestGroupBy_TwoIntColumns(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (a int64 NOT NULL, b int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (a, b) VALUES (1, 10)")
	mustExec(t, db, "INSERT INTO t (a, b) VALUES (1, 10)")
	mustExec(t, db, "INSERT INTO t (a, b) VALUES (1, 20)")
	mustExec(t, db, "INSERT INTO t (a, b) VALUES (2, 10)")

	rows := mustValues(t, db, "SELECT a, b, count(*) FROM t GROUP BY a, b")
	if len(rows) != 3 {
		t.Fatalf("got %d groups, want 3", len(rows))
	}
	counts := map[[2]int64]int64{}
	for _, r := range rows {
		key := [2]int64{r[0].(int64), r[1].(int64)}
		counts[key] = r[2].(int64)
	}
	want := map[[2]int64]int64{
		{1, 10}: 2,
		{1, 20}: 1,
		{2, 10}: 1,
	}
	for k, v := range want {
		if counts[k] != v {
			t.Errorf("group %v: count=%d, want %d", k, counts[k], v)
		}
	}
}

func TestGroupBy_IntPlusText(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, cat text NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, cat) VALUES (1, 'red')")
	mustExec(t, db, "INSERT INTO t (id, cat) VALUES (1, 'red')")
	mustExec(t, db, "INSERT INTO t (id, cat) VALUES (1, 'blue')")
	mustExec(t, db, "INSERT INTO t (id, cat) VALUES (2, 'red')")
	rows := mustValues(t, db, "SELECT id, cat, count(*) FROM t GROUP BY id, cat")
	if len(rows) != 3 {
		t.Fatalf("got %d groups, want 3", len(rows))
	}
	type key struct {
		id  int64
		cat string
	}
	counts := map[key]int64{}
	for _, r := range rows {
		counts[key{r[0].(int64), r[1].(string)}] = r[2].(int64)
	}
	if counts[key{1, "red"}] != 2 {
		t.Errorf("(1,red): got %d, want 2", counts[key{1, "red"}])
	}
	if counts[key{1, "blue"}] != 1 {
		t.Errorf("(1,blue): got %d, want 1", counts[key{1, "blue"}])
	}
	if counts[key{2, "red"}] != 1 {
		t.Errorf("(2,red): got %d, want 1", counts[key{2, "red"}])
	}
}

func TestGroupBy_SingleColumnStillWorks(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")
	rows := mustValues(t, db, "SELECT id, count(*) FROM t GROUP BY id")
	if len(rows) != 2 {
		t.Fatalf("single-col GROUP BY got %d, want 2", len(rows))
	}
}

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
