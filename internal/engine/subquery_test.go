// Compound and derived query shapes covering scalar subqueries, IN, EXISTS, correlated subqueries, WITH CTE binding, and UNION set operators.
package engine

import (
	"context"
	"testing"
)

func TestSubquery_InWhere(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
	got := mustValues(t, db, "SELECT id, v FROM t WHERE v > (SELECT avg(v) FROM t) ORDER BY id")
	want := [][]any{
		{int64(3), int64(30)},
		{int64(4), int64(40)},
	}
	wantRows(t, got, want)
}

func TestSubquery_InProjection(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30)")
	got := mustValues(t, db, "SELECT id, (SELECT max(v) FROM t) AS m FROM t WHERE id = 1")
	want := [][]any{{int64(1), int64(30)}}
	wantRows(t, got, want)
}

func TestSubquery_EmptyReturnsNull(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	got := mustValues(t, db, "SELECT (SELECT max(id) FROM t WHERE id > 999) AS m FROM t")
	want := [][]any{{nil}}
	wantRows(t, got, want)
}

func TestSubquery_MultiRowRejected(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1), (2), (3)")
	_, err := db.Query(context.Background(), "SELECT (SELECT id FROM t) AS m FROM t LIMIT 1")
	if err == nil {
		t.Fatal("expected error for multi-row scalar subquery")
	}
}

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

func TestCorrelated_ExistsAcrossRows(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE outer_t (id int64 NOT NULL, tag text NOT NULL)")
	mustExec(t, db, "CREATE TABLE inner_t (oid int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO outer_t (id, tag) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	mustExec(t, db, "INSERT INTO inner_t (oid, v) VALUES (1, 10), (3, 30)")

	rows := mustValues(t, db, "SELECT id FROM outer_t WHERE EXISTS (SELECT oid FROM inner_t WHERE oid = id) ORDER BY id ASC")
	if len(rows) != 2 || rows[0][0] != int64(1) || rows[1][0] != int64(3) {
		t.Fatalf("EXISTS correlated: got %v want [[1] [3]]", rows)
	}
}

func TestCorrelated_ScalarMatchesPerRow(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE o (id int64 NOT NULL)")
	mustExec(t, db, "CREATE TABLE i (oid int64 NOT NULL, val int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO o (id) VALUES (1), (2), (3)")
	mustExec(t, db, "INSERT INTO i (oid, val) VALUES (1, 100), (2, 200), (3, 300)")

	rows := mustValues(t, db, "SELECT id, (SELECT val FROM i WHERE oid = id) AS v FROM o ORDER BY id ASC")
	if len(rows) != 3 {
		t.Fatalf("scalar correlated: got %d rows want 3", len(rows))
	}
	want := []int64{100, 200, 300}
	for i, w := range want {
		if rows[i][1] != w {
			t.Fatalf("row %d v: got %v want %d", i, rows[i][1], w)
		}
	}
}

func TestCorrelated_QualifiedOuterRefs(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE outer_t (id int64 NOT NULL)")
	mustExec(t, db, "CREATE TABLE inner_t (oid int64 NOT NULL, val int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO outer_t (id) VALUES (1), (2), (3)")
	mustExec(t, db, "INSERT INTO inner_t (oid, val) VALUES (1, 10), (2, 20), (3, 30)")

	rows := mustValues(t, db, "SELECT outer_t.id, (SELECT inner_t.val FROM inner_t WHERE inner_t.oid = outer_t.id) AS v FROM outer_t ORDER BY outer_t.id ASC")
	if len(rows) != 3 {
		t.Fatalf("qualified correlated: got %d rows want 3", len(rows))
	}
	want := []int64{10, 20, 30}
	for i, w := range want {
		if rows[i][1] != w {
			t.Fatalf("row %d v: got %v want %d", i, rows[i][1], w)
		}
	}
}

func TestQualifiedRef_SingleTableSelect(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE foo (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO foo (id) VALUES (1), (2), (3)")
	rows := mustValues(t, db, "SELECT foo.id FROM foo WHERE foo.id = 2")
	if len(rows) != 1 || rows[0][0] != int64(2) {
		t.Fatalf("qualified single-table: got %v want [[2]]", rows)
	}
}

func TestCorrelated_NotExistsExcludesMatches(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE o (id int64 NOT NULL)")
	mustExec(t, db, "CREATE TABLE i (oid int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO o (id) VALUES (1), (2), (3)")
	mustExec(t, db, "INSERT INTO i (oid) VALUES (2)")

	rows := mustValues(t, db, "SELECT id FROM o WHERE NOT EXISTS (SELECT oid FROM i WHERE oid = id) ORDER BY id ASC")
	if len(rows) != 2 || rows[0][0] != int64(1) || rows[1][0] != int64(3) {
		t.Fatalf("NOT EXISTS correlated: got %v want [[1] [3]]", rows)
	}
}

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

func TestEngine_UnionAll_IntegerSingleColumn(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE a (id int64 NOT NULL)")
	mustExec(t, db, "CREATE TABLE b (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO a (id) VALUES (1), (2), (3)")
	mustExec(t, db, "INSERT INTO b (id) VALUES (3), (4)")
	got := mustValues(t, db, "SELECT id FROM a UNION ALL SELECT id FROM b ORDER BY id")
	wantRows(t, got, [][]any{
		{int64(1)},
		{int64(2)},
		{int64(3)},
		{int64(3)},
		{int64(4)},
	})
}

func TestEngine_UnionDistinct_DeduplicatesIntegers(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE a (id int64 NOT NULL)")
	mustExec(t, db, "CREATE TABLE b (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO a (id) VALUES (1), (2), (3)")
	mustExec(t, db, "INSERT INTO b (id) VALUES (3), (4)")
	got := mustValues(t, db, "SELECT id FROM a UNION SELECT id FROM b ORDER BY id")
	wantRows(t, got, [][]any{
		{int64(1)},
		{int64(2)},
		{int64(3)},
		{int64(4)},
	})
}

func TestEngine_Union_MultiColumn(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE a (id int64 NOT NULL, name text NOT NULL)")
	mustExec(t, db, "CREATE TABLE b (id int64 NOT NULL, name text NOT NULL)")
	mustExec(t, db, "INSERT INTO a (id, name) VALUES (1, 'x'), (2, 'y')")
	mustExec(t, db, "INSERT INTO b (id, name) VALUES (2, 'y'), (3, 'z')")
	got := mustValues(t, db, "SELECT id, name FROM a UNION SELECT id, name FROM b ORDER BY id")
	wantRows(t, got, [][]any{
		{int64(1), "x"},
		{int64(2), "y"},
		{int64(3), "z"},
	})
}

func TestEngine_UnionAll_Mismatched_ColumnCount(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE a (id int64 NOT NULL, name text NOT NULL)")
	mustExec(t, db, "CREATE TABLE b (id int64 NOT NULL)")
	if _, err := db.Query(context.Background(), "SELECT id, name FROM a UNION SELECT id FROM b"); err == nil {
		t.Fatalf("expected error for mismatched UNION column count")
	}
}
