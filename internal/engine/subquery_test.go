// Scalar subquery end-to-end. Uncorrelated only -- inner runs once at BuildOperator.
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
