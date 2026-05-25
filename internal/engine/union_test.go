// Engine tests for UNION and UNION ALL across single-column and multi-column shapes.
// Each test seeds two tables, then asserts the lowered Rel produces the expected rows.
package engine

import (
	"context"
	"testing"
)

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
