// End-to-end correlated subquery tests across EXISTS, scalar, and IN forms.
// Verifies outer column references inside the inner query bind per outer row.
package engine

import "testing"

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
