// SQL `<table> AS OF <commit_ts>` time travel. Parser disambiguates AS OF from AS alias
// via a 2-token lookahead. Different tables in one join can specify different AS OF
// snapshots independently.
package engine

import (
	"context"
	"fmt"
	"testing"
)

func TestSQLAsOf_HistoricalSnapshot(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	tsAfterFirst := db.nextCommitTs.Load()
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")

	q := fmt.Sprintf("SELECT id FROM t AS OF %d", tsAfterFirst)
	rows, err := db.Query(context.Background(), q)
	if err != nil {
		t.Fatalf("Query AS OF: %v", err)
	}
	if len(rows.Values) != 1 {
		t.Fatalf("AS OF tsAfterFirst: got %d rows, want 1", len(rows.Values))
	}
}

func TestSQLAsOf_ParserDoesNotBreakBareAlias(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	if len(mustValues(t, db, "SELECT id FROM t alias")) != 1 {
		t.Fatal("bare alias regressed")
	}
}

func TestSQLAsOf_ParserDoesNotBreakASAlias(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	if len(mustValues(t, db, "SELECT id FROM t AS alias")) != 1 {
		t.Fatal("AS alias regressed")
	}
}

func TestSQLAsOf_AliasAndAsOf(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	tsAfterFirst := db.nextCommitTs.Load()
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")

	q := fmt.Sprintf("SELECT id FROM t alias AS OF %d", tsAfterFirst)
	rows, err := db.Query(context.Background(), q)
	if err != nil {
		t.Fatalf("alias + AS OF: %v", err)
	}
	if len(rows.Values) != 1 {
		t.Fatalf("alias + AS OF: got %d rows, want 1", len(rows.Values))
	}
}

// count(*) used to slip past the AS OF clause because the metadata aggregate
// short-circuit ignored scan.Table.AsOf. Regression for that path.
func TestSQLAsOf_CountStarRespectsSnapshot(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1), (2)")
	tsAfterFirst := db.nextCommitTs.Load()
	mustExec(t, db, "INSERT INTO t (id) VALUES (3), (4)")

	q := fmt.Sprintf("SELECT count(*) FROM t AS OF %d", tsAfterFirst)
	rows, err := db.Query(context.Background(), q)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(rows.Values) != 1 {
		t.Fatalf("count rows = %d, want 1", len(rows.Values))
	}
	if got := rows.Values[0][0]; got != int64(2) {
		t.Fatalf("count(*) AS OF earlyTs = %v, want 2", got)
	}
}

func TestSQLAsOf_PerTableInJoin(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE a (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "CREATE TABLE b (id int64 NOT NULL, w int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO a (id, v) VALUES (1, 100)")
	mustExec(t, db, "INSERT INTO b (id, w) VALUES (1, 999)")
	tsAfterFirstB := db.nextCommitTs.Load()
	mustExec(t, db, "INSERT INTO b (id, w) VALUES (1, 222)")

	q := fmt.Sprintf("SELECT a.v, b.w FROM a JOIN b AS OF %d ON a.id = b.id", tsAfterFirstB)
	rows, err := db.Query(context.Background(), q)
	if err != nil {
		t.Fatalf("join with per-table AS OF: %v", err)
	}
	if len(rows.Values) != 1 {
		t.Fatalf("got %d rows, want 1", len(rows.Values))
	}
	got := rows.Values[0]
	if got[1] != int64(999) {
		t.Fatalf("b.w = %v, want 999 (snapshot before second insert)", got[1])
	}
}
