// FOR-bitpack range predicates (LT/GT/BETWEEN) run on encoded bytes when the
// predicate column is not in the projection. Correctness check across boundary cases.
package engine

import (
	"fmt"
	"strings"
	"testing"
)

func TestFORRange_LessThan(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, payload text NOT NULL)")
	var b strings.Builder
	b.WriteString("INSERT INTO t (id, payload) VALUES ")
	for i := range 256 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "(%d, 'p%d')", 1000+i, i)
	}
	mustExec(t, db, b.String())

	got := mustValues(t, db, "SELECT count(*) FROM t WHERE id < 1100")
	if len(got) != 1 || got[0][0].(int64) != 100 {
		t.Fatalf("count(id < 1100) = %v, want 100", got)
	}
}

func TestFORRange_GreaterThan(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, payload text NOT NULL)")
	var b strings.Builder
	b.WriteString("INSERT INTO t (id, payload) VALUES ")
	for i := range 256 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "(%d, 'p%d')", 1000+i, i)
	}
	mustExec(t, db, b.String())

	got := mustValues(t, db, "SELECT count(*) FROM t WHERE id > 1200")
	if len(got) != 1 || got[0][0].(int64) != 55 {
		t.Fatalf("count(id > 1200) = %v, want 55", got)
	}
}

func TestFORRange_BetweenLowersToAnd(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, payload text NOT NULL)")
	var b strings.Builder
	b.WriteString("INSERT INTO t (id, payload) VALUES ")
	for i := range 256 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "(%d, 'p%d')", 1000+i, i)
	}
	mustExec(t, db, b.String())

	got := mustValues(t, db, "SELECT count(*) FROM t WHERE id BETWEEN 1100 AND 1149")
	if len(got) != 1 || got[0][0].(int64) != 50 {
		t.Fatalf("count(id BETWEEN 1100 AND 1149) = %v, want 50", got)
	}
}

func TestFORRange_BelowBaseEmpty(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, payload text NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, payload) VALUES (1000, 'a'), (1001, 'b'), (1002, 'c')")
	got := mustValues(t, db, "SELECT count(*) FROM t WHERE id < 500")
	if got[0][0].(int64) != 0 {
		t.Fatalf("count(id < 500) = %v, want 0", got)
	}
}
