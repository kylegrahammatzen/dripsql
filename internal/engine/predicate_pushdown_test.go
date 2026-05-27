// End-to-end checks that predicates run on encoded payloads when the predicate column is not in the projection.
// Covers FOR-bitpack int range, delta-bitpack equality, and dict-encoded text comparison fast paths.
package engine

import (
	"fmt"
	"strings"
	"testing"
)

func TestDeltaEncoded_EqMatch(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, tag text NOT NULL)")
	var b strings.Builder
	b.WriteString("INSERT INTO t (id, tag) VALUES ")
	for i := range 512 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "(%d, 'r%d')", 10000+i*3+(i%7), i)
	}
	mustExec(t, db, b.String())
	got := mustValues(t, db, "SELECT count(*) FROM t WHERE id = 10000")
	if got[0][0].(int64) != 1 {
		t.Fatalf("count(id = 10000) = %v, want 1", got)
	}
}

func TestDeltaEncoded_EqMiss(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, tag text NOT NULL)")
	var b strings.Builder
	b.WriteString("INSERT INTO t (id, tag) VALUES ")
	for i := range 256 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, "(%d, 'r%d')", 10000+i*3+(i%5), i)
	}
	mustExec(t, db, b.String())
	got := mustValues(t, db, "SELECT count(*) FROM t WHERE id = 99999")
	if got[0][0].(int64) != 0 {
		t.Fatalf("count(id = 99999) = %v, want 0", got)
	}
}

func TestDictText_LessThan(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, label text NOT NULL)")
	var b strings.Builder
	b.WriteString("INSERT INTO t (id, label) VALUES ")
	labels := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	want := int64(0)
	for i := range 250 {
		if i > 0 {
			b.WriteByte(',')
		}
		label := labels[i%len(labels)]
		fmt.Fprintf(&b, "(%d, '%s')", i, label)
		if label < "delta" {
			want++
		}
	}
	mustExec(t, db, b.String())

	got := mustValues(t, db, "SELECT count(*) FROM t WHERE label < 'delta'")
	if len(got) != 1 || got[0][0].(int64) != want {
		t.Fatalf("count(label < 'delta') = %v, want %d", got, want)
	}
}

func TestDictText_GreaterThanInclusive(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, label text NOT NULL)")
	var b strings.Builder
	b.WriteString("INSERT INTO t (id, label) VALUES ")
	for i := range 100 {
		if i > 0 {
			b.WriteByte(',')
		}
		label := "alpha"
		switch i % 3 {
		case 1:
			label = "beta"
		case 2:
			label = "gamma"
		}
		fmt.Fprintf(&b, "(%d, '%s')", i, label)
	}
	mustExec(t, db, b.String())

	got := mustValues(t, db, "SELECT count(*) FROM t WHERE label >= 'beta'")
	if len(got) != 1 {
		t.Fatalf("expected 1 row, got %d", len(got))
	}
	if got[0][0].(int64) <= 0 {
		t.Fatalf("count(label >= 'beta') = %v, want > 0", got)
	}
}

func TestDictText_LessEqualMatchesAll(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, label text NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, label) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	got := mustValues(t, db, "SELECT count(*) FROM t WHERE label <= 'c'")
	if len(got) != 1 || got[0][0].(int64) != 3 {
		t.Fatalf("count(label <= 'c') = %v, want 3", got)
	}
}

func TestDictText_GreaterStrictExcludesEqual(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, label text NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, label) VALUES (1, 'a'), (2, 'b'), (3, 'c')")
	got := mustValues(t, db, "SELECT count(*) FROM t WHERE label > 'b'")
	if len(got) != 1 || got[0][0].(int64) != 1 {
		t.Fatalf("count(label > 'b') = %v, want 1", got)
	}
}

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
