// Dict-text LT/GT predicates push into Scan and run on encoded dictionary codes
// when the column is not in the projection. Covers strict and inclusive forms.
package engine

import (
	"fmt"
	"strings"
	"testing"
)

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
