// Delta-bitpack equality on encoded payload. Column must be predicate-only.
// Monotonic-with-noise sequence makes cascade pick delta over plain.
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
		// Non-uniform deltas keep delta+bitpack the cascade winner over sequence.
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
