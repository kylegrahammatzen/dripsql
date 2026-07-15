// Grouped sum queries must answer from the group sums sidecar when eligible and
// fall back to the scan path with identical results when not.
package engine

import (
	"context"
	"fmt"
	"testing"
)

func seedGroupSums(t *testing.T, db *DB) [][]any {
	t.Helper()
	mustExec(t, db, "CREATE TABLE t (cat text NOT NULL, price int64 NOT NULL, opt int64)")
	var stmts []string
	for i := range 400 {
		cat := []string{"a", "b", "c"}[i%3]
		opt := fmt.Sprintf("%d", i)
		if i%7 == 0 {
			opt = "NULL"
		}
		stmts = append(stmts, fmt.Sprintf("INSERT INTO t (cat, price, opt) VALUES ('%s', %d, %s)", cat, i*3, opt))
	}
	if _, err := db.BulkInsert(context.Background(), stmts); err != nil {
		t.Fatalf("BulkInsert: %v", err)
	}
	var wantA, wantB, wantC int64
	for i := range 400 {
		v := int64(i * 3)
		switch i % 3 {
		case 0:
			wantA += v
		case 1:
			wantB += v
		case 2:
			wantC += v
		}
	}
	return [][]any{
		{"a", wantA, int64(134)},
		{"b", wantB, int64(133)},
		{"c", wantC, int64(133)},
	}
}

func TestEngine_GroupSumMetadataAnswer(t *testing.T) {
	db := openTestDB(t)
	want := seedGroupSums(t, db)
	const q = "SELECT cat, sum(price) AS s, count(*) AS c FROM t GROUP BY cat ORDER BY cat"
	wantRows(t, mustValues(t, db, q), want)

	if err := db.lockOpen(); err != nil {
		t.Fatalf("lockOpen: %v", err)
	}
	plan, err := db.planForQuery("SELECT cat, sum(price) AS s, count(*) AS c FROM t GROUP BY cat")
	if err != nil {
		db.mu.Unlock()
		t.Fatalf("plan: %v", err)
	}
	rows, ok, err := db.answerFromMetadata(plan)
	db.mu.Unlock()
	if err != nil {
		t.Fatalf("answerFromMetadata: %v", err)
	}
	if !ok || rows == nil || len(rows.Values) != 3 {
		t.Fatalf("grouped sum should answer from metadata, ok=%v rows=%v", ok, rows)
	}
}

func TestEngine_GroupSumNullableFallsBack(t *testing.T) {
	db := openTestDB(t)
	seedGroupSums(t, db)
	var wantA, wantB, wantC int64
	for i := range 400 {
		if i%7 == 0 {
			continue
		}
		v := int64(i)
		switch i % 3 {
		case 0:
			wantA += v
		case 1:
			wantB += v
		case 2:
			wantC += v
		}
	}
	wantRows(t, mustValues(t, db, "SELECT cat, sum(opt) AS s FROM t GROUP BY cat ORDER BY cat"), [][]any{
		{"a", wantA},
		{"b", wantB},
		{"c", wantC},
	})
}

func TestEngine_GroupSumDeleteFallsBack(t *testing.T) {
	db := openTestDB(t)
	seedGroupSums(t, db)
	mustExec(t, db, "DELETE FROM t WHERE price = 0")
	got := mustValues(t, db, "SELECT cat, sum(price) AS s, count(*) AS c FROM t GROUP BY cat ORDER BY cat")
	if len(got) != 3 {
		t.Fatalf("got %d groups", len(got))
	}
	if got[0][2].(int64) != 133 {
		t.Fatalf("group a count after delete = %v, want 133", got[0][2])
	}
}
