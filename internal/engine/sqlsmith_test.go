// Deterministic SQLsmith-style fuzz: emit valid grammar, run end-to-end, fail only on panics or unexpected errors.
// Default 300 iterations per run. Override with DRIPSQL_SMITH_ITERS.
package engine

import (
	"context"
	"math/rand/v2"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
	"testing"
)

func TestSQLSmith_RandomQueriesSurviveExec(t *testing.T) {
	iters := 300
	if v := os.Getenv("DRIPSQL_SMITH_ITERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			iters = n
		}
	}
	db := openTestDB(t)
	s := defaultSmithSchema()
	mustExec(t, db, smithDDL(s))
	seedR := rand.New(rand.NewPCG(101, 202))
	mustExec(t, db, smithSeed(s, 200, seedR))

	r := rand.New(rand.NewPCG(7, 11))
	ctx := context.Background()
	bad := 0
	for i := 0; i < iters; i++ {
		q := smithSelect(s, r, 0)
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					t.Fatalf("panic on iter %d query %q: %v\n%s", i, q, rec, debug.Stack())
				}
			}()
			_, err := db.Query(ctx, q)
			if err != nil {
				// Unexpected grammar drift: report a few then bail to keep output bounded.
				if bad++; bad < 5 {
					t.Errorf("unexpected error iter %d %q: %v", i, q, err)
				} else if bad == 5 {
					t.Fatalf("too many smith errors; last: %q: %v", q, err)
				}
			}
		}()
	}
}

func TestSQLSmith_ShrinkOnFailure(t *testing.T) {
	// Sanity check the shrinker by feeding a query we know fails.
	bad := "SELECT bogus_col FROM fuzz_t"
	got := smithShrink(bad, func(q string) bool { return strings.Contains(q, "bogus_col") })
	if got != bad && !strings.Contains(got, "bogus_col") {
		t.Fatalf("shrink should preserve the failure marker, got %q", got)
	}
}

// smithShrink trims trailing clauses while the predicate still reports failure.
// The check func returns true when q still triggers the bug.
func smithShrink(q string, fails func(string) bool) string {
	cur := q
	clauses := []string{" LIMIT ", " ORDER BY ", " GROUP BY ", " WHERE "}
	for _, c := range clauses {
		i := strings.Index(cur, c)
		if i < 0 {
			continue
		}
		trial := cur[:i]
		if fails(trial) {
			cur = trial
		}
	}
	return cur
}
