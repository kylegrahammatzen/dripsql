// Deterministic SQLsmith-style fuzz: emit valid grammar, run end-to-end, fail only on panics or unexpected errors.
// Default 300 iterations per run. Override with DRIPSQL_SMITH_ITERS.
package engine

import (
	"context"
	"math/rand/v2"
	"os"
	"runtime/debug"
	"strconv"
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
	for i := range iters {
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
