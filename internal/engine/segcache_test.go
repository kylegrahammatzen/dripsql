// Segment cache LRU bound: the cache never exceeds segCacheLimit after a query, even when
// the table has more segments than the cap, and reused segments keep working across the cap.
package engine

import (
	"context"
	"fmt"
	"testing"
)

func TestSegCache_EvictsColdAfterQueryOnSmallTable(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE big (id int64 NOT NULL)")
	mustExec(t, db, "CREATE TABLE small (id int64 NOT NULL)")
	for i := range 300 {
		mustExec(t, db, fmt.Sprintf("INSERT INTO big (id) VALUES (%d)", i))
	}
	mustExec(t, db, "INSERT INTO small (id) VALUES (1)")

	// First query loads all 300 big segments into the cache. count(*) resolves
	// from metadata so it does not open segments, so query big in a way that
	// forces opens by referencing a column.
	if _, err := db.Query(ctx, "SELECT id FROM big LIMIT 1"); err != nil {
		t.Fatalf("query big: %v", err)
	}
	// Second query touches only 'small'. All 300 big segments are now cold
	// and must be evicted until the cache fits the cap.
	if _, err := db.Query(ctx, "SELECT id FROM small"); err != nil {
		t.Fatalf("query small: %v", err)
	}
	if db.segLRU.Len() > segCacheLimit {
		t.Fatalf("segLRU.Len() = %d, want <= %d after cold table evicted", db.segLRU.Len(), segCacheLimit)
	}
	if len(db.segCache) != db.segLRU.Len() {
		t.Fatalf("cache map size %d != lru list size %d", len(db.segCache), db.segLRU.Len())
	}
}

func TestSegCache_RepeatedQueriesStable(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	for i := range 50 {
		mustExec(t, db, fmt.Sprintf("INSERT INTO t (id) VALUES (%d)", i))
	}
	for range 5 {
		if _, err := db.Query(ctx, "SELECT count(*) FROM t"); err != nil {
			t.Fatalf("query: %v", err)
		}
	}
	if db.segLRU.Len() != 50 {
		t.Fatalf("expected all 50 segments cached, got %d", db.segLRU.Len())
	}
}
