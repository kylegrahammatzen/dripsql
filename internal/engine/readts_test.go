// PR-V2c: runQuery pins a snapshot ts at statement start. openSegmentsAt with a
// historical ts returns the segment view as of that ts, including the DV state.
package engine

import (
	"testing"
)

func TestOpenSegmentsAt_HistoricalSnapshot(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	tsAfterFirst := db.nextCommitTs.Load()
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (3)")

	atFirst, err := db.openSegmentsAt("t", tsAfterFirst)
	if err != nil {
		t.Fatalf("openSegmentsAt(tsAfterFirst): %v", err)
	}
	if len(atFirst) != 1 {
		t.Fatalf("openSegmentsAt(tsAfterFirst) = %d segs, want 1", len(atFirst))
	}
	latest, err := db.openSegmentsForQuery("t")
	if err != nil {
		t.Fatalf("openSegmentsForQuery: %v", err)
	}
	if len(latest) != 3 {
		t.Fatalf("openSegmentsForQuery = %d segs, want 3", len(latest))
	}
}

func TestOpenSegmentsAt_DeletePreservedAtEarlierTs(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")
	tsBeforeDelete := db.nextCommitTs.Load()
	mustExec(t, db, "DELETE FROM t WHERE id = 1")

	preDelete, err := db.openSegmentsAt("t", tsBeforeDelete)
	if err != nil {
		t.Fatalf("openSegmentsAt(tsBeforeDelete): %v", err)
	}
	var preDV int
	for _, s := range preDelete {
		if s.DV != nil {
			preDV++
		}
	}
	if preDV != 0 {
		t.Fatalf("snapshot before delete had %d segs with DV, want 0", preDV)
	}

	postDelete, err := db.openSegmentsForQuery("t")
	if err != nil {
		t.Fatalf("openSegmentsForQuery: %v", err)
	}
	var postDV int
	for _, s := range postDelete {
		if s.DV != nil {
			postDV++
		}
	}
	if postDV == 0 {
		t.Fatalf("latest snapshot after delete had 0 segs with DV, want >=1")
	}
}
