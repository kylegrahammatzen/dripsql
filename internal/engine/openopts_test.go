// DB.SetAutoRetention + SetRetentionLag make DB.Vacuum auto-run VacuumRetention with the configured lag.
// Operators do not have to recompute the cutoff each cycle.
package engine

import (
	"testing"
)

func TestAutoRetention_RunsInsideVacuum(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	db.SetAutoRetention(true)
	db.SetRetentionLag(1)
	t.Cleanup(func() { db.Close() })
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")
	mustExec(t, db, "DELETE FROM t WHERE id = 1")
	// Pile up more commits so (nextCommitTs - RetentionLag) > DVCommitTs.
	mustExec(t, db, "INSERT INTO t (id) VALUES (3)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (4)")

	// Vacuum() with AutoRetention should retire the fully-dead seg whose
	// max(seg.CommitTs, DVCommitTs) is below (nextCommitTs - RetentionLag).
	n, err := db.Vacuum()
	if err != nil {
		t.Fatalf("Vacuum: %v", err)
	}
	if n < 1 {
		t.Fatalf("AutoRetention should have retired >=1 segment, got %d", n)
	}
	rows := mustValues(t, db, "SELECT id FROM t")
	if len(rows) != 3 {
		t.Fatalf("after auto-retention got %d live rows, want 3", len(rows))
	}
}

func TestAutoRetention_DefaultDoesNotRetire(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "DELETE FROM t WHERE id = 1")
	n, err := db.Vacuum()
	if err != nil {
		t.Fatalf("Vacuum: %v", err)
	}
	// Without AutoRetention the manifest still references the segment regardless of
	// how many stale DV files the legacy Vacuum sweep cleaned up.
	view := snapshotOf(t, db, "t")
	if len(view.Entries) != 1 {
		t.Fatalf("default Vacuum should not retire (entries=%d, n=%d)", len(view.Entries), n)
	}
}
