// Retention must compare against max(seg.CommitTs, DV-out CommitTs). A segment
// inserted long ago but DV'd-out recently stays alive until the DV-out ts is
// also below cutoff -- otherwise mid-snapshot readers would lose live rows.
package engine

import "testing"

func TestVacuumRetention_HonorsDVOutCommitTs(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	tsAfterInsert := db.nextCommitTs.Load()
	// Pile up a bunch of unrelated commits, then DV-out the original segment.
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (3)")
	mustExec(t, db, "DELETE FROM t WHERE id = 1")
	tsAfterDelete := db.nextCommitTs.Load()

	// Cutoff between insert and DV-out: must NOT retire (a reader pinned anywhere
	// in [tsAfterInsert, tsAfterDelete) would still see id=1 as alive).
	cutoff := tsAfterInsert + 1
	n, err := db.VacuumRetention(cutoff)
	if err != nil {
		t.Fatalf("VacuumRetention mid-range: %v", err)
	}
	if n != 0 {
		t.Fatalf("cutoff between insert and DV-out: expected 0, got %d", n)
	}

	// Cutoff above the DV-out ts: now safe to retire.
	n, err = db.VacuumRetention(tsAfterDelete + 1)
	if err != nil {
		t.Fatalf("VacuumRetention above DV-out: %v", err)
	}
	if n != 1 {
		t.Fatalf("cutoff above DV-out: expected 1, got %d", n)
	}
}
