// Retention vacuum drops segments whose CommitTs is below the cutoff AND whose DV
// has marked every row deleted. Active Tx readers pin their read_ts so retention
// can't retire a snapshot a reader still needs.
package engine

import (
	"os"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

func TestVacuumRetention_DropsFullyDeletedOldSegment(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")
	view := snapshotOf(t, db, "t")
	pathFirst := view.Entries[0].Path
	mustExec(t, db, "DELETE FROM t WHERE id = 1")
	cutoff := db.nextCommitTs.Load() + 1

	n, err := db.VacuumRetention(cutoff)
	if err != nil {
		t.Fatalf("VacuumRetention: %v", err)
	}
	if n != 1 {
		t.Fatalf("VacuumRetention retired %d, want 1", n)
	}
	if _, err := os.Stat(pathFirst); !os.IsNotExist(err) {
		t.Fatalf("segment file %q should be gone (err=%v)", pathFirst, err)
	}
	rows := mustValues(t, db, "SELECT id FROM t")
	if len(rows) != 1 {
		t.Fatalf("after retention got %d rows, want 1 (the surviving INSERT)", len(rows))
	}
}

func TestVacuumRetention_RespectsPinRegistry(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "DELETE FROM t WHERE id = 1")

	// Simulate an active reader pin without holding db.mu (today BeginTx holds the
	// lock for its lifetime, so the lock-free-reader scenario isn't yet exercisable).
	pinTs := db.nextCommitTs.Load() - 1
	db.mu.Lock()
	db.pinnedReadTs[pinTs] = 1
	db.mu.Unlock()
	cutoff := db.nextCommitTs.Load() + 100
	n, err := db.VacuumRetention(cutoff)
	if err != nil {
		t.Fatalf("VacuumRetention: %v", err)
	}
	if n != 0 {
		t.Fatalf("pinned reader at ts=%d should block retirement (cutoff=%d), got %d retired",
			pinTs, cutoff, n)
	}
	db.mu.Lock()
	delete(db.pinnedReadTs, pinTs)
	db.mu.Unlock()
	n, err = db.VacuumRetention(cutoff)
	if err != nil {
		t.Fatalf("VacuumRetention after unpin: %v", err)
	}
	if n != 1 {
		t.Fatalf("after unpin: expected 1 retired, got %d", n)
	}
}

func snapshotOf(t *testing.T, db *DB, table string) storage.ManifestView {
	t.Helper()
	db.mu.Lock()
	defer db.mu.Unlock()
	m, err := db.manifestFor(table)
	if err != nil {
		t.Fatalf("manifestFor: %v", err)
	}
	return m.Snapshot()
}

func TestVacuumRetention_BelowCutoffOnly(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	tsBeforeInsert := db.nextCommitTs.Load()
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "DELETE FROM t WHERE id = 1")
	// Cutoff equals or precedes the segment's own CommitTs: strict-below leaves it alone.
	n, err := db.VacuumRetention(tsBeforeInsert + 1)
	if err != nil {
		t.Fatalf("VacuumRetention: %v", err)
	}
	if n != 0 {
		t.Fatalf("cutoff not above seg.CommitTs: expected 0, got %d", n)
	}
}
