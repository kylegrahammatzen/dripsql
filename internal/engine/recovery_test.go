// Cross-table forward-roll recovery: WAL holds N intents under one TxnID, table 1's
// manifest already absorbed its commit, table 2's didn't. Recovery must apply table 2's
// intent so the txn lands fully instead of half-committed.
package engine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestRecovery_ForwardRollPartialMultiTableCommit(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mustExec(t, db, "CREATE TABLE a (id int64 NOT NULL)")
	mustExec(t, db, "CREATE TABLE b (id int64 NOT NULL)")

	// Build pending segment files (write but do not commit to manifests).
	pathA := db.nextSegmentPath("a")
	pathB := db.nextSegmentPath("b")
	writeSingleInt(t, pathA, 1)
	writeSingleInt(t, pathB, 2)

	// Apply table A's manifest commit. Skip table B's so reopen mirrors a crash
	// between the two manifests within one Tx.Commit loop.
	mA, err := db.manifestFor("a")
	if err != nil {
		t.Fatalf("manifestFor a: %v", err)
	}
	txnID := uint64(9999)
	commitTs := db.nextCommitTs.Add(1)
	intentA := storage.ManifestIntent{TxnID: txnID, CommitTs: commitTs, Table: "a",
		Adds: []storage.ManifestSegmentAdd{{Path: pathA, Rows: 1}}}
	intentB := storage.ManifestIntent{TxnID: txnID, CommitTs: commitTs, Table: "b",
		Adds: []storage.ManifestSegmentAdd{{Path: pathB, Rows: 1}}}
	if _, err := db.wal.AppendManifestIntent(intentA); err != nil {
		t.Fatalf("AppendManifestIntent a: %v", err)
	}
	if _, err := db.wal.AppendManifestIntent(intentB); err != nil {
		t.Fatalf("AppendManifestIntent b: %v", err)
	}
	if err := mA.Commit(commitTs, intentA.Adds, nil); err != nil {
		t.Fatalf("Commit a: %v", err)
	}
	// b's manifest never sees Commit. Close DB without WAL truncation, then reopen.
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()

	rowsA := mustValues(t, db2, "SELECT id FROM a")
	rowsB := mustValues(t, db2, "SELECT id FROM b")
	if len(rowsA) != 1 {
		t.Errorf("table a: got %d rows, want 1 (was already committed)", len(rowsA))
	}
	if len(rowsB) != 1 {
		t.Errorf("table b: got %d rows, want 1 (forward-roll should have applied)", len(rowsB))
	}
}

func TestRecovery_DeleteOrphansWhenNoTableCommitted(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mustExec(t, db, "CREATE TABLE a (id int64 NOT NULL)")
	mustExec(t, db, "CREATE TABLE b (id int64 NOT NULL)")
	pathA := db.nextSegmentPath("a")
	pathB := db.nextSegmentPath("b")
	writeSingleInt(t, pathA, 1)
	writeSingleInt(t, pathB, 2)
	txnID := uint64(1234)
	commitTs := db.nextCommitTs.Add(1)
	intentA := storage.ManifestIntent{TxnID: txnID, CommitTs: commitTs, Table: "a",
		Adds: []storage.ManifestSegmentAdd{{Path: pathA, Rows: 1}}}
	intentB := storage.ManifestIntent{TxnID: txnID, CommitTs: commitTs, Table: "b",
		Adds: []storage.ManifestSegmentAdd{{Path: pathB, Rows: 1}}}
	if _, err := db.wal.AppendManifestIntent(intentA); err != nil {
		t.Fatalf("AppendManifestIntent a: %v", err)
	}
	if _, err := db.wal.AppendManifestIntent(intentB); err != nil {
		t.Fatalf("AppendManifestIntent b: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer db2.Close()
	if len(mustValues(t, db2, "SELECT id FROM a")) != 0 {
		t.Error("table a should be empty after orphan cleanup")
	}
	if len(mustValues(t, db2, "SELECT id FROM b")) != 0 {
		t.Error("table b should be empty after orphan cleanup")
	}
}

func writeSingleInt(t *testing.T, path string, val int64) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	v := types.NewVec(types.VecInt64, 1)
	v.I64()[0] = val
	batch, err := types.NewBatch([]types.Column{{Name: "id", Type: types.Int64, V: v}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	if _, err := storage.WriteSegment(path, []types.Batch{batch}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
}
