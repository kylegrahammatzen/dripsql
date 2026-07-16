// WAL plumbing and crash recovery tests covering clean open and close, intent plus commit pairing, orphan cleanup, cross-table forward-roll, and partial commit fallout.
package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestWAL_OpenCloseRoundTrip(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "wal")); err != nil {
		t.Fatalf("wal file missing: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
}

func TestWAL_TruncatesAfterCommit(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(ctx, "INSERT INTO t (id, v) VALUES (1, 10), (2, 20), (3, 30)"); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := db.Exec(ctx, "UPDATE t SET v = 99 WHERE id = 2"); err != nil {
		t.Fatalf("update: %v", err)
	}
	db.Close()

	w, records, err := storage.OpenWAL(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	defer w.Close()
	if len(records) != 0 {
		t.Fatalf("expected wal truncated to header after commits, got %d records", len(records))
	}
}

func TestWAL_RecoverDeletesOrphanSegment(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()
	if _, err := db.Exec(ctx, "CREATE TABLE t (id int64 NOT NULL)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	db.Close()

	// Simulate a crashed UPDATE where an orphan segment file exists and the WAL has an
	// intent for it with no matching commit, so recovery should delete the file.
	tableDir := filepath.Join(dir, "tables", "0000000000000001")
	orphan := filepath.Join(tableDir, "999999.dsv4")
	if err := os.WriteFile(orphan, []byte("garbage"), 0o644); err != nil {
		t.Fatalf("write orphan: %v", err)
	}
	w, _, err := storage.OpenWAL(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	intent := storage.ManifestIntent{
		TxnID: 999,
		Table: "t",
		Adds:  []storage.ManifestSegmentAdd{{Path: orphan, Rows: 1}},
	}
	if _, err := w.AppendManifestIntent(intent); err != nil {
		t.Fatalf("append intent: %v", err)
	}
	w.Close()

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Fatalf("orphan segment file not deleted (err=%v)", err)
	}

	// Replay should have written a commit record so subsequent restarts see no pending txn.
	db2.Close()
	w2, records, err := storage.OpenWAL(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	defer w2.Close()
	pending, err := storage.PendingTxnGroups(records)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected no pending intents after recovery, got %d", len(pending))
	}
}

func TestRecovery_ForwardRollPartialMultiTableCommit(t *testing.T) {
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

	// Apply table A's manifest commit but skip table B's so reopen mirrors a crash between the two manifests within one Tx.Commit loop.
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
	v := vector.NewVec(vector.VecInt64, 1)
	v.I64()[0] = val
	batch, err := vector.NewBatch([]vector.Column{{Name: "id", Type: schema.Int64, V: v}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	if err := storage.WriteSegment(path, []vector.Batch{batch}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
}
