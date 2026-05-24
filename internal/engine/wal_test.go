// WAL plumbing tests: clean Open/Close, intent+commit pair appended by UPDATE, orphan
// segment cleanup on recovery when an intent records a path that never reached the manifest.
package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
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

	// Simulate a crashed UPDATE: an orphan segment file exists, and the WAL has an
	// intent for it with no matching commit. Recovery should delete the file.
	tableDir := filepath.Join(dir, "segments", "t")
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
	pending, err := storage.PendingTxns(records)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected no pending intents after recovery, got %d", len(pending))
	}
}
