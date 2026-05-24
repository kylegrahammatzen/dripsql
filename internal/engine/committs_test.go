// PR-V1: monotonic commit timestamps stamped onto every manifest record + WAL trace,
// persisted across reopen, exposed via SnapshotAt.
package engine

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

func TestCommitTs_MonotonicAcrossOperations(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL, v int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (1, 10)")
	mustExec(t, db, "INSERT INTO t (id, v) VALUES (2, 20)")
	if _, err := db.Exec(ctx, "UPDATE t SET v = 99 WHERE id = 1"); err != nil {
		t.Fatalf("UPDATE: %v", err)
	}
	if _, err := db.Exec(ctx, "DELETE FROM t WHERE id = 2"); err != nil {
		t.Fatalf("DELETE: %v", err)
	}

	m, err := db.manifestFor("t")
	if err != nil {
		t.Fatalf("manifestFor: %v", err)
	}
	view := m.Snapshot()
	if len(view.Entries) == 0 {
		t.Fatal("expected manifest entries")
	}
	var prev uint64
	seen := 0
	for _, e := range view.Entries {
		if e.CommitTs == 0 {
			t.Errorf("entry %q has zero CommitTs; every PR-V1 record must be stamped", e.Path)
		}
		if e.CommitTs < prev {
			t.Errorf("CommitTs went backwards: %d < %d", e.CommitTs, prev)
		}
		prev = e.CommitTs
		seen++
	}
	if seen < 2 {
		t.Fatalf("expected at least 2 stamped entries, saw %d", seen)
	}
}

func TestCommitTs_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")
	preReopenMax := db.nextCommitTs.Load()
	db.Close()

	db2, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer db2.Close()
	if got := db2.nextCommitTs.Load(); got != preReopenMax {
		t.Fatalf("nextCommitTs after reopen = %d, want %d (must match max persisted CommitTs)", got, preReopenMax)
	}
	mustExec(t, db2, "INSERT INTO t (id) VALUES (3)")
	if got := db2.nextCommitTs.Load(); got <= preReopenMax {
		t.Fatalf("new commit after reopen did not advance counter: %d <= %d", got, preReopenMax)
	}
}

func TestSnapshotAt_FiltersByCommitTs(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	tsAfterFirst := db.nextCommitTs.Load()
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (3)")

	m, err := db.manifestFor("t")
	if err != nil {
		t.Fatalf("manifestFor: %v", err)
	}
	asOfFirst := m.SnapshotAt(tsAfterFirst)
	if len(asOfFirst.Entries) != 1 {
		t.Fatalf("SnapshotAt(tsAfterFirst).Entries = %d, want 1", len(asOfFirst.Entries))
	}
	full := m.Snapshot()
	if len(full.Entries) != 3 {
		t.Fatalf("Snapshot().Entries = %d, want 3", len(full.Entries))
	}
}

func TestCommitTs_SegmentsCarryStampedCommitTs(t *testing.T) {
	// Verifies the engine plumbs each manifest entry's CommitTs onto the corresponding
	// *storage.Segment so the scan visibility filter has something to compare ReadTs against.
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")
	mustExec(t, db, "INSERT INTO t (id) VALUES (1)")
	tsAfterFirst := db.nextCommitTs.Load()
	mustExec(t, db, "INSERT INTO t (id) VALUES (2)")

	segs, err := db.openSegmentsForQuery("t")
	if err != nil {
		t.Fatalf("openSegmentsForQuery: %v", err)
	}
	if len(segs) != 2 {
		t.Fatalf("got %d segments, want 2", len(segs))
	}
	var olderSeen, newerSeen bool
	for _, s := range segs {
		if s.CommitTs == 0 {
			t.Errorf("segment %q has zero CommitTs; engine must stamp from manifest entry", s.Path())
		}
		if s.CommitTs == tsAfterFirst {
			olderSeen = true
		}
		if s.CommitTs > tsAfterFirst {
			newerSeen = true
		}
	}
	if !olderSeen || !newerSeen {
		t.Fatalf("expected one segment at ts=%d and one at ts>%d; got %+v",
			tsAfterFirst, tsAfterFirst, segCommitTimestamps(segs))
	}
}

func segCommitTimestamps(segs []*storage.Segment) []uint64 {
	out := make([]uint64, len(segs))
	for i, s := range segs {
		out[i] = s.CommitTs
	}
	return out
}

func TestCommitTs_WALRecordsCarryIt(t *testing.T) {
	// Use a sandboxed DB so we can inspect the WAL right after a commit but before
	// the post-commit TruncateToHeader runs by intercepting via the on-disk WAL file
	// is impossible (truncate happens synchronously). Instead we verify the manifest
	// records and the test for orphan recovery covers the WAL field separately.
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")
	w, _, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Fatalf("OpenWAL: %v", err)
	}
	intent := storage.ManifestIntent{TxnID: 1, CommitTs: 42, Table: "t",
		Adds: []storage.ManifestSegmentAdd{{Path: "/x", Rows: 1}}}
	if _, err := w.AppendManifestIntent(intent); err != nil {
		t.Fatalf("AppendManifestIntent: %v", err)
	}
	w.Close()

	w2, records, err := storage.OpenWAL(walPath)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer w2.Close()
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	got, err := storage.DecodeManifestIntent(records[0])
	if err != nil {
		t.Fatalf("DecodeManifestIntent: %v", err)
	}
	if got.CommitTs != 42 {
		t.Errorf("ManifestIntent.CommitTs = %d, want 42", got.CommitTs)
	}
}
