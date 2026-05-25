// Typed WAL records: round-trip per kind, PendingTxns leaves only intents without commits.
package storage

import (
	"path/filepath"
	"testing"
)

func TestWALRecords_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, _, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer w.Close()

	intent := ManifestIntent{
		TxnID:     7,
		Adds:      []ManifestSegmentAdd{{Path: "/seg1.dsv4", Rows: 1000, ContentHash: 0xABCD}},
		DVUpdates: []ManifestDVUpdate{{SegmentPath: "/seg0.dsv4", DVPath: "/seg0.dv", Rows: 800}},
	}
	if _, err := w.AppendManifestIntent(intent); err != nil {
		t.Fatalf("append intent: %v", err)
	}
	commit := ManifestCommit{TxnID: 7, ManifestVersion: 42}
	if _, err := w.AppendManifestCommit(commit); err != nil {
		t.Fatalf("append commit: %v", err)
	}
	cp := Checkpoint{Offset: 1024}
	if _, err := w.AppendCheckpoint(cp); err != nil {
		t.Fatalf("append checkpoint: %v", err)
	}
	w.Close()

	w2, recs, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	if len(recs) != 3 {
		t.Fatalf("want 3 records, got %d", len(recs))
	}
	gotIntent, err := DecodeManifestIntent(recs[0])
	if err != nil {
		t.Fatalf("decode intent: %v", err)
	}
	if gotIntent.TxnID != intent.TxnID || len(gotIntent.Adds) != 1 || gotIntent.Adds[0].Path != "/seg1.dsv4" {
		t.Errorf("intent round-trip mismatch: %+v", gotIntent)
	}
	gotCommit, err := DecodeManifestCommit(recs[1])
	if err != nil {
		t.Fatalf("decode commit: %v", err)
	}
	if gotCommit != commit {
		t.Errorf("commit %+v != %+v", gotCommit, commit)
	}
	gotCP, err := DecodeCheckpoint(recs[2])
	if err != nil {
		t.Fatalf("decode checkpoint: %v", err)
	}
	if gotCP != cp {
		t.Errorf("checkpoint %+v != %+v", gotCP, cp)
	}
}

func TestWALRecords_PendingTxns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, _, _ := OpenWAL(path)

	w.AppendManifestIntent(ManifestIntent{TxnID: 1, Adds: []ManifestSegmentAdd{{Path: "/a"}}})
	w.AppendManifestCommit(ManifestCommit{TxnID: 1, ManifestVersion: 1})
	w.AppendManifestIntent(ManifestIntent{TxnID: 2, Adds: []ManifestSegmentAdd{{Path: "/b"}}})
	w.AppendManifestIntent(ManifestIntent{TxnID: 3, Adds: []ManifestSegmentAdd{{Path: "/c"}}})
	w.AppendManifestCommit(ManifestCommit{TxnID: 3, ManifestVersion: 2})
	w.Close()

	w2, recs, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()

	pending, err := PendingTxns(recs)
	if err != nil {
		t.Fatalf("pending: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("want 1 pending txn, got %d (%+v)", len(pending), pending)
	}
	if pending[0].TxnID != 2 {
		t.Errorf("pending txn id = %d, want 2", pending[0].TxnID)
	}
	if len(pending[0].Adds) != 1 || pending[0].Adds[0].Path != "/b" {
		t.Errorf("pending adds mismatch: %+v", pending[0].Adds)
	}
}

func TestWALRecords_KindMismatchRejected(t *testing.T) {
	rec := WALRecord{Type: WALKindManifestCommit, Payload: []byte(`{"txn_id":1}`)}
	if _, err := DecodeManifestIntent(rec); err == nil {
		t.Fatal("decoding commit as intent must error")
	}
}
