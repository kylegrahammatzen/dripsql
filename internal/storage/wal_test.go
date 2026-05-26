// WAL tests: round-trip, truncated-tail recovery, corrupt-crc tail truncation, reopen size growth.
package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestWAL_AppendAndReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, recs, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if len(recs) != 0 {
		t.Fatalf("fresh wal: want 0 records, got %d", len(recs))
	}
	want := []WALRecord{
		{Type: 1, Payload: []byte("hello")},
		{Type: 2, Payload: []byte{}},
		{Type: 1, Payload: bytes.Repeat([]byte{0xAB}, 1024)},
	}
	for _, r := range want {
		if _, err := w.Append(r); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	w.Close()

	w2, got, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	if len(got) != len(want) {
		t.Fatalf("got %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Type != want[i].Type {
			t.Errorf("rec %d: type %d != %d", i, got[i].Type, want[i].Type)
		}
		if !bytes.Equal(got[i].Payload, want[i].Payload) {
			t.Errorf("rec %d: payload mismatch", i)
		}
	}
}

func TestWAL_TruncatedTailDropped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, _, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := w.Append(WALRecord{Type: 1, Payload: []byte("good")}); err != nil {
		t.Fatalf("append: %v", err)
	}
	size := w.Size()
	w.Close()

	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	f.Write([]byte{0x01, 0x10, 0x00})
	f.Close()

	w2, recs, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	if len(recs) != 1 || string(recs[0].Payload) != "good" {
		t.Fatalf("expected 1 good record, got %+v", recs)
	}
	info, _ := os.Stat(path)
	if info.Size() != size {
		t.Errorf("size after recovery %d != %d", info.Size(), size)
	}
}

func TestWAL_CorruptCRCDropped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, _, _ := OpenWAL(path)
	w.Append(WALRecord{Type: 1, Payload: []byte("a")})
	w.Append(WALRecord{Type: 1, Payload: []byte("b")})
	size1 := w.Size()
	w.Append(WALRecord{Type: 1, Payload: []byte("c")})
	w.Close()

	f, _ := os.OpenFile(path, os.O_WRONLY, 0)
	f.WriteAt([]byte{0xFF, 0xFF, 0xFF, 0xFF}, size1+int64(walFrameHdrLen)+1)
	f.Close()

	w2, recs, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	if len(recs) != 2 {
		t.Fatalf("expected 2 surviving records, got %d", len(recs))
	}
	if string(recs[0].Payload) != "a" || string(recs[1].Payload) != "b" {
		t.Errorf("survivors %q %q", recs[0].Payload, recs[1].Payload)
	}
	info, _ := os.Stat(path)
	if info.Size() != size1 {
		t.Errorf("size after corrupt recovery %d != %d", info.Size(), size1)
	}
}

// Smashing the second record's slot with garbage must leave the durable first record intact.
func TestWAL_PriorSlotSurvivesTornWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, _, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := w.Append(WALRecord{Type: 1, Payload: []byte("durable")}); err != nil {
		t.Fatalf("append1: %v", err)
	}
	sizeAfterFirst := w.Size()
	if _, err := w.Append(WALRecord{Type: 1, Payload: []byte("torn")}); err != nil {
		t.Fatalf("append2: %v", err)
	}
	w.Close()

	garbage := make([]byte, walPageSize)
	for i := range garbage {
		garbage[i] = 0xAA
	}
	f, _ := os.OpenFile(path, os.O_WRONLY, 0)
	f.WriteAt(garbage, sizeAfterFirst)
	f.Close()

	w2, recs, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer w2.Close()
	if len(recs) != 1 || string(recs[0].Payload) != "durable" {
		t.Fatalf("expected 1 durable record, got %+v", recs)
	}
	info, _ := os.Stat(path)
	if info.Size() != sizeAfterFirst {
		t.Errorf("size after recovery %d != %d", info.Size(), sizeAfterFirst)
	}
}

func TestWAL_AppendAfterReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.wal")
	w, _, _ := OpenWAL(path)
	w.Append(WALRecord{Type: 1, Payload: []byte("first")})
	w.Close()

	w, recs, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("want 1 rec, got %d", len(recs))
	}
	if _, err := w.Append(WALRecord{Type: 2, Payload: []byte("second")}); err != nil {
		t.Fatalf("append after reopen: %v", err)
	}
	w.Close()

	w2, recs, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("reopen2: %v", err)
	}
	defer w2.Close()
	if len(recs) != 2 {
		t.Fatalf("want 2 recs, got %d", len(recs))
	}
	if string(recs[0].Payload) != "first" || string(recs[1].Payload) != "second" {
		t.Errorf("order broken: %q %q", recs[0].Payload, recs[1].Payload)
	}
}

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
