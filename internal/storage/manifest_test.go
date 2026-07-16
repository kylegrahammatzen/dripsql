// Manifest tests covering open-empty, append, reload, snapshot/At semantics, and CRC tail truncation.
package storage

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestManifest_OpenEmpty_SnapshotEmpty(t *testing.T) {
	m, err := OpenManifest(filepath.Join(t.TempDir(), "m.jsonl"))
	if err != nil {
		t.Fatalf("OpenManifest: %v", err)
	}
	defer m.Close()
	v := m.Snapshot()
	if v.Version != 0 || len(v.Entries) != 0 {
		t.Fatalf("empty manifest snapshot got %+v", v)
	}
}

func TestManifest_AppendAssignsMonotonicVersions(t *testing.T) {
	m, _ := OpenManifest(filepath.Join(t.TempDir(), "m.jsonl"))
	defer m.Close()
	for i := range 3 {
		if err := m.Append(ManifestEntry{Path: "seg.dsv4", Rows: uint32(i)}); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	v := m.Snapshot()
	if v.Version != 3 {
		t.Fatalf("version = %d, want 3", v.Version)
	}
	for i, e := range v.Entries {
		if e.Version != uint64(i+1) {
			t.Fatalf("entry %d version = %d, want %d", i, e.Version, i+1)
		}
	}
}

func TestManifest_ReloadRestoresEntriesAndVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "m.jsonl")
	m, _ := OpenManifest(path)
	for i := range 3 {
		_ = m.Append(ManifestEntry{Path: "seg.dsv4", Rows: uint32(i), ContentHash: 0xDEAD})
	}
	m.Close()

	m2, err := OpenManifest(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer m2.Close()
	v := m2.Snapshot()
	if v.Version != 3 || len(v.Entries) != 3 {
		t.Fatalf("reopened snapshot got %+v", v)
	}
	if err := m2.Append(ManifestEntry{Path: "seg4.dsv4"}); err != nil {
		t.Fatalf("Append after reopen: %v", err)
	}
	v = m2.Snapshot()
	if v.Version != 4 {
		t.Fatalf("next version after reopen = %d, want 4", v.Version)
	}
}

func TestManifest_Snapshot_StableViewAfterLaterAppends(t *testing.T) {
	m, _ := OpenManifest(filepath.Join(t.TempDir(), "m.jsonl"))
	defer m.Close()
	_ = m.Append(ManifestEntry{Path: "a"})
	_ = m.Append(ManifestEntry{Path: "b"})
	atTwo := m.Snapshot()
	_ = m.Append(ManifestEntry{Path: "c"})
	_ = m.Append(ManifestEntry{Path: "d"})
	if len(atTwo.Entries) != 2 || atTwo.Entries[1].Path != "b" {
		t.Fatalf("Snapshot drifted after later appends: %+v", atTwo)
	}
}

func TestManifest_SnapshotAt_FutureTs_ReturnsAll(t *testing.T) {
	m, _ := OpenManifest(filepath.Join(t.TempDir(), "m.jsonl"))
	defer m.Close()
	_ = m.Append(ManifestEntry{Path: "a"})
	v := m.SnapshotAt(999)
	if len(v.Entries) != 1 {
		t.Fatalf("SnapshotAt(future) got %d entries, want 1", len(v.Entries))
	}
}

func TestManifest_BadTailCRC_DroppedAndTruncated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "m.jsonl")
	m, _ := OpenManifest(path)
	_ = m.Append(ManifestEntry{Path: "a"})
	_ = m.Append(ManifestEntry{Path: "b"})
	m.Close()

	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open for append: %v", err)
	}
	_, _ = f.WriteString(`{"path":"c"}` + "\t" + "deadbeef" + "\n")
	f.Close()

	m2, err := OpenManifest(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer m2.Close()
	v := m2.Snapshot()
	if len(v.Entries) != 2 {
		t.Fatalf("bad CRC tail should be dropped; got %d entries", len(v.Entries))
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	expectedLine1, _ := encodeManifestLine(ManifestEntry{Version: 1, Path: "a"})
	expectedLine2, _ := encodeManifestLine(ManifestEntry{Version: 2, Path: "b"})
	if info.Size() != int64(len(expectedLine1)+len(expectedLine2)) {
		t.Fatalf("file size %d != expected %d after truncate", info.Size(), len(expectedLine1)+len(expectedLine2))
	}
}

func TestManifest_BadTailMidLine_Dropped(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "m.jsonl")
	m, _ := OpenManifest(path)
	_ = m.Append(ManifestEntry{Path: "a"})
	m.Close()

	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	_, _ = f.WriteString(`{"path":"b","rows":1}` + "\t" + "1234")
	f.Close()

	m2, err := OpenManifest(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer m2.Close()
	v := m2.Snapshot()
	if len(v.Entries) != 1 {
		t.Fatalf("partial tail line must be dropped; got %d entries", len(v.Entries))
	}
}

func TestManifest_DeletionVectorPath_OmittedByDefault(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "m.jsonl")
	m, _ := OpenManifest(path)
	_ = m.Append(ManifestEntry{Path: "a", Rows: 5})
	m.Close()

	data, _ := os.ReadFile(path)
	if bytes.Contains(data, []byte("deletion_vector_path")) {
		t.Fatalf("empty DeletionVectorPath should be omitted from JSON; got %s", data)
	}
}

// Read-only manifests must tolerate a missing file, fold in appends via Reload, retry a
// torn tail on the next pass, refuse writes, and never change a byte on disk.
func TestManifest_ReadOnly_ReloadAppendsAndRetriesTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "m.jsonl")

	r, err := OpenManifestReadOnly(path)
	if err != nil {
		t.Fatalf("OpenManifestReadOnly missing file: %v", err)
	}
	defer r.Close()
	if v := r.Snapshot(); v.Version != 0 || len(v.Entries) != 0 {
		t.Fatalf("missing file must read as empty, got %+v", v)
	}

	w, err := OpenManifest(path)
	if err != nil {
		t.Fatalf("OpenManifest: %v", err)
	}
	_ = w.Append(ManifestEntry{Path: "a"})
	_ = w.Append(ManifestEntry{Path: "b"})
	w.Close()
	if err := r.Reload(); err != nil {
		t.Fatalf("Reload: %v", err)
	}
	if got := len(r.Snapshot().Entries); got != 2 {
		t.Fatalf("entries after reload = %d, want 2", got)
	}

	line, err := encodeManifestLine(ManifestEntry{Version: 3, Path: "c"})
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(line[:len(line)/2]); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := r.Reload(); err != nil {
		t.Fatalf("Reload over torn tail: %v", err)
	}
	if got := len(r.Snapshot().Entries); got != 2 {
		t.Fatalf("torn tail must be dropped, entries = %d, want 2", got)
	}

	f, err = os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(line[len(line)/2:]); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if err := r.Reload(); err != nil {
		t.Fatalf("Reload after tail completes: %v", err)
	}
	v := r.Snapshot()
	if len(v.Entries) != 3 || v.Entries[2].Path != "c" {
		t.Fatalf("completed tail must appear, got %+v", v.Entries)
	}

	if err := r.Append(ManifestEntry{Path: "d"}); err == nil {
		t.Fatal("Append on a read-only manifest must error")
	}
	lineA, _ := encodeManifestLine(ManifestEntry{Version: 1, Path: "a"})
	lineB, _ := encodeManifestLine(ManifestEntry{Version: 2, Path: "b"})
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if want := int64(len(lineA) + len(lineB) + len(line)); info.Size() != want {
		t.Fatalf("read-only path must not change the file, size %d want %d", info.Size(), want)
	}
}

func TestManifest_AppendAfterClose_Errors(t *testing.T) {
	m, _ := OpenManifest(filepath.Join(t.TempDir(), "m.jsonl"))
	m.Close()
	if err := m.Append(ManifestEntry{Path: "a"}); err == nil {
		t.Fatal("Append after Close must error")
	}
}
