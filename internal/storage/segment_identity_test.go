// Round-trip tests for the segment identity sidecar. A segment written with identity
// must surface TableID, SchemaGeneration, and per-column ColumnID on Open. Legacy
// segments written via WriteSegment carry zero identity and must still load.
package storage

import (
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestSegmentIdentity_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	id := SegmentIdentity{
		TableID:          7,
		SchemaGeneration: 13,
		ColumnIDs:        []uint64{101},
	}
	if _, err := WriteSegmentWithIdentity(path, []vector.Batch{makeIntBatch(t, "id", 0, 4)}, nil, id); err != nil {
		t.Fatalf("WriteSegmentWithIdentity: %v", err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()
	if seg.TableID != id.TableID || seg.SchemaGeneration != id.SchemaGeneration {
		t.Fatalf("segment identity: got TableID=%d gen=%d, want %d/%d", seg.TableID, seg.SchemaGeneration, id.TableID, id.SchemaGeneration)
	}
	if len(seg.Cols) != 1 || seg.Cols[0].ColumnID != id.ColumnIDs[0] {
		t.Fatalf("column identity: got %+v, want ColumnID=%d", seg.Cols, id.ColumnIDs[0])
	}
}

func TestSegmentIdentity_AbsentOnLegacyWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if _, err := WriteSegment(path, []vector.Batch{makeIntBatch(t, "id", 0, 4)}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()
	if seg.TableID != 0 || seg.SchemaGeneration != 0 {
		t.Fatalf("legacy segment must carry zero identity, got TableID=%d gen=%d", seg.TableID, seg.SchemaGeneration)
	}
	if seg.Cols[0].ColumnID != 0 {
		t.Fatalf("legacy column must carry zero ColumnID, got %d", seg.Cols[0].ColumnID)
	}
}

func TestSegmentIdentity_RejectsColumnCountMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	id := SegmentIdentity{
		TableID:          1,
		SchemaGeneration: 1,
		// Two ids but the batch only writes one column. The reader must refuse to
		// silently pad-or-truncate; this is the contract that lets ColumnID lookups
		// stay safe.
		ColumnIDs: []uint64{1, 2},
	}
	if _, err := WriteSegmentWithIdentity(path, []vector.Batch{makeIntBatch(t, "id", 0, 4)}, nil, id); err != nil {
		t.Fatalf("WriteSegmentWithIdentity: %v", err)
	}
	if _, err := OpenSegment(path); err == nil {
		t.Fatal("expected error on identity column count mismatch")
	}
}
