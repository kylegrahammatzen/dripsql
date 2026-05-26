// Scan with ScanOpts.ColumnIDs matches segments by stable catalog id rather than by
// name. This unlocks rename-as-metadata-only at the catalog level. Legacy segments
// written without the identity sidecar still resolve by name.
package storage

import (
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestScan_ByColumnID_SurvivesRename(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	pages := []vector.Batch{makeIntBatch(t, "old_name", 0, 5)}
	id := SegmentIdentity{TableID: 1, SchemaGeneration: 1, ColumnIDs: []uint64{77}}
	if _, err := WriteSegmentWithIdentity(path, pages, nil, id); err != nil {
		t.Fatal(err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	rows := 0
	err = Scan(ScanOpts{
		Segments:  []*Segment{seg},
		Columns:   []string{"new_name"},
		ColumnIDs: []uint64{77},
	}, func(b vector.Batch, sel *vector.SelectionMask) error {
		rows += b.Len
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if rows != 5 {
		t.Fatalf("rows = %d, want 5", rows)
	}
}

func TestScan_ByColumnID_ErrorsOnUnknownID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	id := SegmentIdentity{TableID: 1, SchemaGeneration: 1, ColumnIDs: []uint64{1}}
	if _, err := WriteSegmentWithIdentity(path, []vector.Batch{makeIntBatch(t, "id", 0, 3)}, nil, id); err != nil {
		t.Fatal(err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	err = Scan(ScanOpts{
		Segments:  []*Segment{seg},
		Columns:   []string{"id"},
		ColumnIDs: []uint64{999},
	}, func(b vector.Batch, sel *vector.SelectionMask) error { return nil })
	if err == nil {
		t.Fatal("expected error when requested column id is missing in a segment with identity")
	}
}

func TestScan_LegacySegmentFallsBackToName(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if _, err := WriteSegment(path, []vector.Batch{makeIntBatch(t, "id", 0, 5)}, nil); err != nil {
		t.Fatal(err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	// Provide IDs anyway; with no identity sidecar the scan must ignore them and
	// match by name. This is the path migrated databases still walk for legacy data.
	rows := 0
	err = Scan(ScanOpts{
		Segments:  []*Segment{seg},
		Columns:   []string{"id"},
		ColumnIDs: []uint64{42},
	}, func(b vector.Batch, sel *vector.SelectionMask) error {
		rows += b.Len
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if rows != 5 {
		t.Fatalf("rows = %d, want 5", rows)
	}
}
