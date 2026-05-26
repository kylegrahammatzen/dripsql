// A comparison predicate on a column that is missing from a segment and synthesises
// NULL must skip the whole segment because comparisons against NULL never match.
package storage

import (
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestScan_PrunesSegmentForMissingColumnEq(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	id := SegmentIdentity{TableID: 1, SchemaGeneration: 1, ColumnIDs: []uint64{1}}
	if _, err := WriteSegmentWithIdentity(path, []vector.Batch{makeIntBatch(t, "id", 0, 5)}, nil, id); err != nil {
		t.Fatal(err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	calls := 0
	err = Scan(ScanOpts{
		Segments:    []*Segment{seg},
		Columns:     []string{"id", "added"},
		ColumnIDs:   []uint64{1, 7},
		ColumnKinds: []vector.VecKind{vector.VecInt64, vector.VecInt64},
		Pred: &Pred{
			Op:    OpEq,
			Col:   "added",
			ColID: 7,
			Kind:  vector.VecInt64,
			I64:   42,
		},
	}, func(b vector.Batch, sel *vector.SelectionMask) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if calls != 0 {
		t.Fatalf("scan emitted %d batches but the segment should have been pruned", calls)
	}
}

func TestScan_DoesNotPruneWhenDefaultSatisfiesPredicate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	id := SegmentIdentity{TableID: 1, SchemaGeneration: 1, ColumnIDs: []uint64{1}}
	if _, err := WriteSegmentWithIdentity(path, []vector.Batch{makeIntBatch(t, "id", 0, 5)}, nil, id); err != nil {
		t.Fatal(err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatal(err)
	}
	defer seg.Close()

	rows := 0
	err = Scan(ScanOpts{
		Segments:    []*Segment{seg},
		Columns:     []string{"id", "added"},
		ColumnIDs:   []uint64{1, 7},
		ColumnKinds: []vector.VecKind{vector.VecInt64, vector.VecInt64},
		ColumnDefaults: []ScanDefault{
			{},
			{Set: true, I64: 42},
		},
		Pred: &Pred{
			Op:    OpEq,
			Col:   "added",
			ColID: 7,
			Kind:  vector.VecInt64,
			I64:   42,
		},
	}, func(b vector.Batch, sel *vector.SelectionMask) error {
		rows += sel.PopCount()
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if rows != 5 {
		t.Fatalf("rows = %d, want 5 (all rows match because default = literal)", rows)
	}
}

// avoid unused import warning if we trim above
var _ = schema.Bool
