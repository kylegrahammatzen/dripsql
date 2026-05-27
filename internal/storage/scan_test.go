// Scan tests: enumerate without predicate, filtered scan, segment-prune skipping,
// per-segment column-order tolerance, cross-segment kind mismatch detection.
package storage

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func openWrittenSegment(t *testing.T, dir, name string, pages []vector.Batch) *Segment {
	t.Helper()
	path := filepath.Join(dir, name)
	if _, err := WriteSegment(path, pages, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	return seg
}

func makeIntValuesBatch(t *testing.T, name string, vals ...int64) vector.Batch {
	t.Helper()
	v := vector.NewVec(vector.VecInt64, len(vals))
	copy(v.I64(), vals)
	b, err := vector.NewBatch([]vector.Column{{Name: name, Type: schema.Int64, V: v}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return b
}

func makeNullableIntValuesBatch(t *testing.T, name string, vals []int64, nullRows ...int) vector.Batch {
	t.Helper()
	v := vector.NewVec(vector.VecInt64, len(vals))
	copy(v.I64(), vals)
	v.Valid = vector.NewValidity(len(vals))
	for _, row := range nullRows {
		v.Valid.SetInvalid(row)
	}
	b, err := vector.NewBatch([]vector.Column{{Name: name, Type: schema.Int64, V: v}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return b
}

func containsInt64(vals []int64, want int64) bool {
	for _, v := range vals {
		if v == want {
			return true
		}
	}
	return false
}

func TestScan_EnumerateAll_NoPredicate(t *testing.T) {
	seg := openWrittenSegment(t, t.TempDir(), "seg.dsv4", []vector.Batch{
		makeIntBatch(t, "id", 0, 50),
		makeIntBatch(t, "id", 50, 50),
	})
	defer seg.Close()

	var seen []int64
	err := Scan(ScanOpts{Segments: []*Segment{seg}, Columns: []string{"id"}}, func(b vector.Batch, sel *vector.SelectionMask) error {
		sel.IterSet(func(row int) {
			seen = append(seen, b.Columns[0].V.I64()[row])
		})
		return nil
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(seen) != 100 {
		t.Fatalf("saw %d rows, want 100", len(seen))
	}
	for i, v := range seen {
		if v != int64(i) {
			t.Fatalf("row %d: got %d want %d", i, v, i)
		}
	}
}

func TestScan_FiltersWithPredicate(t *testing.T) {
	seg := openWrittenSegment(t, t.TempDir(), "seg.dsv4", []vector.Batch{makeIntBatch(t, "id", 0, 100)})
	defer seg.Close()

	var seen []int64
	err := Scan(ScanOpts{
		Segments: []*Segment{seg},
		Columns:  []string{"id"},
		Pred:     &Pred{Op: OpLt, Col: "id", Kind: vector.VecInt64, I64: 10},
	}, func(b vector.Batch, sel *vector.SelectionMask) error {
		sel.IterSet(func(row int) { seen = append(seen, b.Columns[0].V.I64()[row]) })
		return nil
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(seen) != 10 {
		t.Fatalf("saw %d rows, want 10", len(seen))
	}
}

func TestScan_MetadataMatchesPageForAllRows(t *testing.T) {
	seg := openWrittenSegment(t, t.TempDir(), "seg.dsv4", []vector.Batch{makeIntBatch(t, "id", 0, 10)})
	defer seg.Close()
	seg.LoadPageStats()

	compiled, err := CompilePred(Pred{Op: OpLt, Col: "id", Kind: vector.VecInt64, I64: 100})
	if err != nil {
		t.Fatalf("CompilePred: %v", err)
	}
	if !compiled.MatchesPage(seg, 0) {
		t.Fatal("id < 100 must be recognized as matching the whole page")
	}

	nullable := openWrittenSegment(t, t.TempDir(), "nullable.dsv4", []vector.Batch{
		makeNullableIntValuesBatch(t, "id", []int64{1, 2, 3}, 1),
	})
	defer nullable.Close()
	nullable.LoadPageStats()
	if compiled.MatchesPage(nullable, 0) {
		t.Fatal("nullable page must not be marked as a whole-page match")
	}
}

func TestScan_TopKPushdownKeepsMixedRangeCandidatePage(t *testing.T) {
	seg := openWrittenSegment(t, t.TempDir(), "seg.dsv4", []vector.Batch{
		makeIntValuesBatch(t, "id", 100, 0),
		makeIntValuesBatch(t, "id", 99, 98),
	})
	defer seg.Close()

	var seen []int64
	err := Scan(ScanOpts{
		Segments: []*Segment{seg},
		Columns:  []string{"id"},
		TopK:     &TopKPushdown{Column: "id", Desc: true, K: 2},
	}, func(b vector.Batch, sel *vector.SelectionMask) error {
		sel.IterSet(func(row int) { seen = append(seen, b.Columns[0].V.I64()[row]) })
		return nil
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !containsInt64(seen, 99) {
		t.Fatalf("top-K pushdown pruned a page needed for final sorting; saw %v, want value 99", seen)
	}
}

func TestScan_TopKPushdownDoesNotPruneNullablePages(t *testing.T) {
	seg := openWrittenSegment(t, t.TempDir(), "seg.dsv4", []vector.Batch{
		makeIntValuesBatch(t, "id", 5, 6),
		makeNullableIntValuesBatch(t, "id", []int64{0, 100}, 0),
	})
	defer seg.Close()

	sawNull := false
	err := Scan(ScanOpts{
		Segments: []*Segment{seg},
		Columns:  []string{"id"},
		TopK:     &TopKPushdown{Column: "id", K: 1},
	}, func(b vector.Batch, sel *vector.SelectionMask) error {
		col := b.Columns[0]
		sel.IterSet(func(row int) {
			if col.V.Valid != nil && !col.V.Valid.IsValid(row) {
				sawNull = true
			}
		})
		return nil
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !sawNull {
		t.Fatal("ascending top-K pushdown must not prune nullable pages because nulls sort first")
	}
}

func TestScan_TopKPushdownDoesNotPruneWithDeletionVector(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.dsv4")
	if _, err := WriteSegment(path, []vector.Batch{
		makeIntValuesBatch(t, "id", 100, 0),
		makeIntValuesBatch(t, "id", 99, 98),
	}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	dv := vector.NewValidity(4)
	dv.SetInvalid(0)
	if err := WriteDV(path, 4, dv); err != nil {
		t.Fatalf("WriteDV: %v", err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()

	var seen []int64
	err = Scan(ScanOpts{
		Segments: []*Segment{seg},
		Columns:  []string{"id"},
		TopK:     &TopKPushdown{Column: "id", Desc: true, K: 1},
	}, func(b vector.Batch, sel *vector.SelectionMask) error {
		sel.IterSet(func(row int) { seen = append(seen, b.Columns[0].V.I64()[row]) })
		return nil
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if !containsInt64(seen, 99) {
		t.Fatalf("top-K pushdown must account for deleted rows before pruning; saw %v, want value 99", seen)
	}
}

func TestScan_TopKPushdownPrunesClusteredPages(t *testing.T) {
	// Pages laid out so per-page min/max are disjoint: [0..99], [100..199], [200..299], [300..399].
	// DESC top-2 must scan only the last page; ASC top-2 must scan only the first.
	dir := t.TempDir()
	mk := func(start int64) vector.Batch {
		vals := make([]int64, 100)
		for i := range vals {
			vals[i] = start + int64(i)
		}
		return makeIntValuesBatch(t, "id", vals...)
	}
	pages := []vector.Batch{mk(0), mk(100), mk(200), mk(300)}
	path := filepath.Join(dir, "seg.dsv4")
	if _, err := WriteSegment(path, pages, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()

	cases := []struct {
		name      string
		desc      bool
		wantRange [2]int64
	}{
		{"desc keeps only top page", true, [2]int64{300, 399}},
		{"asc keeps only bottom page", false, [2]int64{0, 99}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var seen []int64
			err := Scan(ScanOpts{
				Segments: []*Segment{seg},
				Columns:  []string{"id"},
				TopK:     &TopKPushdown{Column: "id", Desc: tc.desc, K: 2},
			}, func(b vector.Batch, sel *vector.SelectionMask) error {
				sel.IterSet(func(row int) { seen = append(seen, b.Columns[0].V.I64()[row]) })
				return nil
			})
			if err != nil {
				t.Fatalf("Scan: %v", err)
			}
			if len(seen) != 100 {
				t.Fatalf("scanned %d rows, want 100 (only one page should survive pruning)", len(seen))
			}
			for _, v := range seen {
				if v < tc.wantRange[0] || v > tc.wantRange[1] {
					t.Fatalf("scanned value %d outside expected page range %v", v, tc.wantRange)
				}
			}
		})
	}
}

func TestScan_PrunesSegmentsOutsideRange(t *testing.T) {
	dir := t.TempDir()
	low := openWrittenSegment(t, dir, "low.dsv4", []vector.Batch{makeIntBatch(t, "id", 0, 50)})
	high := openWrittenSegment(t, dir, "high.dsv4", []vector.Batch{makeIntBatch(t, "id", 1000, 50)})
	defer low.Close()
	defer high.Close()

	callCount := 0
	err := Scan(ScanOpts{
		Segments: []*Segment{low, high},
		Columns:  []string{"id"},
		Pred:     &Pred{Op: OpEq, Col: "id", Kind: vector.VecInt64, I64: 25},
	}, func(b vector.Batch, sel *vector.SelectionMask) error {
		callCount++
		return nil
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if callCount != 1 {
		t.Fatalf("fn called %d times, want 1 (high segment must be pruned)", callCount)
	}
}

func TestScan_StopsOnFnError(t *testing.T) {
	seg := openWrittenSegment(t, t.TempDir(), "seg.dsv4", []vector.Batch{
		makeIntBatch(t, "id", 0, 10),
		makeIntBatch(t, "id", 10, 10),
		makeIntBatch(t, "id", 20, 10),
	})
	defer seg.Close()

	sentinel := errors.New("stop")
	calls := 0
	err := Scan(ScanOpts{Segments: []*Segment{seg}, Columns: []string{"id"}}, func(b vector.Batch, sel *vector.SelectionMask) error {
		calls++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("Scan must surface fn error, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("fn called %d times after error, want 1", calls)
	}
}

func TestScan_ProjectionDropsColumns(t *testing.T) {
	seg := openWrittenSegment(t, t.TempDir(), "seg.dsv4", []vector.Batch{makeTwoColumnBatch(t, 0, 32)})
	defer seg.Close()

	var seenCols int
	err := Scan(ScanOpts{Segments: []*Segment{seg}, Columns: []string{"id"}}, func(b vector.Batch, sel *vector.SelectionMask) error {
		seenCols = len(b.Columns)
		return nil
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if seenCols != 1 {
		t.Fatalf("scanned batch carries %d columns, want 1", seenCols)
	}
}

func TestScan_EmptyColumnsScansAll(t *testing.T) {
	seg := openWrittenSegment(t, t.TempDir(), "seg.dsv4", []vector.Batch{makeTwoColumnBatch(t, 0, 32)})
	defer seg.Close()

	var seenCols int
	err := Scan(ScanOpts{Segments: []*Segment{seg}}, func(b vector.Batch, sel *vector.SelectionMask) error {
		seenCols = len(b.Columns)
		return nil
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if seenCols != 2 {
		t.Fatalf("empty Columns must scan all columns, got %d", seenCols)
	}
}

func TestScan_RejectsMissingColumn(t *testing.T) {
	seg := openWrittenSegment(t, t.TempDir(), "seg.dsv4", []vector.Batch{makeIntBatch(t, "id", 0, 5)})
	defer seg.Close()
	err := Scan(ScanOpts{Segments: []*Segment{seg}, Columns: []string{"nope"}}, func(b vector.Batch, sel *vector.SelectionMask) error {
		return nil
	})
	if err == nil {
		t.Fatal("Scan must error when projection references a missing column")
	}
}

func TestScan_RejectsNilFn(t *testing.T) {
	if err := Scan(ScanOpts{}, nil); err == nil {
		t.Fatal("Scan must reject nil fn")
	}
}

func TestScan_PredicateOnUnprojectedColumn(t *testing.T) {
	seg := openWrittenSegment(t, t.TempDir(), "seg.dsv4", []vector.Batch{makeTwoColumnBatch(t, 0, 32)})
	defer seg.Close()

	var seen []int64
	err := Scan(ScanOpts{
		Segments: []*Segment{seg},
		Columns:  []string{"tag"},
		Pred:     &Pred{Op: OpLt, Col: "id", Kind: vector.VecInt64, I64: 5},
	}, func(b vector.Batch, sel *vector.SelectionMask) error {
		if len(b.Columns) != 1 || b.Columns[0].Name != "tag" {
			t.Fatalf("callback batch shape wrong: %d cols, first=%q", len(b.Columns), b.Columns[0].Name)
		}
		sel.IterSet(func(row int) { seen = append(seen, int64(row)) })
		return nil
	})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(seen) != 5 {
		t.Fatalf("predicate on unprojected column matched %d rows, want 5", len(seen))
	}
}

func TestScan_ProjectionCaseInsensitive(t *testing.T) {
	seg := openWrittenSegment(t, t.TempDir(), "seg.dsv4", []vector.Batch{makeIntBatch(t, "id", 0, 5)})
	defer seg.Close()
	err := Scan(ScanOpts{Segments: []*Segment{seg}, Columns: []string{"ID"}}, func(b vector.Batch, sel *vector.SelectionMask) error {
		if b.Columns[0].Name != "ID" {
			t.Fatalf("column name = %q, want %q", b.Columns[0].Name, "ID")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Scan must accept case-insensitive projection: %v", err)
	}
}

func TestScan_NoSegments_NoOp(t *testing.T) {
	calls := 0
	err := Scan(ScanOpts{}, func(b vector.Batch, sel *vector.SelectionMask) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("Scan with no segments must succeed: %v", err)
	}
	if calls != 0 {
		t.Fatal("fn must not be called when there are no segments")
	}
}

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

func TestScan_ByColumnID_SynthesisesNullForUnknownID(t *testing.T) {
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

	rows := 0
	nulls := 0
	err = Scan(ScanOpts{
		Segments:    []*Segment{seg},
		Columns:     []string{"id", "added_later"},
		ColumnIDs:   []uint64{1, 999},
		ColumnKinds: []vector.VecKind{vector.VecInt64, vector.VecInt64},
	}, func(b vector.Batch, sel *vector.SelectionMask) error {
		rows += b.Len
		col := b.Columns[1]
		if col.Name != "added_later" {
			t.Fatalf("synthesised column name = %q, want %q", col.Name, "added_later")
		}
		for r := 0; r < b.Len; r++ {
			if col.V.Valid == nil || !col.V.Valid.IsValid(r) {
				nulls++
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if rows != 3 || nulls != 3 {
		t.Fatalf("rows=%d nulls=%d, want 3/3", rows, nulls)
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

	// With no identity sidecar the scan ignores ColumnIDs and matches by name.
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

func TestScan_PrunesSegmentForMissingColumnIsNotNull(t *testing.T) {
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
			Op: OpNot,
			Children: []Pred{
				{Op: OpIsNull, Col: "added", ColID: 7, Kind: vector.VecInt64},
			},
		},
	}, func(b vector.Batch, sel *vector.SelectionMask) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if calls != 0 {
		t.Fatalf("scan emitted %d batches but IS NOT NULL on a missing-null column should prune", calls)
	}
}

func TestScan_KeepsSegmentForMissingColumnIsNull(t *testing.T) {
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
		Pred:        &Pred{Op: OpIsNull, Col: "added", ColID: 7, Kind: vector.VecInt64},
	}, func(b vector.Batch, sel *vector.SelectionMask) error {
		rows += sel.PopCount()
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if rows != 5 {
		t.Fatalf("rows = %d, want 5 (every synthetic null matches IS NULL)", rows)
	}
}

func TestScan_PrunesSegmentForAndOfMissingComparisons(t *testing.T) {
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
		Columns:     []string{"id", "a", "b"},
		ColumnIDs:   []uint64{1, 7, 8},
		ColumnKinds: []vector.VecKind{vector.VecInt64, vector.VecInt64, vector.VecInt64},
		Pred: &Pred{
			Op: OpAnd,
			Children: []Pred{
				{Op: OpEq, Col: "a", ColID: 7, Kind: vector.VecInt64, I64: 1},
				{Op: OpEq, Col: "b", ColID: 8, Kind: vector.VecInt64, I64: 2},
			},
		},
	}, func(b vector.Batch, sel *vector.SelectionMask) error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if calls != 0 {
		t.Fatalf("scan emitted %d batches but AND of two missing-null comparisons should prune", calls)
	}
}

func TestScan_KeepsSegmentForOrWithIsNull(t *testing.T) {
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
		Pred: &Pred{
			Op: OpOr,
			Children: []Pred{
				{Op: OpEq, Col: "added", ColID: 7, Kind: vector.VecInt64, I64: 42},
				{Op: OpIsNull, Col: "added", ColID: 7, Kind: vector.VecInt64},
			},
		},
	}, func(b vector.Batch, sel *vector.SelectionMask) error {
		rows += sel.PopCount()
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if rows != 5 {
		t.Fatalf("rows = %d, want 5 (the IS NULL branch always matches)", rows)
	}
}
