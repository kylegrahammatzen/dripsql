// Predicate tests: Bind validation, Eval correctness per op + composites, PruneSegment soundness.
package storage

import (
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func buildIntBatch(t *testing.T, name string, vals []int64) types.Batch {
	t.Helper()
	v := types.NewVec(types.VecInt64, len(vals))
	copy(v.I64(), vals)
	b, err := types.NewBatch([]types.Column{{Name: name, Type: types.Int64, V: v}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return b
}

func buildBytesBatch(t *testing.T, name string, vals []string) types.Batch {
	t.Helper()
	v := types.NewVarVec(types.VecText, len(vals), 0)
	vb := v.Var()
	for i, s := range vals {
		vb.AppendString(i, s)
	}
	b, err := types.NewBatch([]types.Column{{Name: name, Type: types.Text, V: v}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return b
}

func selectedRows(sel *types.SelectionMask) []int {
	var out []int
	sel.IterSet(func(row int) { out = append(out, row) })
	return out
}

func TestPredicate_TaggedUnion_AllVariantsImplementSealed(t *testing.T) {
	preds := []Predicate{
		EqInt64{Column: "x", Value: 1},
		LtInt64{Column: "x", Value: 1},
		GtInt64{Column: "x", Value: 1},
		EqBytes{Column: "y", Value: []byte("a")},
		IsNull{Column: "x"},
		And{Children: []Predicate{EqInt64{Column: "x", Value: 1}}},
		Or{Children: []Predicate{EqInt64{Column: "x", Value: 1}}},
		Not{Child: EqInt64{Column: "x", Value: 1}},
	}
	for _, p := range preds {
		p.isPredicate()
	}
}

func TestBindPredicate_RejectsUnknownColumn(t *testing.T) {
	schema := []types.Column{{Name: "id", Type: types.Int64}}
	if _, err := BindPredicate(EqInt64{Column: "missing", Value: 1}, ColumnsSchema(schema)); err == nil {
		t.Fatal("Bind must reject unknown column")
	}
}

func TestBindPredicate_RejectsKindMismatch(t *testing.T) {
	schema := []types.Column{{Name: "id", Type: types.Int32}}
	if _, err := BindPredicate(EqInt64{Column: "id", Value: 1}, ColumnsSchema(schema)); err == nil {
		t.Fatal("Bind must reject Int32 column for EqInt64 predicate")
	}
}

func TestBoundPredicate_EqInt64_Eval(t *testing.T) {
	batch := buildIntBatch(t, "id", []int64{1, 2, 2, 3, 2, 5})
	bp, err := BindPredicate(EqInt64{Column: "id", Value: 2}, BatchSchema(batch))
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	sel := types.NewSelectionMask(batch.Len)
	bp.Eval(batch, &sel)
	got := selectedRows(&sel)
	want := []int{1, 2, 4}
	if !equalInts(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestBoundPredicate_LtInt64_Eval(t *testing.T) {
	batch := buildIntBatch(t, "id", []int64{1, 5, 3, 7, 2})
	bp, _ := BindPredicate(LtInt64{Column: "id", Value: 4}, BatchSchema(batch))
	sel := types.NewSelectionMask(batch.Len)
	bp.Eval(batch, &sel)
	got := selectedRows(&sel)
	want := []int{0, 2, 4}
	if !equalInts(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestBoundPredicate_GtInt64_Eval(t *testing.T) {
	batch := buildIntBatch(t, "id", []int64{1, 5, 3, 7, 2})
	bp, _ := BindPredicate(GtInt64{Column: "id", Value: 4}, BatchSchema(batch))
	sel := types.NewSelectionMask(batch.Len)
	bp.Eval(batch, &sel)
	got := selectedRows(&sel)
	want := []int{1, 3}
	if !equalInts(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestBoundPredicate_EqBytes_Eval(t *testing.T) {
	batch := buildBytesBatch(t, "tag", []string{"alpha", "beta", "alpha", "gamma"})
	bp, _ := BindPredicate(EqBytes{Column: "tag", Value: []byte("alpha")}, BatchSchema(batch))
	sel := types.NewSelectionMask(batch.Len)
	bp.Eval(batch, &sel)
	got := selectedRows(&sel)
	want := []int{0, 2}
	if !equalInts(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestBoundPredicate_And_Eval(t *testing.T) {
	batch := buildIntBatch(t, "id", []int64{1, 2, 3, 4, 5})
	bp, _ := BindPredicate(And{Children: []Predicate{
		GtInt64{Column: "id", Value: 1},
		LtInt64{Column: "id", Value: 5},
	}}, BatchSchema(batch))
	sel := types.NewSelectionMask(batch.Len)
	bp.Eval(batch, &sel)
	got := selectedRows(&sel)
	want := []int{1, 2, 3}
	if !equalInts(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestBoundPredicate_Or_Eval(t *testing.T) {
	batch := buildIntBatch(t, "id", []int64{1, 2, 3, 4, 5})
	bp, _ := BindPredicate(Or{Children: []Predicate{
		EqInt64{Column: "id", Value: 1},
		EqInt64{Column: "id", Value: 4},
	}}, BatchSchema(batch))
	sel := types.NewSelectionMask(batch.Len)
	bp.Eval(batch, &sel)
	got := selectedRows(&sel)
	want := []int{0, 3}
	if !equalInts(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestBoundPredicate_Not_Eval(t *testing.T) {
	batch := buildIntBatch(t, "id", []int64{1, 2, 3, 4, 5})
	bp, _ := BindPredicate(Not{Child: EqInt64{Column: "id", Value: 3}}, BatchSchema(batch))
	sel := types.NewSelectionMask(batch.Len)
	bp.Eval(batch, &sel)
	got := selectedRows(&sel)
	want := []int{0, 1, 3, 4}
	if !equalInts(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestBoundPredicate_EmptyBatch_NoPanic(t *testing.T) {
	v := types.NewVec(types.VecInt64, 0)
	batch, err := types.NewBatch([]types.Column{{Name: "id", Type: types.Int64, V: v}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	bp, _ := BindPredicate(EqInt64{Column: "id", Value: 1}, BatchSchema(batch))
	sel := types.NewSelectionMask(0)
	bp.Eval(batch, &sel)
	if sel.PopCount() != 0 {
		t.Fatal("empty batch must yield 0 matches")
	}
}

func TestPruneSegment_EqInt64_OutsideRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if _, err := WriteSegment(path, []types.Batch{makeIntBatch(t, "id", 10, 100)}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()
	bp, _ := BindPredicate(EqInt64{Column: "id", Value: 99999}, SegmentSchema(seg))
	if !bp.PruneSegment(seg) {
		t.Fatal("PruneSegment must skip when value is outside [Min, Max]")
	}
	bp2, _ := BindPredicate(EqInt64{Column: "id", Value: 50}, SegmentSchema(seg))
	if bp2.PruneSegment(seg) {
		t.Fatal("PruneSegment must not skip when value falls inside [Min, Max]")
	}
}

func TestPruneSegment_LtAndGt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if _, err := WriteSegment(path, []types.Batch{makeIntBatch(t, "id", 100, 100)}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, _ := OpenSegment(path)
	defer seg.Close()
	schema := []types.Column{{Name: "id", Type: types.Int64}}

	lt, _ := BindPredicate(LtInt64{Column: "id", Value: 100}, ColumnsSchema(schema))
	if !lt.PruneSegment(seg) {
		t.Fatal("Lt should prune when Min >= value")
	}
	gt, _ := BindPredicate(GtInt64{Column: "id", Value: 1000}, ColumnsSchema(schema))
	if !gt.PruneSegment(seg) {
		t.Fatal("Gt should prune when Max <= value")
	}
}

func TestPruneSegment_And_SkipsIfAnyChildSkips(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if _, err := WriteSegment(path, []types.Batch{makeIntBatch(t, "id", 0, 50)}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, _ := OpenSegment(path)
	defer seg.Close()
	schema := []types.Column{{Name: "id", Type: types.Int64}}

	bp, _ := BindPredicate(And{Children: []Predicate{
		EqInt64{Column: "id", Value: 10},
		EqInt64{Column: "id", Value: 99999},
	}}, ColumnsSchema(schema))
	if !bp.PruneSegment(seg) {
		t.Fatal("And must prune if any child prunes")
	}
}

func TestPruneSegment_Or_SkipsOnlyIfAllChildrenSkip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if _, err := WriteSegment(path, []types.Batch{makeIntBatch(t, "id", 0, 50)}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, _ := OpenSegment(path)
	defer seg.Close()
	schema := []types.Column{{Name: "id", Type: types.Int64}}

	skip, _ := BindPredicate(Or{Children: []Predicate{
		EqInt64{Column: "id", Value: 99999},
		EqInt64{Column: "id", Value: 88888},
	}}, ColumnsSchema(schema))
	if !skip.PruneSegment(seg) {
		t.Fatal("Or must prune when all children prune")
	}
	noSkip, _ := BindPredicate(Or{Children: []Predicate{
		EqInt64{Column: "id", Value: 10},
		EqInt64{Column: "id", Value: 99999},
	}}, ColumnsSchema(schema))
	if noSkip.PruneSegment(seg) {
		t.Fatal("Or must not prune when any child does not prune")
	}
}

func TestPruneSegment_NoFalseNegatives_Random(t *testing.T) {
	rows := 1000
	vals := make([]int64, rows)
	for i := range vals {
		vals[i] = int64(i)*7 + 100
	}
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	v := types.NewVec(types.VecInt64, rows)
	copy(v.I64(), vals)
	batch, _ := types.NewBatch([]types.Column{{Name: "id", Type: types.Int64, V: v}})
	if _, err := WriteSegment(path, []types.Batch{batch}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, _ := OpenSegment(path)
	defer seg.Close()
	schema := []types.Column{{Name: "id", Type: types.Int64}}
	for _, val := range vals {
		bp, _ := BindPredicate(EqInt64{Column: "id", Value: val}, ColumnsSchema(schema))
		if bp.PruneSegment(seg) {
			t.Fatalf("PruneSegment(EqInt64=%d) pruned a value present in segment", val)
		}
	}
}

func TestPrunePage_NoFalseNegatives(t *testing.T) {
	// Multi-page segment with disjoint page bounds so prune decisions actually differ
	// across pages. For each value present anywhere in the segment, no page that
	// contains it may be pruned: PrunePage must be sound (false negatives only).
	const pageRows = 100
	pages := []types.Batch{
		makeIntBatch(t, "id", 0, pageRows),       // [0, 100)
		makeIntBatch(t, "id", 200, pageRows),     // [200, 300)
		makeIntBatch(t, "id", 400, pageRows),     // [400, 500)
	}
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if _, err := WriteSegment(path, pages, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()

	schema := []types.Column{{Name: "id", Type: types.Int64}}
	pageContains := func(pi int, v int64) bool {
		base := int64(0)
		switch pi {
		case 0:
			base = 0
		case 1:
			base = 200
		case 2:
			base = 400
		}
		return v >= base && v < base+int64(pageRows)
	}

	check := func(p Predicate, v int64) {
		t.Helper()
		bp, err := BindPredicate(p, ColumnsSchema(schema))
		if err != nil {
			t.Fatalf("Bind(%T value=%d): %v", p, v, err)
		}
		for pi := range len(seg.Cols[0].Pages) {
			if bp.PrunePage(seg, pi) && pageContains(pi, v) {
				t.Fatalf("PrunePage(%T value=%d, page %d) pruned a page that contains the value", p, v, pi)
			}
		}
	}

	for _, v := range []int64{0, 50, 99, 200, 250, 299, 400, 499} {
		check(EqInt64{Column: "id", Value: v}, v)
	}
	// Lt/Gt: every page that has any row satisfying the predicate must NOT be pruned.
	for pi, bound := range []struct{ lo, hi int64 }{{0, 99}, {200, 299}, {400, 499}} {
		bp, _ := BindPredicate(LtInt64{Column: "id", Value: bound.hi}, ColumnsSchema(schema))
		if bp.PrunePage(seg, pi) {
			t.Fatalf("LtInt64(value=%d) pruned page %d that has rows < value", bound.hi, pi)
		}
		bp, _ = BindPredicate(GtInt64{Column: "id", Value: bound.lo}, ColumnsSchema(schema))
		if bp.PrunePage(seg, pi) {
			t.Fatalf("GtInt64(value=%d) pruned page %d that has rows > value", bound.lo, pi)
		}
	}
}

func TestPruneSegment_NotConstant(t *testing.T) {
	v := types.NewVec(types.VecInt64, 50)
	for i := range v.I64() {
		v.I64()[i] = 7
	}
	batch, _ := types.NewBatch([]types.Column{{Name: "id", Type: types.Int64, V: v}})
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if _, err := WriteSegment(path, []types.Batch{batch}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, _ := OpenSegment(path)
	defer seg.Close()
	schema := []types.Column{{Name: "id", Type: types.Int64}}

	prune, _ := BindPredicate(Not{Child: EqInt64{Column: "id", Value: 7}}, ColumnsSchema(schema))
	if !prune.PruneSegment(seg) {
		t.Fatal("NOT(id = 7) on constant-7 segment must prune")
	}
	noPrune, _ := BindPredicate(Not{Child: EqInt64{Column: "id", Value: 8}}, ColumnsSchema(schema))
	if noPrune.PruneSegment(seg) {
		t.Fatal("NOT(id = 8) on constant-7 segment must not prune")
	}
	ltPrune, _ := BindPredicate(Not{Child: LtInt64{Column: "id", Value: 10}}, ColumnsSchema(schema))
	if !ltPrune.PruneSegment(seg) {
		t.Fatal("NOT(id < 10) on all-7 segment must prune")
	}
}

func TestPruneSegment_EqBytes_DictHistogram(t *testing.T) {
	batch := buildBytesBatch(t, "cat", []string{"a", "b", "c", "a", "b"})
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if _, err := WriteSegment(path, []types.Batch{batch}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()
	schema := []types.Column{{Name: "cat", Type: types.Text}}

	miss, _ := BindPredicate(EqBytes{Column: "cat", Value: []byte("zzz")}, ColumnsSchema(schema))
	if !miss.PruneSegment(seg) {
		t.Fatal("PruneSegment must skip when literal absent from dict histogram")
	}
	hit, _ := BindPredicate(EqBytes{Column: "cat", Value: []byte("a")}, ColumnsSchema(schema))
	if hit.PruneSegment(seg) {
		t.Fatal("PruneSegment must not skip when literal present in dict histogram")
	}
}

func equalInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestBoundPredicate_SkipsNullRows(t *testing.T) {
	v := types.NewVec(types.VecInt64, 5)
	v.I64()[0], v.I64()[1], v.I64()[2], v.I64()[3], v.I64()[4] = 1, 2, 3, 2, 5
	v.Valid = types.NewValidity(5)
	v.Valid.SetInvalid(1)
	v.Valid.SetInvalid(3)
	batch, err := types.NewBatch([]types.Column{{Name: "id", Type: types.Int64, V: v}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	bp, err := BindPredicate(EqInt64{Column: "id", Value: 2}, BatchSchema(batch))
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	sel := types.NewSelectionMask(batch.Len)
	bp.Eval(batch, &sel)
	if sel.PopCount() != 0 {
		t.Fatalf("Eq must skip invalid rows; got %v", selectedRows(&sel))
	}
}

func TestBoundPredicate_ReusedMaskResizesToBatch(t *testing.T) {
	bp, err := BindPredicate(EqInt64{Column: "id", Value: 2}, ColumnsSchema([]types.Column{{Name: "id", Type: types.Int64}}))
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	sel := types.NewSelectionMask(100)
	small := buildIntBatch(t, "id", []int64{1, 2, 2})
	bp.Eval(small, &sel)
	if sel.Rows() != 3 {
		t.Fatalf("sel.Rows() = %d, want resized to 3", sel.Rows())
	}
	got := selectedRows(&sel)
	if !equalInts(got, []int{1, 2}) {
		t.Fatalf("got %v want [1 2]", got)
	}

	large := buildIntBatch(t, "id", []int64{2, 1, 2, 1, 2, 1, 2})
	bp.Eval(large, &sel)
	if sel.Rows() != 7 {
		t.Fatalf("sel.Rows() = %d, want resized to 7", sel.Rows())
	}
	got = selectedRows(&sel)
	if !equalInts(got, []int{0, 2, 4, 6}) {
		t.Fatalf("got %v want [0 2 4 6]", got)
	}
}

func TestBoundPredicate_NotReusedMaskCorrectRowCount(t *testing.T) {
	bp, _ := BindPredicate(Not{Child: EqInt64{Column: "id", Value: 2}}, ColumnsSchema([]types.Column{{Name: "id", Type: types.Int64}}))
	sel := types.NewSelectionMask(100)
	batch := buildIntBatch(t, "id", []int64{1, 2, 3})
	bp.Eval(batch, &sel)
	if sel.Rows() != 3 {
		t.Fatalf("Not eval must size sel to batch.Len; got %d", sel.Rows())
	}
	got := selectedRows(&sel)
	if !equalInts(got, []int{0, 2}) {
		t.Fatalf("Not Eq=2 over [1 2 3]: got %v want [0 2]", got)
	}
}

func TestBindPredicate_CaseInsensitive(t *testing.T) {
	schema := []types.Column{{Name: "id", Type: types.Int64}}
	if _, err := BindPredicate(EqInt64{Column: "ID", Value: 1}, ColumnsSchema(schema)); err != nil {
		t.Fatalf("uppercase column should bind case-insensitively: %v", err)
	}
	if _, err := BindPredicate(EqInt64{Column: "Id", Value: 1}, ColumnsSchema(schema)); err != nil {
		t.Fatalf("mixed-case column should bind case-insensitively: %v", err)
	}
}

func TestBindPredicate_EqBytesClonesValue(t *testing.T) {
	value := []byte("alpha")
	batch := buildBytesBatch(t, "tag", []string{"alpha", "beta", "alpha"})
	bp, err := BindPredicate(EqBytes{Column: "tag", Value: value}, BatchSchema(batch))
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	for i := range value {
		value[i] = 'X'
	}
	sel := types.NewSelectionMask(batch.Len)
	bp.Eval(batch, &sel)
	got := selectedRows(&sel)
	if !equalInts(got, []int{0, 2}) {
		t.Fatalf("EqBytes must clone Value at bind; got %v want [0 2]", got)
	}
}
