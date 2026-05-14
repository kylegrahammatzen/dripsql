package storage

import (
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/storage/codec"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestPlainInt64EqEvaluator(t *testing.T) {
	batch := predicateBatch(t)
	var sel types.SelectionMask
	if _, err := NewPredicateEvaluator(Predicate{Column: "tenant_id", Op: PredicateOpEq, Int64: 42}).Eval(batch, &sel); err != nil {
		t.Fatalf("Eval: %v", err)
	}
	assertMaskRows(t, sel, []int{0, 2})
}

func TestPlainInt64BetweenEvaluatorWithNull(t *testing.T) {
	batch := predicateBatch(t)
	var sel types.SelectionMask
	if _, err := NewPredicateEvaluator(Predicate{Column: "tenant_id", Op: PredicateOpBetween, Lo: 40, Hi: 50}).Eval(batch, &sel); err != nil {
		t.Fatalf("Eval: %v", err)
	}
	assertMaskRows(t, sel, []int{0, 2})
}

func TestFORBitPackInt64EqEvaluator(t *testing.T) {
	encoded := forBitPackPredicateVec(t)
	batch, err := types.NewBatch([]types.Column{{Name: "tenant_id", Type: types.Int64, V: encoded}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	var sel types.SelectionMask
	if _, err := NewPredicateEvaluator(Predicate{Column: "tenant_id", Op: PredicateOpEq, Int64: 42}).Eval(batch, &sel); err != nil {
		t.Fatalf("Eval: %v", err)
	}
	assertMaskRows(t, sel, []int{0, 2})
}

func TestFORBitPackInt64SelectedEvaluator(t *testing.T) {
	encoded := forBitPackPredicateVec(t)
	batch, err := types.NewBatch([]types.Column{{Name: "tenant_id", Type: types.Int64, V: encoded}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	input := types.NewSelectionMask(batch.Len)
	input.Set(1)
	input.Set(2)
	input.Set(3)
	var sel types.SelectionMask
	matched, err := NewPredicateEvaluator(Predicate{Column: "tenant_id", Op: PredicateOpEq, Int64: 42}).EvalSelected(batch, input, &sel)
	if err != nil {
		t.Fatalf("EvalSelected: %v", err)
	}
	if matched != 1 {
		t.Fatalf("matched = %d, want 1", matched)
	}
	assertMaskRows(t, sel, []int{2})
}

func TestFORBitPackInt64EqWithNulls(t *testing.T) {
	valid := types.NewValidity(4)
	types.SetInvalid(valid, 2)
	page, err := (codec.FORBitPack{}).Encode(types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: 4, I64: []int64{42, 7, 42, 8}, Valid: valid})
	if err != nil {
		t.Fatalf("FOR Encode: %v", err)
	}
	vec, err := (codec.FORBitPack{}).DecodeEncoded(page)
	if err != nil {
		t.Fatalf("DecodeEncoded: %v", err)
	}
	batch, err := types.NewBatch([]types.Column{{Name: "tenant_id", Type: types.Int64, V: vec}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	var sel types.SelectionMask
	if _, err := NewPredicateEvaluator(Predicate{Column: "tenant_id", Op: PredicateOpEq, Int64: 42}).Eval(batch, &sel); err != nil {
		t.Fatalf("Eval: %v", err)
	}
	assertMaskRows(t, sel, []int{0})
}

func TestFORBitPackInt64BetweenAndInFastPaths(t *testing.T) {
	encoded := forBitPackPredicateVec(t)
	batch, err := types.NewBatch([]types.Column{{Name: "tenant_id", Type: types.Int64, V: encoded}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	cases := []struct {
		name string
		pred Predicate
		want []int
	}{
		{"between_inclusive", Predicate{Column: "tenant_id", Op: PredicateOpBetween, Lo: 7, Hi: 42}, []int{0, 1, 2, 3}},
		{"between_narrow", Predicate{Column: "tenant_id", Op: PredicateOpBetween, Lo: 8, Hi: 42}, []int{0, 2, 3}},
		{"compare_lt", Predicate{Column: "tenant_id", Op: PredicateOpLess, Int64: 9}, []int{1, 3}},
		{"in_set", Predicate{Column: "tenant_id", Op: PredicateOpIn, Int64s: []int64{7, 42}}, []int{0, 1, 2}},
		{"not_in_set", Predicate{Column: "tenant_id", Op: PredicateOpNotIn, Int64s: []int64{7, 42}}, []int{3}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var sel types.SelectionMask
			if _, err := NewPredicateEvaluator(tc.pred).Eval(batch, &sel); err != nil {
				t.Fatalf("Eval: %v", err)
			}
			assertMaskRows(t, sel, tc.want)
		})
	}
}

func TestFORBitPackInt64EqMissingRHS(t *testing.T) {
	encoded := forBitPackPredicateVec(t)
	batch, err := types.NewBatch([]types.Column{{Name: "tenant_id", Type: types.Int64, V: encoded}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	var sel types.SelectionMask
	if _, err := NewPredicateEvaluator(Predicate{Column: "tenant_id", Op: PredicateOpEq, Int64: 999}).Eval(batch, &sel); err != nil {
		t.Fatalf("Eval: %v", err)
	}
	assertMaskRows(t, sel, nil)
}

func TestPlainTextEqEvaluator(t *testing.T) {
	batch := predicateBatch(t)
	var sel types.SelectionMask
	if _, err := NewPredicateEvaluator(Predicate{Column: "event_type", Op: PredicateOpEq, Text: "checkout"}).Eval(batch, &sel); err != nil {
		t.Fatalf("Eval: %v", err)
	}
	assertMaskRows(t, sel, []int{0, 1, 3})
}

func TestPredicateEvaluatorReusesSelectionMask(t *testing.T) {
	batch := predicateBatch(t)
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	words := &sel.Words[0]
	if _, err := NewPredicateEvaluator(Predicate{Column: "event_type", Op: PredicateOpEq, Text: "login"}).Eval(batch, &sel); err != nil {
		t.Fatalf("Eval: %v", err)
	}
	if &sel.Words[0] != words {
		t.Fatalf("selection words were not reused")
	}
	assertMaskRows(t, sel, []int{2})
}

func TestDictTextEqEvaluator(t *testing.T) {
	batch := dictPredicateBatch(t)
	var sel types.SelectionMask
	if _, err := NewPredicateEvaluator(Predicate{Column: "event_type", Op: PredicateOpEq, Text: "checkout"}).Eval(batch, &sel); err != nil {
		t.Fatalf("Eval: %v", err)
	}
	assertMaskRows(t, sel, []int{0, 3})
}

func TestDictTextEqMissingRHS(t *testing.T) {
	batch := dictPredicateBatch(t)
	var sel types.SelectionMask
	if _, err := NewPredicateEvaluator(Predicate{Column: "event_type", Op: PredicateOpEq, Text: "purchase"}).Eval(batch, &sel); err != nil {
		t.Fatalf("Eval: %v", err)
	}
	assertMaskRows(t, sel, nil)
}

func TestDictTextNotEqEvaluator(t *testing.T) {
	batch := dictPredicateBatch(t)
	var sel types.SelectionMask
	if _, err := NewPredicateEvaluator(Predicate{Column: "event_type", Op: PredicateOpNotEq, Text: "checkout"}).Eval(batch, &sel); err != nil {
		t.Fatalf("Eval: %v", err)
	}
	assertMaskRows(t, sel, []int{2})
}

func TestDictTextNotEqMissingRHS(t *testing.T) {
	batch := dictPredicateBatch(t)
	var sel types.SelectionMask
	if _, err := NewPredicateEvaluator(Predicate{Column: "event_type", Op: PredicateOpNotEq, Text: "purchase"}).Eval(batch, &sel); err != nil {
		t.Fatalf("Eval: %v", err)
	}
	assertMaskRows(t, sel, []int{0, 2, 3})
}

func TestDictTextInEvaluator(t *testing.T) {
	batch := dictPredicateBatch(t)
	var sel types.SelectionMask
	if _, err := NewPredicateEvaluator(Predicate{Column: "event_type", Op: PredicateOpIn, Texts: []string{"login", "checkout"}}).Eval(batch, &sel); err != nil {
		t.Fatalf("Eval: %v", err)
	}
	assertMaskRows(t, sel, []int{0, 2, 3})
}

func TestDictTextNotInEvaluator(t *testing.T) {
	batch := dictPredicateBatch(t)
	var sel types.SelectionMask
	if _, err := NewPredicateEvaluator(Predicate{Column: "event_type", Op: PredicateOpNotIn, Texts: []string{"checkout"}}).Eval(batch, &sel); err != nil {
		t.Fatalf("Eval: %v", err)
	}
	assertMaskRows(t, sel, []int{2})
}

func TestDictTextSelectedEvaluator(t *testing.T) {
	batch := dictPredicateBatch(t)
	input := types.NewSelectionMask(batch.Len)
	input.Set(2)
	input.Set(3)
	var sel types.SelectionMask
	matched, err := NewPredicateEvaluator(Predicate{Column: "event_type", Op: PredicateOpEq, Text: "checkout"}).EvalSelected(batch, input, &sel)
	if err != nil {
		t.Fatalf("EvalSelected: %v", err)
	}
	if matched != 1 {
		t.Fatalf("matched = %d, want 1", matched)
	}
	assertMaskRows(t, sel, []int{3})
}

func TestDictTextSelectedNotEqMissingRHS(t *testing.T) {
	batch := dictPredicateBatch(t)
	input := types.NewSelectionMask(batch.Len)
	input.Set(1)
	input.Set(2)
	input.Set(3)
	var sel types.SelectionMask
	matched, err := NewPredicateEvaluator(Predicate{Column: "event_type", Op: PredicateOpNotEq, Text: "purchase"}).EvalSelected(batch, input, &sel)
	if err != nil {
		t.Fatalf("EvalSelected: %v", err)
	}
	if matched != 2 {
		t.Fatalf("matched = %d, want 2", matched)
	}
	assertMaskRows(t, sel, []int{2, 3})
}

func TestDictTextInWithDuplicateRHS(t *testing.T) {
	batch := dictPredicateBatch(t)
	var sel types.SelectionMask
	if _, err := NewPredicateEvaluator(Predicate{Column: "event_type", Op: PredicateOpIn, Texts: []string{"checkout", "checkout", "missing"}}).Eval(batch, &sel); err != nil {
		t.Fatalf("Eval: %v", err)
	}
	assertMaskRows(t, sel, []int{0, 3})
}

func TestDictTextSingleValuePage(t *testing.T) {
	batch := dictSingleValueBatch(t)
	var sel types.SelectionMask
	if _, err := NewPredicateEvaluator(Predicate{Column: "event_type", Op: PredicateOpEq, Text: "checkout"}).Eval(batch, &sel); err != nil {
		t.Fatalf("Eval: %v", err)
	}
	assertMaskRows(t, sel, []int{0, 1, 2, 3})
}

func TestDictTextAllNullPage(t *testing.T) {
	batch := dictAllNullBatch(t)
	var sel types.SelectionMask
	if _, err := NewPredicateEvaluator(Predicate{Column: "event_type", Op: PredicateOpNotEq, Text: "purchase"}).Eval(batch, &sel); err != nil {
		t.Fatalf("Eval: %v", err)
	}
	assertMaskRows(t, sel, nil)
}

func TestDictTextRejectsShortIDs(t *testing.T) {
	dict := types.NewVarBytes(1, 0)
	dict.AppendString(0, "checkout")
	batch, err := types.NewBatch([]types.Column{{Name: "event_type", Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingDictionary, Len: 4, Encoded: &types.EncodedState{DictIDs: []uint8{0}, DictValues: dict}}}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	var sel types.SelectionMask
	if _, err := NewPredicateEvaluator(Predicate{Column: "event_type", Op: PredicateOpEq, Text: "checkout"}).Eval(batch, &sel); err == nil {
		t.Fatal("expected short dictionary IDs error")
	}
}

func TestDictTextPersistedSegmentScan(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	meta, err := WriteSegment(path, 30, []types.Batch{segmentBatch(t, []int64{1, 2, 3, 4}, []string{"checkout", "login", "checkout", "checkout"})})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	if meta.Columns[1].Pages[0].Encoding != types.EncodingDictionary {
		t.Fatalf("encoding = %s, want dictionary", meta.Columns[1].Pages[0].Encoding)
	}
	pred := Predicate{Column: "event_type", Op: PredicateOpEq, Text: "checkout"}
	it := SegmentScanIterator{Segments: []ScanSegment{{Path: path, Meta: meta}}, Predicate: NewPredicateEvaluator(pred), Prune: pred}
	visits := 0
	if err := it.ForEach(func(_ types.Batch, sel types.SelectionMask) error {
		visits++
		assertMaskRows(t, sel, []int{0, 2, 3})
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visits != 1 {
		t.Fatalf("visits = %d, want 1", visits)
	}
}

func TestConjunctionEvaluator(t *testing.T) {
	batch := predicateBatch(t)
	var sel types.SelectionMask
	pred := Predicate{Op: PredicateAnd, Children: []Predicate{
		{Column: "tenant_id", Op: PredicateOpEq, Int64: 42},
		{Column: "event_type", Op: PredicateOpEq, Text: "checkout"},
	}}
	if _, err := NewPredicateEvaluator(pred).Eval(batch, &sel); err != nil {
		t.Fatalf("Eval: %v", err)
	}
	assertMaskRows(t, sel, []int{0})
}

func TestDisjunctionEvaluator(t *testing.T) {
	batch := predicateBatch(t)
	var sel types.SelectionMask
	pred := Predicate{Op: PredicateOr, Children: []Predicate{
		{Column: "tenant_id", Op: PredicateOpEq, Int64: 7},
		{Column: "event_type", Op: PredicateOpEq, Text: "login"},
	}}
	if _, err := NewPredicateEvaluator(pred).Eval(batch, &sel); err != nil {
		t.Fatalf("Eval: %v", err)
	}
	assertMaskRows(t, sel, []int{2, 3})
}

func TestPredicateEvaluatorSelectedRows(t *testing.T) {
	batch := predicateBatch(t)
	input := types.NewSelectionMask(batch.Len)
	input.Set(2)
	input.Set(3)
	var sel types.SelectionMask
	matched, err := NewPredicateEvaluator(Predicate{Column: "event_type", Op: PredicateOpEq, Text: "checkout"}).EvalSelected(batch, input, &sel)
	if err != nil {
		t.Fatalf("EvalSelected: %v", err)
	}
	if matched != 1 {
		t.Fatalf("matched = %d, want 1", matched)
	}
	assertMaskRows(t, sel, []int{3})
}

func TestPredicateEvaluatorSelectedCompound(t *testing.T) {
	batch := predicateBatch(t)
	input := types.NewSelectionMask(batch.Len)
	input.Set(0)
	input.Set(2)
	input.Set(3)
	var sel types.SelectionMask
	pred := Predicate{Op: PredicateOr, Children: []Predicate{
		{Column: "tenant_id", Op: PredicateOpEq, Int64: 7},
		{Column: "event_type", Op: PredicateOpEq, Text: "login"},
	}}
	matched, err := NewPredicateEvaluator(pred).EvalSelected(batch, input, &sel)
	if err != nil {
		t.Fatalf("EvalSelected: %v", err)
	}
	if matched != 2 {
		t.Fatalf("matched = %d, want 2", matched)
	}
	assertMaskRows(t, sel, []int{2, 3})
}

func predicateBatch(t *testing.T) types.Batch {
	t.Helper()
	valid := types.NewValidity(4)
	types.SetInvalid(valid, 1)
	varText := types.NewVarBytes(4, 0)
	for i, value := range []string{"checkout", "checkout", "login", "checkout"} {
		varText.AppendString(i, value)
	}
	batch, err := types.NewBatch([]types.Column{
		{Name: "tenant_id", Type: types.Int64, V: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: 4, Valid: valid, I64: []int64{42, 42, 42, 7}}},
		{Name: "event_type", Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: 4, Var: varText}},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func dictPredicateBatch(t *testing.T) types.Batch {
	t.Helper()
	valid := types.NewValidity(4)
	types.SetInvalid(valid, 1)
	dict := types.NewVarBytes(2, 0)
	dict.AppendString(0, "checkout")
	dict.AppendString(1, "login")
	batch, err := types.NewBatch([]types.Column{
		{Name: "event_type", Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingDictionary, Len: 4, Valid: valid, Encoded: &types.EncodedState{DictIDs: []uint8{0, 0, 1, 0}, DictValues: dict}}},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func dictSingleValueBatch(t *testing.T) types.Batch {
	t.Helper()
	dict := types.NewVarBytes(1, 0)
	dict.AppendString(0, "checkout")
	batch, err := types.NewBatch([]types.Column{
		{Name: "event_type", Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingDictionary, Len: 4, Encoded: &types.EncodedState{DictIDs: []uint8{0, 0, 0, 0}, DictValues: dict}}},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func dictAllNullBatch(t *testing.T) types.Batch {
	t.Helper()
	valid := types.NewValidity(4)
	for row := range 4 {
		types.SetInvalid(valid, row)
	}
	dict := types.NewVarBytes(1, 0)
	dict.AppendString(0, "checkout")
	batch, err := types.NewBatch([]types.Column{
		{Name: "event_type", Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingDictionary, Len: 4, Valid: valid, Encoded: &types.EncodedState{DictIDs: []uint8{0, 0, 0, 0}, DictValues: dict}}},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func forBitPackPredicateVec(t *testing.T) types.Vec {
	t.Helper()
	page, err := (codec.FORBitPack{}).Encode(types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: 4, I64: []int64{42, 7, 42, 8}})
	if err != nil {
		t.Fatalf("FOR Encode: %v", err)
	}
	vec, err := (codec.FORBitPack{}).DecodeEncoded(page)
	if err != nil {
		t.Fatalf("DecodeEncoded: %v", err)
	}
	return vec
}

func assertMaskRows(t *testing.T, got types.SelectionMask, want []int) {
	t.Helper()
	rows := make([]int, 0, got.PopCount())
	got.IterSet(func(row int) { rows = append(rows, row) })
	if len(rows) != len(want) {
		t.Fatalf("rows = %v, want %v", rows, want)
	}
	for i := range want {
		if rows[i] != want[i] {
			t.Fatalf("rows = %v, want %v", rows, want)
		}
	}
}
