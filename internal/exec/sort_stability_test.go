// Verifies the typed int64 sort path and streaming top-K both use the cmpInt64Sort seq
// tie-break, so equal keys keep their input arrival order in the output.
package exec

import (
	"context"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestSort_Stable_EqualKeysPreserveInputOrder(t *testing.T) {
	const rows = 256
	const distinctKeys = 8
	k := vector.NewVec(vector.VecInt64, rows)
	tag := vector.NewVec(vector.VecInt64, rows)
	for i := range rows {
		k.I64()[i] = int64(i % distinctKeys)
		tag.I64()[i] = int64(i)
	}
	batch, err := vector.NewBatch([]vector.Column{
		{Name: "k", Type: schema.Int64, V: k},
		{Name: "tag", Type: schema.Int64, V: tag},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	sel := vector.NewSelectionMask(rows)
	sel.FillAll()
	batch.Sel = &sel

	for _, tc := range []struct {
		name string
		op   *SortOp
	}{
		{
			name: "full sort",
			op: &SortOp{
				Source: &bufferSource{batches: []vector.Batch{batch}},
				Keys:   []sql.SortKey{{Expr: sql.BoundExpr{Op: sql.ExprColumn, Type: schema.Int64, Column: "k"}}},
			},
		},
		{
			name: "streaming top-K",
			op: &SortOp{
				Source: &bufferSource{batches: []vector.Batch{batch}},
				Keys:   []sql.SortKey{{Expr: sql.BoundExpr{Op: sql.ExprColumn, Type: schema.Int64, Column: "k"}}},
				K:      int64(rows),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.op.Open(context.Background()); err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer tc.op.Close()
			out, ok, err := tc.op.Next()
			if err != nil || !ok {
				t.Fatalf("Next: ok=%v err=%v", ok, err)
			}
			outK := out.Columns[0].V.I64()
			outTag := out.Columns[1].V.I64()
			// Group output by k and assert tags within each group are strictly increasing.
			lastK := int64(-1)
			var prevTag int64
			for i := range out.Len {
				kv, tv := outK[i], outTag[i]
				if kv != lastK {
					lastK = kv
					prevTag = tv
					continue
				}
				if tv <= prevTag {
					t.Fatalf("unstable: at row %d, k=%d tag=%d after tag=%d", i, kv, tv, prevTag)
				}
				prevTag = tv
			}
		})
	}
}

func TestSort_TopKMatchesFullSortWithOffsetAndNulls(t *testing.T) {
	const rows = 64
	k := vector.NewVec(vector.VecInt64, rows)
	tag := vector.NewVec(vector.VecInt64, rows)
	valid := vector.NewAllValid(rows)
	for i := range rows {
		k.I64()[i] = int64((i*17 + 3) % 19)
		tag.I64()[i] = int64(i)
		if i%11 == 0 {
			valid.SetInvalid(i)
		}
	}
	k.Valid = valid
	batch, err := vector.NewBatch([]vector.Column{
		{Name: "k", Type: schema.Int64, V: k},
		{Name: "tag", Type: schema.Int64, V: tag},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	sel := vector.NewSelectionMask(rows)
	sel.FillAll()
	batch.Sel = &sel

	keys := []sql.SortKey{{Expr: sql.BoundExpr{Op: sql.ExprColumn, Type: schema.Int64, Column: "k"}, Desc: true}}
	full := &SortOp{Source: &bufferSource{batches: []vector.Batch{batch}}, Keys: keys}
	if err := full.Open(context.Background()); err != nil {
		t.Fatalf("full Open: %v", err)
	}
	fullOut, ok, err := full.Next()
	if err != nil || !ok {
		t.Fatalf("full Next: ok=%v err=%v", ok, err)
	}
	if err := full.Close(); err != nil {
		t.Fatalf("full Close: %v", err)
	}

	top := &SortOp{Source: &bufferSource{batches: []vector.Batch{batch}}, Keys: keys, K: 7, Offset: 5}
	if err := top.Open(context.Background()); err != nil {
		t.Fatalf("top Open: %v", err)
	}
	topOut, ok, err := top.Next()
	if err != nil || !ok {
		t.Fatalf("top Next: ok=%v err=%v", ok, err)
	}
	if err := top.Close(); err != nil {
		t.Fatalf("top Close: %v", err)
	}

	fullTags := fullOut.Columns[1].V.I64()
	topTags := topOut.Columns[1].V.I64()
	if topOut.Len != 7 {
		t.Fatalf("top len=%d want 7", topOut.Len)
	}
	for i := range topOut.Len {
		if topTags[i] != fullTags[i+5] {
			t.Fatalf("row %d tag=%d want %d", i, topTags[i], fullTags[i+5])
		}
	}
}

func TestSort_TextPrefixFallbackOrdersSharedPrefixes(t *testing.T) {
	values := []string{"aaab2", "aaab10", "aaab1", "aaaa", "aaab", "", "aaac", "aaab1"}
	wantValues := []string{"", "aaaa", "aaab", "aaab1", "aaab1", "aaab10", "aaab2", "aaac"}
	wantTags := []int64{5, 3, 4, 2, 7, 1, 0, 6}

	text := vector.NewVarVec(vector.VecText, len(values), 0)
	tags := vector.NewVec(vector.VecInt64, len(values))
	for i, value := range values {
		text.Var().AppendString(i, value)
		tags.I64()[i] = int64(i)
	}
	batch, err := vector.NewBatch([]vector.Column{
		{Name: "s", Type: schema.Text, V: text},
		{Name: "tag", Type: schema.Int64, V: tags},
	})
	if err != nil {
		t.Fatalf("NewBatch failed %v", err)
	}
	sel := vector.NewSelectionMask(len(values))
	sel.FillAll()
	batch.Sel = &sel

	op := &SortOp{
		Source: &bufferSource{batches: []vector.Batch{batch}},
		Keys:   []sql.SortKey{{Expr: sql.BoundExpr{Op: sql.ExprColumn, Type: schema.Text, Column: "s"}}},
	}
	if err := op.Open(context.Background()); err != nil {
		t.Fatalf("Open failed %v", err)
	}
	out, ok, err := op.Next()
	if err != nil || !ok {
		t.Fatalf("Next returned ok %v err %v", ok, err)
	}
	if err := op.Close(); err != nil {
		t.Fatalf("Close failed %v", err)
	}

	outText := out.Columns[0].V.Var()
	outTags := out.Columns[1].V.I64()
	for i, want := range wantValues {
		if got := outText.String(i); got != want {
			t.Fatalf("row %d value got %q want %q", i, got, want)
		}
		if got := outTags[i]; got != wantTags[i] {
			t.Fatalf("row %d tag got %d want %d", i, got, wantTags[i])
		}
	}
}
