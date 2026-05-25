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
