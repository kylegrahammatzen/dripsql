package exec

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func buildBenchBatch(rows int) (types.Batch, *types.SelectionMask) {
	v := types.NewVec(types.VecInt64, rows)
	s := v.I64()
	for i := range s {
		s[i] = int64(i)
	}
	col := types.Column{Name: "x", Type: types.Int64, V: v}
	batch, _ := types.NewBatch([]types.Column{col})
	sel := types.NewSelectionMask(rows)
	sel.FillAll()
	return batch, &sel
}

func cmpPred(name string, v int64, op sql.ExprOp) sql.BoundExpr {
	col := sql.BoundExpr{Op: sql.ExprColumn, Type: types.Int64, Column: name}
	lit := sql.BoundExpr{Op: sql.ExprLiteral, Type: types.Int64, Literal: v}
	return sql.BoundExpr{Op: op, Type: types.Bool, Args: []sql.BoundExpr{col, lit}}
}

func BenchmarkFilter_Int64Less(b *testing.B) {
	batch, sel := buildBenchBatch(types.StandardBatchRows)
	pred := cmpPred("x", int64(types.StandardBatchRows/2), sql.ExprLess)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		res, err := filterPredicate(batch, *sel, pred)
		if err != nil || res.count == 0 {
			b.Fatal("fast-path missed or errored")
		}
	}
}

func BenchmarkFilter_Int64Between(b *testing.B) {
	batch, sel := buildBenchBatch(types.StandardBatchRows)
	col := sql.BoundExpr{Op: sql.ExprColumn, Type: types.Int64, Column: "x"}
	lo := sql.BoundExpr{Op: sql.ExprLiteral, Type: types.Int64, Literal: int64(500)}
	hi := sql.BoundExpr{Op: sql.ExprLiteral, Type: types.Int64, Literal: int64(1500)}
	pred := sql.BoundExpr{Op: sql.ExprBetween, Type: types.Bool, Args: []sql.BoundExpr{col, lo, hi}}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		res, err := filterPredicate(batch, *sel, pred)
		if err != nil || res.count == 0 {
			b.Fatal("between fast-path missed")
		}
	}
}

func BenchmarkFilter_Int64AndCompound(b *testing.B) {
	batch, sel := buildBenchBatch(types.StandardBatchRows)
	left := cmpPred("x", int64(1500), sql.ExprLess)
	right := cmpPred("x", int64(500), sql.ExprGreater)
	pred := sql.BoundExpr{Op: sql.ExprAnd, Type: types.Bool, Args: []sql.BoundExpr{left, right}}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		res, err := filterPredicate(batch, *sel, pred)
		if err != nil || res.count == 0 {
			b.Fatal("AND fast-path missed")
		}
	}
}

func BenchmarkFilter_Int64Equal(b *testing.B) {
	batch, sel := buildBenchBatch(types.StandardBatchRows)
	pred := cmpPred("x", int64(1234), sql.ExprEqual)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		res, err := filterPredicate(batch, *sel, pred)
		if err != nil || res.count == 0 {
			b.Fatal("fast-path missed or errored")
		}
	}
}
