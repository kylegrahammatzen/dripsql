// Contract, vectorized arithmetic must match the row-by-row eval bit for bit on selected rows.
package exec

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func exprCol(name string, t schema.Type) sql.BoundExpr {
	return sql.BoundExpr{Op: sql.ExprColumn, Type: t, Column: name}
}

func exprLit(v any, t schema.Type) sql.BoundExpr {
	return sql.BoundExpr{Op: sql.ExprLiteral, Type: t, Literal: v}
}

func exprBin(op sql.ExprOp, t schema.Type, l, r sql.BoundExpr) sql.BoundExpr {
	return sql.BoundExpr{Op: op, Type: t, Args: []sql.BoundExpr{l, r}}
}

func buildExprBatch(t *testing.T) vector.Batch {
	t.Helper()
	const rows = 8
	iv := vector.NewVec(vector.VecInt64, rows)
	copy(iv.I64(), []int64{4, -3, 0, 100, 7, 2, -50, 9})
	iv.Valid = vector.NewAllValid(rows)
	iv.Valid.SetInvalid(5)
	fv := vector.NewVec(vector.VecFloat64, rows)
	copy(fv.F64(), []float64{0.5, 2.0, -1.25, 0.0, 3.5, 10.0, 0.25, -4.0})
	dv := vector.NewVec(vector.VecInt64, rows)
	copy(dv.I64(), []int64{2, 3, 0, 5, 1, 4, 2, 3})
	batch, err := vector.NewBatch([]vector.Column{
		{Name: "a", Type: schema.Int64, V: iv},
		{Name: "f", Type: schema.Float64, V: fv},
		{Name: "d", Type: schema.Int64, V: dv},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	sel := vector.NewSelectionMask(rows)
	sel.FillAll()
	sel.Unset(2)
	if err := batch.SetSel(&sel); err != nil {
		t.Fatalf("SetSel: %v", err)
	}
	return batch
}

func TestVecEvalArith_MatchesRowEval(t *testing.T) {
	batch := buildExprBatch(t)
	i64, f64 := schema.Int64, schema.Float64
	cases := []struct {
		name string
		expr sql.BoundExpr
		vk   vector.VecKind
	}{
		{"int add const", exprBin(sql.ExprAdd, i64, exprCol("a", i64), exprLit(int64(7), i64)), vector.VecInt64},
		{"int mul cols", exprBin(sql.ExprMultiply, i64, exprCol("a", i64), exprCol("d", i64)), vector.VecInt64},
		{"int sub swapped", exprBin(sql.ExprSubtract, i64, exprLit(int64(1), i64), exprCol("a", i64)), vector.VecInt64},
		{"int div col", exprBin(sql.ExprDivide, i64, exprCol("a", i64), exprCol("d", i64)), vector.VecInt64},
		{"int mod const", exprBin(sql.ExprModulo, i64, exprCol("a", i64), exprLit(int64(3), i64)), vector.VecInt64},
		{"float mul", exprBin(sql.ExprMultiply, f64, exprCol("f", f64), exprLit(2.5, f64)), vector.VecFloat64},
		{"mixed promote", exprBin(sql.ExprMultiply, f64, exprCol("a", i64), exprCol("f", f64)), vector.VecFloat64},
		{"revenue shape", exprBin(sql.ExprMultiply, f64, exprCol("f", f64), exprBin(sql.ExprSubtract, f64, exprLit(1.0, f64), exprCol("f", f64))), vector.VecFloat64},
		{"const fold", exprBin(sql.ExprAdd, i64, exprLit(int64(2), i64), exprLit(int64(3), i64)), vector.VecInt64},
		{"nested nulls", exprBin(sql.ExprAdd, i64, exprBin(sql.ExprMultiply, i64, exprCol("a", i64), exprLit(int64(2), i64)), exprCol("d", i64)), vector.VecInt64},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, gotValid, ok, err := vecEvalArith(batch, tc.expr, batch.Sel, batch.Len, tc.vk)
			if err != nil {
				t.Fatalf("vecEvalArith: %v", err)
			}
			if !ok {
				t.Fatal("expected vectorized coverage")
			}
			ctx := newEvalCtx(batch)
			batch.Sel.IterSet(func(row int) {
				want, err := ctx.eval(tc.expr, row)
				if err != nil {
					t.Fatalf("row eval row %d: %v", row, err)
				}
				if want == nil {
					if gotValid == nil || gotValid.IsValid(row) {
						t.Fatalf("row %d want NULL, got valid", row)
					}
					return
				}
				if gotValid != nil && !gotValid.IsValid(row) {
					t.Fatalf("row %d want %v, got NULL", row, want)
				}
				switch tc.vk {
				case vector.VecInt64:
					if got.I64()[row] != want.(int64) {
						t.Fatalf("row %d got %d, want %d", row, got.I64()[row], want.(int64))
					}
				case vector.VecFloat64:
					if got.F64()[row] != want.(float64) {
						t.Fatalf("row %d got %v, want %v", row, got.F64()[row], want.(float64))
					}
				}
			})
		})
	}
}

// Row 2 divides by zero but is unselected, so the vectorized path must not raise.
func TestVecEvalArith_MaskedDivideByZero(t *testing.T) {
	batch := buildExprBatch(t)
	expr := exprBin(sql.ExprDivide, schema.Int64, exprCol("a", schema.Int64), exprCol("d", schema.Int64))
	if _, _, ok, err := vecEvalArith(batch, expr, batch.Sel, batch.Len, vector.VecInt64); err != nil || !ok {
		t.Fatalf("masked divide: ok=%v err=%v", ok, err)
	}
	sel := vector.NewSelectionMask(batch.Len)
	sel.FillAll()
	if _, _, _, err := vecEvalArith(batch, expr, &sel, batch.Len, vector.VecInt64); err == nil {
		t.Fatal("selected zero divisor must error")
	}
}

func TestVecEvalArith_FallsBackOnUnsupported(t *testing.T) {
	batch := buildExprBatch(t)
	expr := exprBin(sql.ExprConcat, schema.Text, exprCol("a", schema.Int64), exprLit("x", schema.Text))
	if _, _, ok, err := vecEvalArith(batch, expr, batch.Sel, batch.Len, vector.VecInt64); ok || err != nil {
		t.Fatalf("concat should fall back, ok=%v err=%v", ok, err)
	}
	fm := exprBin(sql.ExprModulo, schema.Float64, exprCol("f", schema.Float64), exprLit(2.0, schema.Float64))
	if _, _, ok, err := vecEvalArith(batch, fm, batch.Sel, batch.Len, vector.VecFloat64); ok || err != nil {
		t.Fatalf("float modulo should fall back, ok=%v err=%v", ok, err)
	}
}
