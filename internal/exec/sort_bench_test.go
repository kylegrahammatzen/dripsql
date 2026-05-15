// Sort benchmarks lock in the typed int64 fast path (full sort + bounded heap top-K) and
// the slow generic fallback. Run with `go test ./internal/exec -bench=Sort -benchmem`.
package exec

import (
	"context"
	"math/rand"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func buildSortSource(rows int, seed int64) *bufferSource {
	r := rand.New(rand.NewSource(seed))
	const batchSize = types.StandardBatchRows
	var batches []types.Batch
	for off := 0; off < rows; off += batchSize {
		n := batchSize
		if off+n > rows {
			n = rows - off
		}
		v := types.NewVec(types.VecInt64, n)
		s := v.I64()
		for i := range s {
			s[i] = r.Int63()
		}
		col := types.Column{Name: "k", Type: types.Int64, V: v}
		b, _ := types.NewBatch([]types.Column{col})
		sel := types.NewSelectionMask(n)
		sel.FillAll()
		b.Sel = &sel
		batches = append(batches, b)
	}
	return &bufferSource{batches: batches}
}

type bufferSource struct {
	batches []types.Batch
	cursor  int
	state   operatorState
}

func (b *bufferSource) Open(ctx context.Context) error { return b.state.open() }
func (b *bufferSource) Next() (types.Batch, bool, error) {
	if b.cursor >= len(b.batches) {
		return types.Batch{}, false, nil
	}
	out := b.batches[b.cursor]
	b.cursor++
	return out, true, nil
}
func (b *bufferSource) Close() error { b.state.close(); return nil }

func runSort(b *testing.B, op *SortOp) {
	b.Helper()
	for b.Loop() {
		src := op.Source.(*bufferSource)
		src.cursor = 0
		src.state = operatorState(0)
		op.state = operatorState(0)
		op.built = false
		op.cursor = 0
		op.bufs = nil
		op.order = nil
		op.refs = nil
		op.wideRefs = nil
		op.keys = nil
		op.nulls = nil
		op.slowRows = nil
		if err := op.Open(context.Background()); err != nil {
			b.Fatal(err)
		}
		for {
			_, ok, err := op.Next()
			if err != nil {
				b.Fatal(err)
			}
			if !ok {
				break
			}
		}
		op.Close()
	}
}

func keyExpr() sql.BoundExpr {
	return sql.BoundExpr{Op: sql.ExprColumn, Type: types.Int64, Column: "k"}
}

func BenchmarkSort_FullAsc_Int64_10k(b *testing.B) {
	op := &SortOp{Source: buildSortSource(10_000, 1), Keys: []sql.SortKey{{Expr: keyExpr()}}}
	runSort(b, op)
}

func BenchmarkSort_FullDesc_Int64_10k(b *testing.B) {
	op := &SortOp{Source: buildSortSource(10_000, 1), Keys: []sql.SortKey{{Expr: keyExpr(), Desc: true}}}
	runSort(b, op)
}

func BenchmarkSort_TopK_Int64_10k_K100(b *testing.B) {
	op := &SortOp{
		Source: buildSortSource(10_000, 1),
		Keys:   []sql.SortKey{{Expr: keyExpr(), Desc: true}},
		K:      100,
	}
	runSort(b, op)
}

func BenchmarkSort_TopK_Int64_100k_K100(b *testing.B) {
	op := &SortOp{
		Source: buildSortSource(100_000, 1),
		Keys:   []sql.SortKey{{Expr: keyExpr(), Desc: true}},
		K:      100,
	}
	runSort(b, op)
}

func BenchmarkSort_TopK_Int64_100k_K100_Off50(b *testing.B) {
	op := &SortOp{
		Source: buildSortSource(100_000, 1),
		Keys:   []sql.SortKey{{Expr: keyExpr(), Desc: true}},
		K:      100,
		Offset: 50,
	}
	runSort(b, op)
}

func buildSortSourceWithNulls(rows int, seed int64, nullEvery int) *bufferSource {
	r := rand.New(rand.NewSource(seed))
	const batchSize = types.StandardBatchRows
	var batches []types.Batch
	idx := 0
	for off := 0; off < rows; off += batchSize {
		n := batchSize
		if off+n > rows {
			n = rows - off
		}
		v := types.NewVec(types.VecInt64, n)
		s := v.I64()
		valid := types.NewAllValid(n)
		for i := range s {
			if idx%nullEvery == 0 {
				valid.SetInvalid(i)
			} else {
				s[i] = r.Int63()
			}
			idx++
		}
		v.Valid = valid
		col := types.Column{Name: "k", Type: types.Int64, V: v}
		b, _ := types.NewBatch([]types.Column{col})
		sel := types.NewSelectionMask(n)
		sel.FillAll()
		b.Sel = &sel
		batches = append(batches, b)
	}
	return &bufferSource{batches: batches}
}

func BenchmarkSort_TopK_Int64_100k_K100_NullsEvery10(b *testing.B) {
	op := &SortOp{
		Source: buildSortSourceWithNulls(100_000, 1, 10),
		Keys:   []sql.SortKey{{Expr: keyExpr(), Desc: true}},
		K:      100,
	}
	runSort(b, op)
}

func buildSortSourceText(rows int, seed int64) *bufferSource {
	r := rand.New(rand.NewSource(seed))
	const batchSize = types.StandardBatchRows
	var batches []types.Batch
	for off := 0; off < rows; off += batchSize {
		n := batchSize
		if off+n > rows {
			n = rows - off
		}
		v := types.NewVarVec(types.VecText, n, 0)
		for i := range n {
			v.Var().AppendString(i, randText(r))
		}
		col := types.Column{Name: "s", Type: types.Text, V: v}
		b, _ := types.NewBatch([]types.Column{col})
		sel := types.NewSelectionMask(n)
		sel.FillAll()
		b.Sel = &sel
		batches = append(batches, b)
	}
	return &bufferSource{batches: batches}
}

func randText(r *rand.Rand) string {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	n := 5 + r.Intn(8)
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = letters[r.Intn(len(letters))]
	}
	return string(buf)
}

func BenchmarkSort_FullAsc_Text_10k(b *testing.B) {
	op := &SortOp{
		Source: buildSortSourceText(10_000, 1),
		Keys:   []sql.SortKey{{Expr: sql.BoundExpr{Op: sql.ExprColumn, Type: types.Text, Column: "s"}}},
	}
	runSort(b, op)
}
