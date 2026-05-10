package storage

import (
	"fmt"
	"math/bits"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type PredicateEvaluator interface {
	RequiredColumns() []string
	Eval(batch types.Batch, sel *types.SelectionMask) (int, error)
	EvalSelected(batch types.Batch, input types.SelectionMask, sel *types.SelectionMask) (int, error)
}

type predicateEvaluator struct {
	pred    Predicate
	columns []string
	bound   BoundPredicateEvaluator
	schema  []string
	boundOK bool
}

func NewPredicateEvaluator(pred Predicate) PredicateEvaluator {
	return &predicateEvaluator{pred: pred, columns: PredicateColumns(pred)}
}

func (e *predicateEvaluator) RequiredColumns() []string {
	return e.columns
}

func (e *predicateEvaluator) Eval(batch types.Batch, sel *types.SelectionMask) (int, error) {
	if sel == nil {
		return 0, fmt.Errorf("selection mask is nil")
	}
	bound, err := e.boundForBatch(batch)
	if err != nil {
		return 0, err
	}
	return bound.Eval(batch, sel)
}

func (e *predicateEvaluator) EvalSelected(batch types.Batch, input types.SelectionMask, sel *types.SelectionMask) (int, error) {
	if sel == nil {
		return 0, fmt.Errorf("selection mask is nil")
	}
	if input.Rows != batch.Len {
		return 0, fmt.Errorf("selection rows %d do not match batch length %d", input.Rows, batch.Len)
	}
	if selectionAliases(input, *sel) {
		copyInput := types.NewSelectionMask(input.Rows)
		copy(copyInput.Words, input.Words)
		input = copyInput
	}
	bound, err := e.boundForBatch(batch)
	if err != nil {
		return 0, err
	}
	return bound.EvalSelected(batch, input, sel)
}

func (e *predicateEvaluator) boundForBatch(batch types.Batch) (BoundPredicateEvaluator, error) {
	if e.boundOK && sameBatchSchema(e.schema, batch) {
		return e.bound, nil
	}
	bound, err := BindPredicate(e.pred, batch)
	if err != nil {
		return BoundPredicateEvaluator{}, err
	}
	e.bound = bound
	e.schema = batchSchema(e.schema[:0], batch)
	e.boundOK = true
	return bound, nil
}

const selectionScratchWords = (types.StandardBatchRows + 63) / 64

func scratchSelectionMask(rows int, words *[selectionScratchWords]uint64) types.SelectionMask {
	wordCount := types.ValidityWords(rows)
	if wordCount > len(words) {
		return types.NewSelectionMask(rows)
	}
	return types.SelectionMask{Words: words[:wordCount], Rows: rows}
}

func copySelectionInto(dst *types.SelectionMask, src types.SelectionMask) int {
	if selectionAliases(src, *dst) {
		return src.PopCount()
	}
	dst.Resize(src.Rows)
	copy(dst.Words, src.Words)
	return dst.PopCount()
}

func selectValidRows(valid types.Validity, rows int, input *types.SelectionMask, out *types.SelectionMask) int {
	if input == nil {
		if valid == nil {
			return out.FillAll()
		}
		matched := 0
		for row := 0; row < rows; row++ {
			if types.IsValid(valid, row) {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	if valid == nil {
		return copySelectionInto(out, *input)
	}
	matched := 0
	input.IterSet(func(row int) {
		if types.IsValid(valid, row) {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func copySelectionWords(dst *types.SelectionMask, src types.SelectionMask) {
	dst.Resize(src.Rows)
	copy(dst.Words, src.Words)
}

func andNotSelectionInto(dst *types.SelectionMask, left types.SelectionMask, right types.SelectionMask) int {
	leftRows := left.Rows
	dst.Resize(leftRows)
	if leftRows == 0 {
		return 0
	}
	last := len(left.Words) - 1
	count := 0
	for i := range left.Words {
		word := left.Words[i] &^ right.Words[i]
		if i == last {
			word &= predicateTailMask(leftRows)
		}
		dst.Words[i] = word
		count += bits.OnesCount64(word)
	}
	return count
}

func selectionAliases(left types.SelectionMask, right types.SelectionMask) bool {
	return len(left.Words) != 0 && len(right.Words) != 0 && &left.Words[0] == &right.Words[0]
}

func predicateTailMask(rows int) uint64 {
	if rem := rows & 63; rem != 0 {
		return (uint64(1) << uint(rem)) - 1
	}
	return ^uint64(0)
}

func dictTextID(values types.VarBytes, value string) (uint8, bool) {
	for row := 0; row < values.Rows(); row++ {
		if values.String(row) == value {
			return uint8(row), true
		}
	}
	return 0, false
}

func matchDictTextID(present bool, op PredicateOp) bool {
	switch op {
	case PredicateOpEq, PredicateOpIn:
		return present
	case PredicateOpNotEq, PredicateOpNotIn:
		return !present
	default:
		return false
	}
}
