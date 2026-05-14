package storage

import (
	"fmt"
	"math/bits"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

// BoundPredicate is the predicate tree used for both row evaluation and
// segment pruning. colIndex points to the bound column source: batch.Columns
// for eval, meta.Columns for pruning. A negative leaf colIndex means the
// column is missing: eval fails at bind time, while pruning treats it as
// possibly matching. The root carries cached rebind state (pred, columns,
// schema, bound); inner nodes leave those zero.
type BoundPredicate struct {
	op       PredicateOp
	colIndex int

	boolValue  bool
	int64Value int64
	lo         int64
	hi         int64
	textValue  string
	uuidValue  types.UUID16

	boolSet setMatcher[bool]
	intSet  setMatcher[int64]
	textSet setMatcher[string]
	uuidSet setMatcher[types.UUID16]

	children []BoundPredicate

	pred    Predicate
	columns []string
	schema  []string
	bound   bool
}

func BindPredicate(pred Predicate) *BoundPredicate {
	return &BoundPredicate{pred: pred, columns: PredicateColumns(pred)}
}

func (b *BoundPredicate) RequiredColumns() []string {
	return b.columns
}

func (b *BoundPredicate) Eval(batch types.Batch, sel *types.SelectionMask) (int, error) {
	if sel == nil {
		return 0, fmt.Errorf("selection mask is nil")
	}
	if err := b.rebindIfNeeded(batch); err != nil {
		return 0, err
	}
	return evalBoundInto(batch, *b, sel)
}

func (b *BoundPredicate) EvalSelected(batch types.Batch, input types.SelectionMask, sel *types.SelectionMask) (int, error) {
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
	if err := b.rebindIfNeeded(batch); err != nil {
		return 0, err
	}
	return evalBoundSelectedInto(batch, *b, input, sel)
}

func (b *BoundPredicate) rebindIfNeeded(batch types.Batch) error {
	if b.bound && sameBatchSchema(b.schema, batch) {
		return nil
	}
	bound, err := bindPredicate(b.pred, batch)
	if err != nil {
		return err
	}
	bound.pred = b.pred
	bound.columns = b.columns
	bound.schema = batchSchema(b.schema[:0], batch)
	bound.bound = true
	*b = bound
	return nil
}

// newBoundLeaf builds a BoundPredicate node with all per-predicate value
// fields populated but colIndex unresolved. Eval-bind resolves colIndex via
// batch.Columns; prune-bind resolves it via meta.Columns.
func newBoundLeaf(pred Predicate) BoundPredicate {
	return BoundPredicate{
		op:         pred.Op,
		colIndex:   -1,
		boolValue:  pred.Bool,
		int64Value: pred.Int64,
		lo:         pred.Lo,
		hi:         pred.Hi,
		textValue:  pred.Text,
		uuidValue:  pred.UUID,
		boolSet:    newSetMatcher(pred.Bools),
		intSet:     newSetMatcher(pred.Int64s),
		textSet:    newSetMatcher(pred.Texts),
		uuidSet:    newSetMatcher(pred.UUIDs),
	}
}

func bindPredicate(pred Predicate, batch types.Batch) (BoundPredicate, error) {
	node := newBoundLeaf(pred)
	switch pred.Op {
	case PredicateNone:
		return node, nil
	case PredicateAnd, PredicateOr, PredicateNot:
		node.children = make([]BoundPredicate, len(pred.Children))
		for i, child := range pred.Children {
			boundChild, err := bindPredicate(child, batch)
			if err != nil {
				return BoundPredicate{}, err
			}
			node.children[i] = boundChild
		}
		return node, nil
	default:
		colIndex, ok := columnIndexByName(batch, pred.Column)
		if !ok {
			return BoundPredicate{}, fmt.Errorf("missing predicate column %q", pred.Column)
		}
		node.colIndex = colIndex
		return node, nil
	}
}

func evalBoundInto(batch types.Batch, pred BoundPredicate, sel *types.SelectionMask) (int, error) {
	switch pred.op {
	case PredicateNone:
		sel.Resize(batch.Len)
		return sel.FillAll(), nil
	case PredicateAnd:
		if len(pred.children) == 0 {
			return 0, fmt.Errorf("AND predicate requires children")
		}
		matched, err := evalBoundInto(batch, pred.children[0], sel)
		if err != nil || matched == 0 {
			return matched, err
		}
		var scratchWords [selectionScratchWords]uint64
		scratch := scratchSelectionMask(batch.Len, &scratchWords)
		for _, child := range pred.children[1:] {
			if _, err := evalBoundInto(batch, child, &scratch); err != nil {
				return 0, err
			}
			matched = sel.AndCount(scratch)
			if matched == 0 {
				return 0, nil
			}
		}
		return matched, nil
	case PredicateOr:
		if len(pred.children) == 0 {
			return 0, fmt.Errorf("OR predicate requires children")
		}
		matched, err := evalBoundInto(batch, pred.children[0], sel)
		if err != nil || matched == batch.Len {
			return matched, err
		}
		var scratchWords [selectionScratchWords]uint64
		scratch := scratchSelectionMask(batch.Len, &scratchWords)
		for _, child := range pred.children[1:] {
			if _, err := evalBoundInto(batch, child, &scratch); err != nil {
				return 0, err
			}
			matched = sel.OrCount(scratch)
			if matched == batch.Len {
				return matched, nil
			}
		}
		return matched, nil
	case PredicateNot:
		if len(pred.children) != 1 {
			return 0, fmt.Errorf("NOT predicate requires one child")
		}
		if _, err := evalBoundInto(batch, pred.children[0], sel); err != nil {
			return 0, err
		}
		return sel.NotCount(), nil
	default:
		return evalLeafInto(batch, pred, sel)
	}
}

func evalBoundSelectedInto(batch types.Batch, pred BoundPredicate, input types.SelectionMask, sel *types.SelectionMask) (int, error) {
	switch pred.op {
	case PredicateNone:
		return copySelectionInto(sel, input), nil
	case PredicateAnd:
		if len(pred.children) == 0 {
			return 0, fmt.Errorf("AND predicate requires children")
		}
		matched, err := evalBoundSelectedInto(batch, pred.children[0], input, sel)
		if err != nil || matched == 0 {
			return matched, err
		}
		var scratchWords [selectionScratchWords]uint64
		scratch := scratchSelectionMask(batch.Len, &scratchWords)
		for _, child := range pred.children[1:] {
			matched, err = evalBoundSelectedInto(batch, child, *sel, &scratch)
			if err != nil || matched == 0 {
				return matched, err
			}
			copySelectionWords(sel, scratch)
		}
		return matched, nil
	case PredicateOr:
		if len(pred.children) == 0 {
			return 0, fmt.Errorf("OR predicate requires children")
		}
		inputCount := input.PopCount()
		matched, err := evalBoundSelectedInto(batch, pred.children[0], input, sel)
		if err != nil || matched == inputCount {
			return matched, err
		}
		var scratchWords [selectionScratchWords]uint64
		scratch := scratchSelectionMask(batch.Len, &scratchWords)
		for _, child := range pred.children[1:] {
			if _, err := evalBoundSelectedInto(batch, child, input, &scratch); err != nil {
				return 0, err
			}
			matched = sel.OrCount(scratch)
			if matched == inputCount {
				return matched, nil
			}
		}
		return matched, nil
	case PredicateNot:
		if len(pred.children) != 1 {
			return 0, fmt.Errorf("NOT predicate requires one child")
		}
		var scratchWords [selectionScratchWords]uint64
		scratch := scratchSelectionMask(batch.Len, &scratchWords)
		if _, err := evalBoundSelectedInto(batch, pred.children[0], input, &scratch); err != nil {
			return 0, err
		}
		return andNotSelectionInto(sel, input, scratch), nil
	default:
		return evalLeafSelectedInto(batch, pred, input, sel)
	}
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

func copySelectionWords(dst *types.SelectionMask, src types.SelectionMask) {
	dst.Resize(src.Rows)
	copy(dst.Words, src.Words)
}

func selectValidRows(valid types.Validity, rows int, input *types.SelectionMask, out *types.SelectionMask) int {
	if input == nil {
		if valid == nil {
			return out.FillAll()
		}
		matched := 0
		for row := range rows {
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

func sameBatchSchema(schema []string, batch types.Batch) bool {
	if len(schema) != len(batch.Columns) {
		return false
	}
	for i, name := range schema {
		if name != batch.Columns[i].Name {
			return false
		}
	}
	return true
}

func batchSchema(dst []string, batch types.Batch) []string {
	if cap(dst) < len(batch.Columns) {
		dst = make([]string, 0, len(batch.Columns))
	}
	dst = dst[:0]
	for _, col := range batch.Columns {
		dst = append(dst, col.Name)
	}
	return dst
}

func columnIndexByName(batch types.Batch, name string) (int, bool) {
	for i, col := range batch.Columns {
		if col.Name == name {
			return i, true
		}
	}
	return -1, false
}

func dictTextID(values types.VarBytes, value string) (uint8, bool) {
	for row := 0; row < values.Rows(); row++ {
		if values.String(row) == value {
			return uint8(row), true
		}
	}
	return 0, false
}
