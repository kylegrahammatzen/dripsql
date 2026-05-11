package storage

import (
	"fmt"
	"math/bits"
	"slices"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

// boundNode is the shared predicate tree for row evaluation and segment pruning.
// colIndex points to the bound column source: batch.Columns for eval, meta.Columns
// for pruning. A negative leaf colIndex means the column is missing: eval fails
// at bind time, while pruning treats it as possibly matching.
type boundNode struct {
	op       PredicateOp
	colIndex int

	boolValue  bool
	int64Value int64
	lo         int64
	hi         int64
	textValue  string
	uuidValue  types.UUID16

	boolSet boolMatcher
	intSet  int64Matcher
	textSet textMatcher
	uuidSet uuidMatcher

	children []boundNode
}

type boolMatcher struct {
	allowTrue  bool
	allowFalse bool
}

type int64Matcher struct {
	small []int64
	large map[int64]struct{}
}

type textMatcher struct {
	small []string
	large map[string]struct{}
}

type uuidMatcher struct {
	small []types.UUID16
	large map[types.UUID16]struct{}
}

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

type BoundPredicateEvaluator struct {
	root    boundNode
	columns []string
}

func BindPredicate(pred Predicate, batch types.Batch) (BoundPredicateEvaluator, error) {
	root, err := bindEvalNode(pred, batch)
	if err != nil {
		return BoundPredicateEvaluator{}, err
	}
	return BoundPredicateEvaluator{root: root, columns: PredicateColumns(pred)}, nil
}

func (e BoundPredicateEvaluator) RequiredColumns() []string {
	return e.columns
}

func (e BoundPredicateEvaluator) Eval(batch types.Batch, sel *types.SelectionMask) (int, error) {
	if sel == nil {
		return 0, fmt.Errorf("selection mask is nil")
	}
	return evalBoundInto(batch, e.root, sel)
}

func (e BoundPredicateEvaluator) EvalSelected(batch types.Batch, input types.SelectionMask, sel *types.SelectionMask) (int, error) {
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
	return evalBoundSelectedInto(batch, e.root, input, sel)
}

func bindEvalNode(pred Predicate, batch types.Batch) (boundNode, error) {
	node := boundNode{
		op:         pred.Op,
		colIndex:   -1,
		boolValue:  pred.Bool,
		int64Value: pred.Int64,
		lo:         pred.Lo,
		hi:         pred.Hi,
		textValue:  pred.Text,
		uuidValue:  pred.UUID,
		boolSet:    newBoolMatcher(pred.Bools),
		intSet:     newInt64Matcher(pred.Int64s),
		textSet:    newTextMatcher(pred.Texts),
		uuidSet:    newUUIDMatcher(pred.UUIDs),
	}
	switch pred.Op {
	case PredicateNone:
		return node, nil
	case PredicateAnd, PredicateOr, PredicateNot:
		node.children = make([]boundNode, len(pred.Children))
		for i, child := range pred.Children {
			boundChild, err := bindEvalNode(child, batch)
			if err != nil {
				return boundNode{}, err
			}
			node.children[i] = boundChild
		}
		return node, nil
	default:
		colIndex, ok := columnIndexByName(batch, pred.Column)
		if !ok {
			return boundNode{}, fmt.Errorf("missing predicate column %q", pred.Column)
		}
		node.colIndex = colIndex
		return node, nil
	}
}

func evalBoundInto(batch types.Batch, pred boundNode, sel *types.SelectionMask) (int, error) {
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

func evalBoundSelectedInto(batch types.Batch, pred boundNode, input types.SelectionMask, sel *types.SelectionMask) (int, error) {
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

func newBoolMatcher(values []bool) boolMatcher {
	var m boolMatcher
	for _, value := range values {
		if value {
			m.allowTrue = true
		} else {
			m.allowFalse = true
		}
	}
	return m
}

func (m boolMatcher) Has(value bool) bool {
	if value {
		return m.allowTrue
	}
	return m.allowFalse
}

func newInt64Matcher(values []int64) int64Matcher {
	if len(values) <= 8 {
		return int64Matcher{small: slices.Clone(values)}
	}
	large := make(map[int64]struct{}, len(values))
	for _, value := range values {
		large[value] = struct{}{}
	}
	return int64Matcher{large: large}
}

func (m int64Matcher) Has(value int64) bool {
	if m.large != nil {
		_, ok := m.large[value]
		return ok
	}
	for _, candidate := range m.small {
		if candidate == value {
			return true
		}
	}
	return false
}

func (m int64Matcher) AnyBetween(min int64, max int64) bool {
	if m.large != nil {
		for value := range m.large {
			if min <= value && value <= max {
				return true
			}
		}
		return false
	}
	for _, value := range m.small {
		if min <= value && value <= max {
			return true
		}
	}
	return false
}

func newTextMatcher(values []string) textMatcher {
	if len(values) <= 8 {
		return textMatcher{small: slices.Clone(values)}
	}
	large := make(map[string]struct{}, len(values))
	for _, value := range values {
		large[value] = struct{}{}
	}
	return textMatcher{large: large}
}

func (m textMatcher) Has(value string) bool {
	if m.large != nil {
		_, ok := m.large[value]
		return ok
	}
	for _, candidate := range m.small {
		if candidate == value {
			return true
		}
	}
	return false
}

func (m textMatcher) AnyInBloom(bloom []uint64, probes uint64) bool {
	if len(bloom) == 0 {
		return true
	}
	if m.large != nil {
		for value := range m.large {
			if hashBloomHas(bloom, textHash32String(value), probes) {
				return true
			}
		}
		return false
	}
	for _, value := range m.small {
		if hashBloomHas(bloom, textHash32String(value), probes) {
			return true
		}
	}
	return false
}

func newUUIDMatcher(values []types.UUID16) uuidMatcher {
	if len(values) <= 8 {
		return uuidMatcher{small: slices.Clone(values)}
	}
	large := make(map[types.UUID16]struct{}, len(values))
	for _, value := range values {
		large[value] = struct{}{}
	}
	return uuidMatcher{large: large}
}

func (m uuidMatcher) Has(value types.UUID16) bool {
	if m.large != nil {
		_, ok := m.large[value]
		return ok
	}
	for _, candidate := range m.small {
		if candidate == value {
			return true
		}
	}
	return false
}

func (m uuidMatcher) AnyInBloom(bloom []uint64, probes uint64) bool {
	if len(bloom) == 0 {
		return true
	}
	if m.large != nil {
		for value := range m.large {
			if hashBloomHas(bloom, uuidHash32(value), probes) {
				return true
			}
		}
		return false
	}
	for _, value := range m.small {
		if hashBloomHas(bloom, uuidHash32(value), probes) {
			return true
		}
	}
	return false
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
