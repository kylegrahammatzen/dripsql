package storage

import (
	"fmt"
	"slices"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type BoundPredicateEvaluator struct {
	root    boundPredicate
	columns []string
}

type boundPredicate struct {
	op       PredicateOp
	colIndex int

	boolValue  bool
	int64Value int64
	lo         int64
	hi         int64
	textValue  string

	boolSet boolMatcher
	intSet  int64Matcher
	textSet textMatcher

	children []boundPredicate
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

func BindPredicate(pred Predicate, batch types.Batch) (BoundPredicateEvaluator, error) {
	root, err := bindPredicateNode(pred, batch)
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
	return evalBoundPredicateInto(batch, e.root, sel)
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
	return evalBoundPredicateSelectedInto(batch, e.root, input, sel)
}

func bindPredicateNode(pred Predicate, batch types.Batch) (boundPredicate, error) {
	node := boundPredicate{
		op:         pred.Op,
		colIndex:   -1,
		boolValue:  pred.Bool,
		int64Value: pred.Int64,
		lo:         pred.Lo,
		hi:         pred.Hi,
		textValue:  pred.Text,
		boolSet:    newBoolMatcher(pred.Bools),
		intSet:     newInt64Matcher(pred.Int64s),
		textSet:    newTextMatcher(pred.Texts),
	}
	switch pred.Op {
	case PredicateNone:
		return node, nil
	case PredicateAnd, PredicateOr, PredicateNot:
		node.children = make([]boundPredicate, len(pred.Children))
		for i, child := range pred.Children {
			boundChild, err := bindPredicateNode(child, batch)
			if err != nil {
				return boundPredicate{}, err
			}
			node.children[i] = boundChild
		}
		return node, nil
	default:
		colIndex, ok := columnIndexByName(batch, pred.Column)
		if !ok {
			return boundPredicate{}, fmt.Errorf("missing predicate column %q", pred.Column)
		}
		node.colIndex = colIndex
		return node, nil
	}
}

func evalBoundPredicateInto(batch types.Batch, pred boundPredicate, sel *types.SelectionMask) (int, error) {
	switch pred.op {
	case PredicateNone:
		sel.Resize(batch.Len)
		return sel.FillAll(), nil
	case PredicateAnd:
		if len(pred.children) == 0 {
			return 0, fmt.Errorf("AND predicate requires children")
		}
		matched, err := evalBoundPredicateInto(batch, pred.children[0], sel)
		if err != nil || matched == 0 {
			return matched, err
		}
		var scratchWords [selectionScratchWords]uint64
		scratch := scratchSelectionMask(batch.Len, &scratchWords)
		for _, child := range pred.children[1:] {
			if _, err := evalBoundPredicateInto(batch, child, &scratch); err != nil {
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
		matched, err := evalBoundPredicateInto(batch, pred.children[0], sel)
		if err != nil || matched == batch.Len {
			return matched, err
		}
		var scratchWords [selectionScratchWords]uint64
		scratch := scratchSelectionMask(batch.Len, &scratchWords)
		for _, child := range pred.children[1:] {
			if _, err := evalBoundPredicateInto(batch, child, &scratch); err != nil {
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
		if _, err := evalBoundPredicateInto(batch, pred.children[0], sel); err != nil {
			return 0, err
		}
		return sel.NotCount(), nil
	default:
		return evalBoundLeafInto(batch, pred, sel)
	}
}

func evalBoundPredicateSelectedInto(batch types.Batch, pred boundPredicate, input types.SelectionMask, sel *types.SelectionMask) (int, error) {
	switch pred.op {
	case PredicateNone:
		return copySelectionInto(sel, input), nil
	case PredicateAnd:
		if len(pred.children) == 0 {
			return 0, fmt.Errorf("AND predicate requires children")
		}
		matched, err := evalBoundPredicateSelectedInto(batch, pred.children[0], input, sel)
		if err != nil || matched == 0 {
			return matched, err
		}
		var scratchWords [selectionScratchWords]uint64
		scratch := scratchSelectionMask(batch.Len, &scratchWords)
		for _, child := range pred.children[1:] {
			matched, err = evalBoundPredicateSelectedInto(batch, child, *sel, &scratch)
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
		matched, err := evalBoundPredicateSelectedInto(batch, pred.children[0], input, sel)
		if err != nil || matched == inputCount {
			return matched, err
		}
		var scratchWords [selectionScratchWords]uint64
		scratch := scratchSelectionMask(batch.Len, &scratchWords)
		for _, child := range pred.children[1:] {
			if _, err := evalBoundPredicateSelectedInto(batch, child, input, &scratch); err != nil {
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
		if _, err := evalBoundPredicateSelectedInto(batch, pred.children[0], input, &scratch); err != nil {
			return 0, err
		}
		return andNotSelectionInto(sel, input, scratch), nil
	default:
		return evalBoundLeafSelectedInto(batch, pred, input, sel)
	}
}

func evalBoundLeafInto(batch types.Batch, pred boundPredicate, mask *types.SelectionMask) (int, error) {
	col := batch.Columns[pred.colIndex]
	mask.Resize(batch.Len)
	switch col.V.Kind {
	case types.VecBool:
		return evalBoolLeafBound(col.V, pred, nil, mask), nil
	case types.VecInt16:
		return evalIntLeafBound(col.V, col.V.I16, pred, nil, mask), nil
	case types.VecInt32, types.VecDate:
		return evalIntLeafBound(col.V, col.V.I32, pred, nil, mask), nil
	case types.VecInt64, types.VecTimestamp, types.VecTime:
		return evalIntLeafBound(col.V, col.V.I64, pred, nil, mask), nil
	case types.VecText, types.VecBytes:
		return evalTextLeafBound(col.V, pred, nil, mask)
	default:
		return 0, fmt.Errorf("predicate unsupported vector kind %s", col.V.Kind)
	}
}

func evalBoundLeafSelectedInto(batch types.Batch, pred boundPredicate, input types.SelectionMask, mask *types.SelectionMask) (int, error) {
	col := batch.Columns[pred.colIndex]
	mask.Resize(batch.Len)
	switch col.V.Kind {
	case types.VecBool:
		return evalBoolLeafBound(col.V, pred, &input, mask), nil
	case types.VecInt16:
		return evalIntLeafBound(col.V, col.V.I16, pred, &input, mask), nil
	case types.VecInt32, types.VecDate:
		return evalIntLeafBound(col.V, col.V.I32, pred, &input, mask), nil
	case types.VecInt64, types.VecTimestamp, types.VecTime:
		return evalIntLeafBound(col.V, col.V.I64, pred, &input, mask), nil
	case types.VecText, types.VecBytes:
		return evalTextLeafBound(col.V, pred, &input, mask)
	default:
		return 0, fmt.Errorf("predicate unsupported vector kind %s", col.V.Kind)
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

func (m textMatcher) AnyHashIn(hashes []uint16) bool {
	if len(hashes) == 0 {
		return true
	}
	if m.large != nil {
		for value := range m.large {
			if _, ok := slices.BinarySearch(hashes, textHash16String(value)); ok {
				return true
			}
		}
		return false
	}
	for _, value := range m.small {
		if _, ok := slices.BinarySearch(hashes, textHash16String(value)); ok {
			return true
		}
	}
	return false
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
