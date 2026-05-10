package storage

type PredicatePrunePlan struct {
	root boundPrunePredicate
}

type boundPrunePredicate struct {
	op       PredicateOp
	colIndex int
	missing  bool

	boolValue  bool
	int64Value int64
	lo         int64
	hi         int64
	textValue  string

	boolSet boolMatcher
	intSet  int64Matcher
	textSet textMatcher

	children []boundPrunePredicate
}

func BindPrunePredicate(pred Predicate, meta SegmentMeta) PredicatePrunePlan {
	return PredicatePrunePlan{root: bindPruneNode(pred, meta)}
}

func (p PredicatePrunePlan) PageCandidate(meta SegmentMeta, pageIndex int) bool {
	return boundPrunePageCandidate(meta, pageIndex, p.root)
}

func (p PredicatePrunePlan) SegmentCandidate(meta SegmentMeta) bool {
	return boundPruneSegmentCandidate(meta, p.root)
}

func bindPruneNode(pred Predicate, meta SegmentMeta) boundPrunePredicate {
	node := boundPrunePredicate{
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
		return node
	case PredicateAnd, PredicateOr, PredicateNot:
		node.children = make([]boundPrunePredicate, len(pred.Children))
		for i, child := range pred.Children {
			node.children[i] = bindPruneNode(child, meta)
		}
		return node
	default:
		for i, col := range meta.Columns {
			if col.Name == pred.Column {
				node.colIndex = i
				return node
			}
		}
		node.missing = true
		return node
	}
}

func boundPrunePageCandidate(meta SegmentMeta, pageIndex int, pred boundPrunePredicate) bool {
	switch pred.op {
	case PredicateNone:
		return true
	case PredicateAnd:
		for _, child := range pred.children {
			if !boundPrunePageCandidate(meta, pageIndex, child) {
				return false
			}
		}
		return true
	case PredicateOr:
		if len(pred.children) == 0 {
			return true
		}
		for _, child := range pred.children {
			if boundPrunePageCandidate(meta, pageIndex, child) {
				return true
			}
		}
		return false
	case PredicateNot:
		return true
	default:
		return boundLeafPrunePageCandidate(meta, pageIndex, pred)
	}
}

func boundPruneSegmentCandidate(meta SegmentMeta, pred boundPrunePredicate) bool {
	switch pred.op {
	case PredicateNone:
		return true
	case PredicateAnd:
		for _, child := range pred.children {
			if !boundPruneSegmentCandidate(meta, child) {
				return false
			}
		}
		return true
	case PredicateOr:
		if len(pred.children) == 0 {
			return true
		}
		for _, child := range pred.children {
			if boundPruneSegmentCandidate(meta, child) {
				return true
			}
		}
		return false
	case PredicateNot:
		return true
	default:
		return boundLeafPruneSegmentCandidate(meta, pred)
	}
}

func boundLeafPruneSegmentCandidate(meta SegmentMeta, pred boundPrunePredicate) bool {
	if pred.missing || pred.colIndex < 0 || pred.colIndex >= len(meta.Columns) {
		return true
	}
	col := meta.Columns[pred.colIndex]
	if col.AllNull {
		return false
	}
	if col.Bool != nil && !boundBoolPageCandidate(*col.Bool, pred) {
		return false
	}
	if col.Int64 != nil && !boundInt64PageCandidate(*col.Int64, pred) {
		return false
	}
	if col.Int32 != nil && !boundInt64PageCandidate(Int64Stats{Min: int64(col.Int32.Min), Max: int64(col.Int32.Max)}, pred) {
		return false
	}
	if col.Text != nil && !boundTextPageCandidate(*col.Text, pred) {
		return false
	}
	return true
}

func boundLeafPrunePageCandidate(meta SegmentMeta, pageIndex int, pred boundPrunePredicate) bool {
	if pred.missing || pred.colIndex < 0 || pred.colIndex >= len(meta.Columns) {
		return true
	}
	col := meta.Columns[pred.colIndex]
	if pageIndex < 0 || pageIndex >= len(col.Pages) {
		return true
	}
	page := col.Pages[pageIndex]
	if page.AllNull {
		return false
	}
	if page.Bool != nil && !boundBoolPageCandidate(*page.Bool, pred) {
		return false
	}
	if page.Int64 != nil && !boundInt64PageCandidate(*page.Int64, pred) {
		return false
	}
	if page.Int64Values != nil && !boundInt64ValuePageCandidate(*page.Int64Values, pred) {
		return false
	}
	if page.Int32 != nil && !boundInt64PageCandidate(Int64Stats{Min: int64(page.Int32.Min), Max: int64(page.Int32.Max)}, pred) {
		return false
	}
	if page.Int32Values != nil && !boundInt32ValuePageCandidate(*page.Int32Values, pred) {
		return false
	}
	if page.Text != nil && !boundTextPageCandidate(*page.Text, pred) {
		return false
	}
	return true
}

func boundBoolPageCandidate(stats BoolStats, pred boundPrunePredicate) bool {
	switch pred.op {
	case PredicateOpEq:
		return boolStatsHas(stats, pred.boolValue)
	case PredicateOpNotEq:
		return boolStatsHas(stats, !pred.boolValue)
	case PredicateOpIn:
		return (stats.HasTrue && pred.boolSet.Has(true)) || (stats.HasFalse && pred.boolSet.Has(false))
	case PredicateOpNotIn:
		return (stats.HasTrue && !pred.boolSet.Has(true)) || (stats.HasFalse && !pred.boolSet.Has(false))
	default:
		return true
	}
}

func boundInt64PageCandidate(stats Int64Stats, pred boundPrunePredicate) bool {
	switch pred.op {
	case PredicateOpEq:
		return stats.Min <= pred.int64Value && pred.int64Value <= stats.Max
	case PredicateOpLess:
		return stats.Min < pred.int64Value
	case PredicateOpLessEqual:
		return stats.Min <= pred.int64Value
	case PredicateOpGreater:
		return stats.Max > pred.int64Value
	case PredicateOpGreaterEqual:
		return stats.Max >= pred.int64Value
	case PredicateOpBetween:
		return pred.lo <= stats.Max && stats.Min <= pred.hi
	case PredicateOpIn:
		return pred.intSet.AnyBetween(stats.Min, stats.Max)
	default:
		return true
	}
}

func boundInt64ValuePageCandidate(stats Int64ValueStats, pred boundPrunePredicate) bool {
	if stats.Truncated || len(stats.Values) == 0 {
		return true
	}
	switch pred.op {
	case PredicateOpEq:
		for _, value := range stats.Values {
			if value == pred.int64Value {
				return true
			}
		}
		return false
	case PredicateOpIn:
		for _, value := range stats.Values {
			if pred.intSet.Has(value) {
				return true
			}
		}
		return false
	default:
		return true
	}
}

func boundInt32ValuePageCandidate(stats Int32ValueStats, pred boundPrunePredicate) bool {
	if stats.Truncated || len(stats.Values) == 0 {
		return true
	}
	switch pred.op {
	case PredicateOpEq:
		value, ok := int64ToInt32Value(pred.int64Value)
		if !ok {
			return false
		}
		for _, candidate := range stats.Values {
			if candidate == value {
				return true
			}
		}
		return false
	case PredicateOpIn:
		for _, value := range stats.Values {
			if pred.intSet.Has(int64(value)) {
				return true
			}
		}
		return false
	default:
		return true
	}
}

func boundTextPageCandidate(stats TextStats, pred boundPrunePredicate) bool {
	if stats.Truncated {
		switch pred.op {
		case PredicateOpEq:
			return textHashSetHas(stats.Hashes, pred.textValue)
		case PredicateOpIn:
			return pred.textSet.AnyHashIn(stats.Hashes)
		default:
			return true
		}
	}
	switch pred.op {
	case PredicateOpEq:
		for _, value := range stats.Values {
			if value == pred.textValue {
				return true
			}
		}
		return false
	case PredicateOpIn:
		for _, value := range stats.Values {
			if pred.textSet.Has(value) {
				return true
			}
		}
		return false
	default:
		return true
	}
}
