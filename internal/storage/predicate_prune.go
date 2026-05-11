package storage

import "slices"

const (
	minInt32Value int64 = -1 << 31
	maxInt32Value int64 = 1<<31 - 1
)

type PredicatePrunePlan struct {
	root boundNode
}

func BindPrunePredicate(pred Predicate, meta SegmentMeta) PredicatePrunePlan {
	return PredicatePrunePlan{root: bindPruneNode(pred, meta)}
}

func (p PredicatePrunePlan) PageCandidate(meta SegmentMeta, pageIndex int) bool {
	return prunePageCandidate(meta, pageIndex, p.root)
}

func (p PredicatePrunePlan) SegmentCandidate(meta SegmentMeta) bool {
	return pruneSegmentCandidate(meta, p.root)
}

func bindPruneNode(pred Predicate, meta SegmentMeta) boundNode {
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
		return node
	case PredicateAnd, PredicateOr, PredicateNot:
		node.children = make([]boundNode, len(pred.Children))
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
		return node
	}
}

func prunePageCandidate(meta SegmentMeta, pageIndex int, pred boundNode) bool {
	switch pred.op {
	case PredicateNone:
		return true
	case PredicateAnd:
		for _, child := range pred.children {
			if !prunePageCandidate(meta, pageIndex, child) {
				return false
			}
		}
		return true
	case PredicateOr:
		if len(pred.children) == 0 {
			return true
		}
		for _, child := range pred.children {
			if prunePageCandidate(meta, pageIndex, child) {
				return true
			}
		}
		return false
	case PredicateNot:
		return true
	default:
		return prunePageLeafCandidate(meta, pageIndex, pred)
	}
}

func pruneSegmentCandidate(meta SegmentMeta, pred boundNode) bool {
	switch pred.op {
	case PredicateNone:
		return true
	case PredicateAnd:
		for _, child := range pred.children {
			if !pruneSegmentCandidate(meta, child) {
				return false
			}
		}
		return true
	case PredicateOr:
		if len(pred.children) == 0 {
			return true
		}
		for _, child := range pred.children {
			if pruneSegmentCandidate(meta, child) {
				return true
			}
		}
		return false
	case PredicateNot:
		return true
	default:
		return pruneSegmentLeafCandidate(meta, pred)
	}
}

func pruneSegmentLeafCandidate(meta SegmentMeta, pred boundNode) bool {
	if pred.colIndex < 0 || pred.colIndex >= len(meta.Columns) {
		return true
	}
	col := meta.Columns[pred.colIndex]
	if col.AllNull {
		return false
	}
	if col.Bool != nil && !pruneBoolCandidate(*col.Bool, pred) {
		return false
	}
	if col.Int64 != nil && !pruneInt64RangeCandidate(*col.Int64, pred) {
		return false
	}
	if col.Int32 != nil && !pruneInt64RangeCandidate(Int64Stats{Min: int64(col.Int32.Min), Max: int64(col.Int32.Max)}, pred) {
		return false
	}
	if col.Text != nil && !pruneTextSegmentCandidate(*col.Text, pred) {
		return false
	}
	if col.UUID != nil && !pruneUUIDSegmentCandidate(*col.UUID, pred) {
		return false
	}
	return true
}

func prunePageLeafCandidate(meta SegmentMeta, pageIndex int, pred boundNode) bool {
	if pred.colIndex < 0 || pred.colIndex >= len(meta.Columns) {
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
	if page.Bool != nil && !pruneBoolCandidate(*page.Bool, pred) {
		return false
	}
	if page.Int64 != nil && !pruneInt64RangeCandidate(*page.Int64, pred) {
		return false
	}
	if page.Int64Values != nil && !pruneInt64ValueCandidate(*page.Int64Values, pred) {
		return false
	}
	if page.Int32 != nil && !pruneInt64RangeCandidate(Int64Stats{Min: int64(page.Int32.Min), Max: int64(page.Int32.Max)}, pred) {
		return false
	}
	if page.Int32Values != nil && !pruneInt32ValueCandidate(*page.Int32Values, pred) {
		return false
	}
	if page.Text != nil && !pruneTextPageCandidate(*page.Text, pred) {
		return false
	}
	if page.UUID != nil && !pruneUUIDPageCandidate(*page.UUID, pred) {
		return false
	}
	return true
}

func pruneBoolCandidate(stats BoolStats, pred boundNode) bool {
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

func pruneInt64RangeCandidate(stats Int64Stats, pred boundNode) bool {
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

func pruneInt64ValueCandidate(stats Int64ValueStats, pred boundNode) bool {
	if stats.Truncated || len(stats.Values) == 0 {
		return true
	}
	switch pred.op {
	case PredicateOpEq:
		return slices.Contains(stats.Values, pred.int64Value)
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

func pruneInt32ValueCandidate(stats Int32ValueStats, pred boundNode) bool {
	if stats.Truncated || len(stats.Values) == 0 {
		return true
	}
	switch pred.op {
	case PredicateOpEq:
		value, ok := int64ToInt32Value(pred.int64Value)
		return ok && slices.Contains(stats.Values, value)
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

func pruneTextPageCandidate(stats TextStats, pred boundNode) bool {
	if stats.Truncated {
		switch pred.op {
		case PredicateOpEq:
			if len(stats.HashBloom) != 0 {
				return textPageHashBloomHas(stats.HashBloom, pred.textValue)
			}
			return true
		case PredicateOpIn:
			if len(stats.HashBloom) != 0 {
				return pred.textSet.AnyInBloom(stats.HashBloom, textPageBloomProbes)
			}
			return true
		default:
			return true
		}
	}
	switch pred.op {
	case PredicateOpEq:
		return slices.Contains(stats.Values, pred.textValue)
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

func pruneTextSegmentCandidate(stats TextStats, pred boundNode) bool {
	if stats.Truncated && len(stats.HashBloom) != 0 {
		switch pred.op {
		case PredicateOpEq:
			return textSegmentHashBloomHas(stats.HashBloom, pred.textValue)
		case PredicateOpIn:
			return pred.textSet.AnyInBloom(stats.HashBloom, textSegmentBloomProbes)
		default:
			return true
		}
	}
	return pruneTextPageCandidate(stats, pred)
}

func pruneUUIDPageCandidate(stats UUIDStats, pred boundNode) bool {
	switch pred.op {
	case PredicateOpEq:
		return uuidPageHashBloomHas(stats.HashBloom, pred.uuidValue)
	case PredicateOpIn:
		return pred.uuidSet.AnyInBloom(stats.HashBloom, textPageBloomProbes)
	default:
		return true
	}
}

func pruneUUIDSegmentCandidate(stats UUIDStats, pred boundNode) bool {
	switch pred.op {
	case PredicateOpEq:
		return uuidSegmentHashBloomHas(stats.HashBloom, pred.uuidValue)
	case PredicateOpIn:
		return pred.uuidSet.AnyInBloom(stats.HashBloom, textSegmentBloomProbes)
	default:
		return true
	}
}

func boolStatsHas(stats BoolStats, value bool) bool {
	if value {
		return stats.HasTrue
	}
	return stats.HasFalse
}

func int64ToInt32Value(value int64) (int32, bool) {
	if value < minInt32Value || value > maxInt32Value {
		return 0, false
	}
	return int32(value), true
}
