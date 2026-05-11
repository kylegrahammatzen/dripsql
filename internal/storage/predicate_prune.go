package storage

import "slices"

const (
	minInt32Value int64 = -1 << 31
	maxInt32Value int64 = 1<<31 - 1
)

type PredicatePrunePlan struct {
	meta SegmentMeta
	root boundNode
}

type pruneStatsScope struct {
	allNull    bool
	boolStats  *BoolStats
	int64Stats *Int64Stats
	int32Stats *Int32Stats
	int64Vals  *Int64ValueStats
	int32Vals  *Int32ValueStats
	textStats  *TextStats
	uuidStats  *UUIDStats
	pruneText  func(TextStats, boundNode) bool
	pruneUUID  func(UUIDStats, boundNode) bool
}

func BindPrunePredicate(pred Predicate, meta SegmentMeta) PredicatePrunePlan {
	return PredicatePrunePlan{meta: meta, root: bindPruneNode(pred, meta)}
}

func (p PredicatePrunePlan) PageCandidate(pageIndex int) bool {
	return pruneCandidate(p.root, func(pred boundNode) bool {
		return prunePageLeafCandidate(p.meta, pageIndex, pred)
	})
}

func (p PredicatePrunePlan) SegmentCandidate() bool {
	return pruneCandidate(p.root, func(pred boundNode) bool {
		return pruneSegmentLeafCandidate(p.meta, pred)
	})
}

func bindPruneNode(pred Predicate, meta SegmentMeta) boundNode {
	node := newBoundNode(pred)
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

func pruneCandidate(pred boundNode, leaf func(boundNode) bool) bool {
	switch pred.op {
	case PredicateNone:
		return true
	case PredicateAnd:
		for _, child := range pred.children {
			if !pruneCandidate(child, leaf) {
				return false
			}
		}
		return true
	case PredicateOr:
		if len(pred.children) == 0 {
			return true
		}
		for _, child := range pred.children {
			if pruneCandidate(child, leaf) {
				return true
			}
		}
		return false
	case PredicateNot:
		return true
	default:
		return leaf(pred)
	}
}

func pruneSegmentLeafCandidate(meta SegmentMeta, pred boundNode) bool {
	if pred.colIndex < 0 || pred.colIndex >= len(meta.Columns) {
		return true
	}
	col := meta.Columns[pred.colIndex]
	return pruneStatsCandidate(pruneStatsScope{
		allNull:    col.AllNull,
		boolStats:  col.Bool,
		int64Stats: col.Int64,
		int32Stats: col.Int32,
		textStats:  col.Text,
		uuidStats:  col.UUID,
		pruneText:  pruneTextSegmentCandidate,
		pruneUUID:  pruneUUIDSegmentCandidate,
	}, pred)
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
	return pruneStatsCandidate(pruneStatsScope{
		allNull:    page.AllNull,
		boolStats:  page.Bool,
		int64Stats: page.Int64,
		int32Stats: page.Int32,
		int64Vals:  page.Int64Values,
		int32Vals:  page.Int32Values,
		textStats:  page.Text,
		uuidStats:  page.UUID,
		pruneText:  pruneTextPageCandidate,
		pruneUUID:  pruneUUIDPageCandidate,
	}, pred)
}

func pruneStatsCandidate(stats pruneStatsScope, pred boundNode) bool {
	if stats.allNull {
		return false
	}
	if stats.boolStats != nil && !pruneBoolCandidate(*stats.boolStats, pred) {
		return false
	}
	if stats.int64Stats != nil && !pruneInt64RangeCandidate(*stats.int64Stats, pred) {
		return false
	}
	if stats.int64Vals != nil && !pruneInt64ValueCandidate(*stats.int64Vals, pred) {
		return false
	}
	if stats.int32Stats != nil && !pruneInt64RangeCandidate(Int64Stats{Min: int64(stats.int32Stats.Min), Max: int64(stats.int32Stats.Max)}, pred) {
		return false
	}
	if stats.int32Vals != nil && !pruneInt32ValueCandidate(*stats.int32Vals, pred) {
		return false
	}
	if stats.textStats != nil && !stats.pruneText(*stats.textStats, pred) {
		return false
	}
	if stats.uuidStats != nil && !stats.pruneUUID(*stats.uuidStats, pred) {
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
