package storage

import "slices"

const (
	minInt32Value int64 = -1 << 31
	maxInt32Value int64 = 1<<31 - 1
)

type PredicatePrunePlan struct {
	meta SegmentMeta
	root BoundPredicate
}

type pruneStatsScope struct {
	allNull     bool
	boolStats   *BoolStats
	int64Stats  *Int64Stats
	int32Stats  *Int32Stats
	int64Vals   *Int64ValueStats
	int32Vals   *Int32ValueStats
	textStats   *TextStats
	uuidStats   *UUIDStats
	pruneText   func(TextStats, BoundPredicate) bool
	pruneUUID   func(UUIDStats, BoundPredicate) bool
	pruneInt64V func(Int64ValueStats, BoundPredicate) bool
	pruneInt32V func(Int32ValueStats, BoundPredicate) bool
}

func BindPrunePredicate(pred Predicate, meta SegmentMeta) PredicatePrunePlan {
	return PredicatePrunePlan{meta: meta, root: bindPruneNode(pred, meta)}
}

func (p PredicatePrunePlan) PageCandidate(pageIndex int) bool {
	return pruneCandidate(p.root, func(pred BoundPredicate) bool {
		return prunePageLeafCandidate(p.meta, pageIndex, pred)
	})
}

func (p PredicatePrunePlan) SegmentCandidate() bool {
	return pruneCandidate(p.root, func(pred BoundPredicate) bool {
		return pruneSegmentLeafCandidate(p.meta, pred)
	})
}

func bindPruneNode(pred Predicate, meta SegmentMeta) BoundPredicate {
	node := newBoundLeaf(pred)
	switch pred.Op {
	case PredicateNone:
		return node
	case PredicateAnd, PredicateOr, PredicateNot:
		node.children = make([]BoundPredicate, len(pred.Children))
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

func pruneCandidate(pred BoundPredicate, leaf func(BoundPredicate) bool) bool {
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

func pruneSegmentLeafCandidate(meta SegmentMeta, pred BoundPredicate) bool {
	if pred.colIndex < 0 || pred.colIndex >= len(meta.Columns) {
		return true
	}
	col := meta.Columns[pred.colIndex]
	// Eager checks first so pruned segments never trigger LoadColumn.
	if col.AllNull {
		return false
	}
	if col.Stats.Bool != nil && !pruneBoolCandidate(*col.Stats.Bool, pred) {
		return false
	}
	if col.Stats.Int64 != nil && !pruneInt64RangeCandidate(*col.Stats.Int64, pred) {
		return false
	}
	if col.Stats.Int32 != nil && !pruneInt64RangeCandidate(Int64Stats{Min: int64(col.Stats.Int32.Min), Max: int64(col.Stats.Int32.Max)}, pred) {
		return false
	}
	// Heavy stats may give tighter pruning. Conservatively keep the segment
	// if the load fails so a scan can settle correctness.
	if err := meta.LoadColumn(pred.colIndex); err != nil {
		return true
	}
	col = meta.Columns[pred.colIndex]
	if col.Stats.Int64Values != nil && !pruneInt64ValueSegmentCandidate(*col.Stats.Int64Values, pred) {
		return false
	}
	if col.Stats.Int32Values != nil && !pruneInt32ValueSegmentCandidate(*col.Stats.Int32Values, pred) {
		return false
	}
	if col.Stats.Text != nil && !pruneTextSegmentCandidate(*col.Stats.Text, pred) {
		return false
	}
	if col.Stats.UUID != nil && !pruneUUIDSegmentCandidate(*col.Stats.UUID, pred) {
		return false
	}
	return true
}

func prunePageLeafCandidate(meta SegmentMeta, pageIndex int, pred BoundPredicate) bool {
	if pred.colIndex < 0 || pred.colIndex >= len(meta.Columns) {
		return true
	}
	if err := meta.LoadColumn(pred.colIndex); err != nil {
		return true
	}
	col := meta.Columns[pred.colIndex]
	if pageIndex < 0 || pageIndex >= len(col.Pages) {
		return true
	}
	page := col.Pages[pageIndex]
	return pruneStatsCandidate(pruneStatsScope{
		allNull:     page.AllNull,
		boolStats:   page.Bool,
		int64Stats:  page.Int64,
		int32Stats:  page.Int32,
		int64Vals:   page.Int64Values,
		int32Vals:   page.Int32Values,
		textStats:   page.Text,
		uuidStats:   page.UUID,
		pruneText:   pruneTextPageCandidate,
		pruneUUID:   pruneUUIDPageCandidate,
		pruneInt64V: pruneInt64ValuePageCandidate,
		pruneInt32V: pruneInt32ValuePageCandidate,
	}, pred)
}

func pruneStatsCandidate(stats pruneStatsScope, pred BoundPredicate) bool {
	if stats.allNull {
		return false
	}
	if stats.boolStats != nil && !pruneBoolCandidate(*stats.boolStats, pred) {
		return false
	}
	if stats.int64Stats != nil && !pruneInt64RangeCandidate(*stats.int64Stats, pred) {
		return false
	}
	if stats.int64Vals != nil && stats.pruneInt64V != nil && !stats.pruneInt64V(*stats.int64Vals, pred) {
		return false
	}
	if stats.int32Stats != nil && !pruneInt64RangeCandidate(Int64Stats{Min: int64(stats.int32Stats.Min), Max: int64(stats.int32Stats.Max)}, pred) {
		return false
	}
	if stats.int32Vals != nil && stats.pruneInt32V != nil && !stats.pruneInt32V(*stats.int32Vals, pred) {
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

func pruneBoolCandidate(stats BoolStats, pred BoundPredicate) bool {
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

func pruneInt64RangeCandidate(stats Int64Stats, pred BoundPredicate) bool {
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
		return anyIntBetween(pred.intSet, stats.Min, stats.Max)
	default:
		return true
	}
}

func pruneInt64ValuePageCandidate(stats Int64ValueStats, pred BoundPredicate) bool {
	if stats.Truncated {
		return pruneInt64ValueBloom(stats.HashBloom, pred, textPageBloomProbes)
	}
	if len(stats.Values) == 0 {
		return true
	}
	switch pred.op {
	case PredicateOpEq:
		return slices.Contains(stats.Values, pred.int64Value)
	case PredicateOpIn:
		return slices.ContainsFunc(stats.Values, pred.intSet.Has)
	default:
		return true
	}
}

func pruneInt64ValueSegmentCandidate(stats Int64ValueStats, pred BoundPredicate) bool {
	if stats.Truncated {
		return pruneInt64ValueBloom(stats.HashBloom, pred, textSegmentBloomProbes)
	}
	if len(stats.Values) == 0 {
		return true
	}
	switch pred.op {
	case PredicateOpEq:
		return slices.Contains(stats.Values, pred.int64Value)
	case PredicateOpIn:
		return slices.ContainsFunc(stats.Values, pred.intSet.Has)
	default:
		return true
	}
}

func pruneInt32ValuePageCandidate(stats Int32ValueStats, pred BoundPredicate) bool {
	if stats.Truncated {
		return pruneInt32ValueBloom(stats.HashBloom, pred, textPageBloomProbes)
	}
	if len(stats.Values) == 0 {
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

func pruneInt32ValueSegmentCandidate(stats Int32ValueStats, pred BoundPredicate) bool {
	if stats.Truncated {
		return pruneInt32ValueBloom(stats.HashBloom, pred, textSegmentBloomProbes)
	}
	if len(stats.Values) == 0 {
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

func pruneInt64ValueBloom(bloom []uint64, pred BoundPredicate, probes uint64) bool {
	if len(bloom) == 0 {
		return true
	}
	switch pred.op {
	case PredicateOpEq:
		return hashBloomHas(bloom, intHash32(pred.int64Value), probes)
	case PredicateOpIn:
		hit := false
		pred.intSet.Each(func(value int64) bool {
			if hashBloomHas(bloom, intHash32(value), probes) {
				hit = true
				return false
			}
			return true
		})
		return hit
	default:
		return true
	}
}

func pruneInt32ValueBloom(bloom []uint64, pred BoundPredicate, probes uint64) bool {
	if len(bloom) == 0 {
		return true
	}
	switch pred.op {
	case PredicateOpEq:
		value, ok := int64ToInt32Value(pred.int64Value)
		if !ok {
			return false
		}
		return hashBloomHas(bloom, intHash32(int64(value)), probes)
	case PredicateOpIn:
		hit := false
		pred.intSet.Each(func(value int64) bool {
			scoped, ok := int64ToInt32Value(value)
			if !ok {
				return true
			}
			if hashBloomHas(bloom, intHash32(int64(scoped)), probes) {
				hit = true
				return false
			}
			return true
		})
		return hit
	default:
		return true
	}
}

func pruneTextPageCandidate(stats TextStats, pred BoundPredicate) bool {
	if stats.Truncated {
		switch pred.op {
		case PredicateOpEq:
			if len(stats.HashBloom) != 0 {
				return textPageHashBloomHas(stats.HashBloom, pred.textValue)
			}
			return true
		case PredicateOpIn:
			if len(stats.HashBloom) != 0 {
				return anyTextInBloom(pred.textSet, stats.HashBloom, textPageBloomProbes)
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
		return slices.ContainsFunc(stats.Values, pred.textSet.Has)
	default:
		return true
	}
}

func pruneTextSegmentCandidate(stats TextStats, pred BoundPredicate) bool {
	if stats.Truncated && len(stats.HashBloom) != 0 {
		switch pred.op {
		case PredicateOpEq:
			return textSegmentHashBloomHas(stats.HashBloom, pred.textValue)
		case PredicateOpIn:
			return anyTextInBloom(pred.textSet, stats.HashBloom, textSegmentBloomProbes)
		default:
			return true
		}
	}
	return pruneTextPageCandidate(stats, pred)
}

func pruneUUIDPageCandidate(stats UUIDStats, pred BoundPredicate) bool {
	switch pred.op {
	case PredicateOpEq:
		return uuidPageHashBloomHas(stats.HashBloom, pred.uuidValue)
	case PredicateOpIn:
		return anyUUIDInBloom(pred.uuidSet, stats.HashBloom, textPageBloomProbes)
	default:
		return true
	}
}

func pruneUUIDSegmentCandidate(stats UUIDStats, pred BoundPredicate) bool {
	switch pred.op {
	case PredicateOpEq:
		return uuidSegmentHashBloomHas(stats.HashBloom, pred.uuidValue)
	case PredicateOpIn:
		return anyUUIDInBloom(pred.uuidSet, stats.HashBloom, textSegmentBloomProbes)
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
