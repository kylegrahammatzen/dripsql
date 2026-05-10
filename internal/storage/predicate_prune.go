package storage

import "slices"

const (
	minInt32Value int64 = -1 << 31
	maxInt32Value int64 = 1<<31 - 1
)

func segmentPageCandidate(meta SegmentMeta, pageIndex int, pred Predicate) bool {
	switch pred.Op {
	case PredicateNone:
		return true
	case PredicateAnd:
		for _, child := range pred.Children {
			if !segmentPageCandidate(meta, pageIndex, child) {
				return false
			}
		}
		return true
	case PredicateOr:
		if len(pred.Children) == 0 {
			return true
		}
		for _, child := range pred.Children {
			if segmentPageCandidate(meta, pageIndex, child) {
				return true
			}
		}
		return false
	case PredicateNot:
		return true
	default:
		return leafPageCandidate(meta, pageIndex, pred)
	}
}

func leafPageCandidate(meta SegmentMeta, pageIndex int, pred Predicate) bool {
	page, ok := pageMetaByColumn(meta, pageIndex, pred.Column)
	if !ok || page.AllNull {
		return !ok
	}
	if page.Bool != nil && !boolPageCandidate(*page.Bool, pred) {
		return false
	}
	if page.Int64 != nil && !int64PageCandidate(*page.Int64, pred) {
		return false
	}
	if page.Int64Values != nil && !int64ValuePageCandidate(*page.Int64Values, pred) {
		return false
	}
	if page.Int32 != nil && !int64PageCandidate(Int64Stats{Min: int64(page.Int32.Min), Max: int64(page.Int32.Max)}, pred) {
		return false
	}
	if page.Int32Values != nil && !int32ValuePageCandidate(*page.Int32Values, pred) {
		return false
	}
	if page.Text != nil && !textPageCandidate(*page.Text, pred) {
		return false
	}
	return true
}

func pageMetaByColumn(meta SegmentMeta, pageIndex int, column string) (PageMeta, bool) {
	for _, col := range meta.Columns {
		if col.Name != column || pageIndex < 0 || pageIndex >= len(col.Pages) {
			continue
		}
		return col.Pages[pageIndex], true
	}
	return PageMeta{}, false
}

func boolPageCandidate(stats BoolStats, pred Predicate) bool {
	switch pred.Op {
	case PredicateOpEq:
		return boolStatsHas(stats, pred.Bool)
	case PredicateOpNotEq:
		return boolStatsHas(stats, !pred.Bool)
	case PredicateOpIn:
		for _, value := range pred.Bools {
			if boolStatsHas(stats, value) {
				return true
			}
		}
		return false
	case PredicateOpNotIn:
		return (stats.HasTrue && !slices.Contains(pred.Bools, true)) || (stats.HasFalse && !slices.Contains(pred.Bools, false))
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

func int64PageCandidate(stats Int64Stats, pred Predicate) bool {
	switch pred.Op {
	case PredicateOpEq:
		return stats.Min <= pred.Int64 && pred.Int64 <= stats.Max
	case PredicateOpLess:
		return stats.Min < pred.Int64
	case PredicateOpLessEqual:
		return stats.Min <= pred.Int64
	case PredicateOpGreater:
		return stats.Max > pred.Int64
	case PredicateOpGreaterEqual:
		return stats.Max >= pred.Int64
	case PredicateOpBetween:
		return pred.Lo <= stats.Max && stats.Min <= pred.Hi
	case PredicateOpIn:
		for _, value := range pred.Int64s {
			if stats.Min <= value && value <= stats.Max {
				return true
			}
		}
		return false
	default:
		return true
	}
}

func int64ValuePageCandidate(stats Int64ValueStats, pred Predicate) bool {
	if stats.Truncated || len(stats.Values) == 0 {
		return true
	}
	switch pred.Op {
	case PredicateOpEq:
		return slices.Contains(stats.Values, pred.Int64)
	case PredicateOpIn:
		for _, value := range pred.Int64s {
			if slices.Contains(stats.Values, value) {
				return true
			}
		}
		return false
	default:
		return true
	}
}

func int32ValuePageCandidate(stats Int32ValueStats, pred Predicate) bool {
	if stats.Truncated || len(stats.Values) == 0 {
		return true
	}
	switch pred.Op {
	case PredicateOpEq:
		value, ok := int64ToInt32Value(pred.Int64)
		return ok && slices.Contains(stats.Values, value)
	case PredicateOpIn:
		for _, value := range pred.Int64s {
			value32, ok := int64ToInt32Value(value)
			if ok && slices.Contains(stats.Values, value32) {
				return true
			}
		}
		return false
	default:
		return true
	}
}

func int64ToInt32Value(value int64) (int32, bool) {
	if value < minInt32Value || value > maxInt32Value {
		return 0, false
	}
	return int32(value), true
}

func textPageCandidate(stats TextStats, pred Predicate) bool {
	if stats.Truncated {
		switch pred.Op {
		case PredicateOpEq:
			return textHashSetHas(stats.Hashes, pred.Text)
		case PredicateOpIn:
			for _, value := range pred.Texts {
				if textHashSetHas(stats.Hashes, value) {
					return true
				}
			}
			return false
		default:
			return true
		}
	}
	switch pred.Op {
	case PredicateOpEq:
		return slices.Contains(stats.Values, pred.Text)
	case PredicateOpIn:
		for _, value := range pred.Texts {
			if slices.Contains(stats.Values, value) {
				return true
			}
		}
		return false
	default:
		return true
	}
}

func textHashSetHas(hashes []uint16, value string) bool {
	if len(hashes) == 0 {
		return true
	}
	_, ok := slices.BinarySearch(hashes, textHash16String(value))
	return ok
}
