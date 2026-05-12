package storage

import (
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func evalFORBitPackLeafBound(v types.Vec, pred boundNode, input *types.SelectionMask, out *types.SelectionMask) int {
	switch pred.op {
	case PredicateOpEq:
		return evalFORBitPackEq(v, pred.int64Value, false, input, out)
	case PredicateOpNotEq:
		return evalFORBitPackEq(v, pred.int64Value, true, input, out)
	case PredicateOpLess:
		return evalFORBitPackCompare(v, pred.int64Value, func(value, want int64) bool { return value < want }, input, out)
	case PredicateOpLessEqual:
		return evalFORBitPackCompare(v, pred.int64Value, func(value, want int64) bool { return value <= want }, input, out)
	case PredicateOpGreater:
		return evalFORBitPackCompare(v, pred.int64Value, func(value, want int64) bool { return value > want }, input, out)
	case PredicateOpGreaterEqual:
		return evalFORBitPackCompare(v, pred.int64Value, func(value, want int64) bool { return value >= want }, input, out)
	case PredicateOpBetween:
		return evalFORBitPackBetween(v, pred.lo, pred.hi, input, out)
	case PredicateOpIn:
		return evalFORBitPackIn(v, pred.intSet, false, input, out)
	case PredicateOpNotIn:
		return evalFORBitPackIn(v, pred.intSet, true, input, out)
	default:
		return 0
	}
}

func evalFORBitPackEq(v types.Vec, want int64, invert bool, input *types.SelectionMask, out *types.SelectionMask) int {
	wantOffset, ok := v.Encoded.FORTargetOffset(want)
	if !ok {
		if invert {
			return selectValidRows(v.Valid, v.Len, input, out)
		}
		return 0
	}
	enc := v.Encoded
	matched := 0
	if input == nil {
		if v.Valid == nil {
			for row := 0; row < v.Len; row++ {
				if enc.FOROffsetMatches(row, wantOffset) != invert {
					out.SetUnsafe(row)
					matched++
				}
			}
			return matched
		}
		for row := 0; row < v.Len; row++ {
			if types.IsValid(v.Valid, row) && enc.FOROffsetMatches(row, wantOffset) != invert {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	if v.Valid == nil {
		input.IterSet(func(row int) {
			if enc.FOROffsetMatches(row, wantOffset) != invert {
				out.SetUnsafe(row)
				matched++
			}
		})
		return matched
	}
	input.IterSet(func(row int) {
		if types.IsValid(v.Valid, row) && enc.FOROffsetMatches(row, wantOffset) != invert {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalFORBitPackCompare(v types.Vec, want int64, match func(value int64, want int64) bool, input *types.SelectionMask, out *types.SelectionMask) int {
	enc := v.Encoded
	matched := 0
	if input == nil {
		if v.Valid == nil {
			for row := 0; row < v.Len; row++ {
				if match(enc.FORValue(row), want) {
					out.SetUnsafe(row)
					matched++
				}
			}
			return matched
		}
		for row := 0; row < v.Len; row++ {
			if types.IsValid(v.Valid, row) && match(enc.FORValue(row), want) {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	if v.Valid == nil {
		input.IterSet(func(row int) {
			if match(enc.FORValue(row), want) {
				out.SetUnsafe(row)
				matched++
			}
		})
		return matched
	}
	input.IterSet(func(row int) {
		if types.IsValid(v.Valid, row) && match(enc.FORValue(row), want) {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalFORBitPackBetween(v types.Vec, lo int64, hi int64, input *types.SelectionMask, out *types.SelectionMask) int {
	enc := v.Encoded
	matched := 0
	if input == nil {
		if v.Valid == nil {
			for row := 0; row < v.Len; row++ {
				value := enc.FORValue(row)
				if lo <= value && value <= hi {
					out.SetUnsafe(row)
					matched++
				}
			}
			return matched
		}
		for row := 0; row < v.Len; row++ {
			value := enc.FORValue(row)
			if types.IsValid(v.Valid, row) && lo <= value && value <= hi {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	if v.Valid == nil {
		input.IterSet(func(row int) {
			value := enc.FORValue(row)
			if lo <= value && value <= hi {
				out.SetUnsafe(row)
				matched++
			}
		})
		return matched
	}
	input.IterSet(func(row int) {
		value := enc.FORValue(row)
		if types.IsValid(v.Valid, row) && lo <= value && value <= hi {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalFORBitPackIn(v types.Vec, matcher int64Matcher, invert bool, input *types.SelectionMask, out *types.SelectionMask) int {
	enc := v.Encoded
	matched := 0
	if input == nil {
		if v.Valid == nil {
			for row := 0; row < v.Len; row++ {
				if matcher.Has(enc.FORValue(row)) != invert {
					out.SetUnsafe(row)
					matched++
				}
			}
			return matched
		}
		for row := 0; row < v.Len; row++ {
			if types.IsValid(v.Valid, row) && matcher.Has(enc.FORValue(row)) != invert {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	if v.Valid == nil {
		input.IterSet(func(row int) {
			if matcher.Has(enc.FORValue(row)) != invert {
				out.SetUnsafe(row)
				matched++
			}
		})
		return matched
	}
	input.IterSet(func(row int) {
		if types.IsValid(v.Valid, row) && matcher.Has(enc.FORValue(row)) != invert {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

