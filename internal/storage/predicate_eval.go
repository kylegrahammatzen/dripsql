package storage

import (
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func evalLeafInto(batch types.Batch, pred boundNode, mask *types.SelectionMask) (int, error) {
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
	case types.VecUUID:
		return evalUUIDLeafBound(col.V, pred, nil, mask), nil
	default:
		return 0, fmt.Errorf("predicate unsupported vector kind %s", col.V.Kind)
	}
}

func evalLeafSelectedInto(batch types.Batch, pred boundNode, input types.SelectionMask, mask *types.SelectionMask) (int, error) {
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
	case types.VecUUID:
		return evalUUIDLeafBound(col.V, pred, &input, mask), nil
	default:
		return 0, fmt.Errorf("predicate unsupported vector kind %s", col.V.Kind)
	}
}

func evalUUIDLeafBound(v types.Vec, pred boundNode, input *types.SelectionMask, out *types.SelectionMask) int {
	switch pred.op {
	case PredicateOpEq:
		return evalUUIDEq(v.UUID, v.Valid, v.Len, pred.uuidValue, input, out)
	case PredicateOpNotEq:
		return evalUUIDNotEq(v.UUID, v.Valid, v.Len, pred.uuidValue, input, out)
	case PredicateOpIn:
		return evalUUIDIn(v.UUID, v.Valid, v.Len, pred.uuidSet, false, input, out)
	case PredicateOpNotIn:
		return evalUUIDIn(v.UUID, v.Valid, v.Len, pred.uuidSet, true, input, out)
	default:
		return 0
	}
}

func evalUUIDEq(values []types.UUID16, valid types.Validity, rows int, want types.UUID16, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < rows; row++ {
			if types.IsValid(valid, row) && values[row] == want {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		if types.IsValid(valid, row) && values[row] == want {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalUUIDNotEq(values []types.UUID16, valid types.Validity, rows int, want types.UUID16, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < rows; row++ {
			if types.IsValid(valid, row) && values[row] != want {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		if types.IsValid(valid, row) && values[row] != want {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalUUIDIn(values []types.UUID16, valid types.Validity, rows int, matcher uuidMatcher, invert bool, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < rows; row++ {
			if !types.IsValid(valid, row) {
				continue
			}
			if matcher.Has(values[row]) != invert {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		if !types.IsValid(valid, row) {
			return
		}
		if matcher.Has(values[row]) != invert {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalBoolLeafBound(v types.Vec, pred boundNode, input *types.SelectionMask, out *types.SelectionMask) int {
	switch pred.op {
	case PredicateOpEq:
		return evalBoolEq(v, pred.boolValue, input, out)
	case PredicateOpNotEq:
		return evalBoolEq(v, !pred.boolValue, input, out)
	case PredicateOpIn:
		return evalBoolIn(v, pred.boolSet, false, input, out)
	case PredicateOpNotIn:
		return evalBoolIn(v, pred.boolSet, true, input, out)
	default:
		return 0
	}
}

func evalBoolEq(v types.Vec, want bool, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < v.Len; row++ {
			if !types.IsValid(v.Valid, row) {
				continue
			}
			value := v.BoolBits[row>>6]&(uint64(1)<<uint(row&63)) != 0
			if value == want {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		if !types.IsValid(v.Valid, row) {
			return
		}
		value := v.BoolBits[row>>6]&(uint64(1)<<uint(row&63)) != 0
		if value == want {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalBoolIn(v types.Vec, matcher boolMatcher, invert bool, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < v.Len; row++ {
			if !types.IsValid(v.Valid, row) {
				continue
			}
			value := v.BoolBits[row>>6]&(uint64(1)<<uint(row&63)) != 0
			if matcher.Has(value) != invert {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		if !types.IsValid(v.Valid, row) {
			return
		}
		value := v.BoolBits[row>>6]&(uint64(1)<<uint(row&63)) != 0
		if matcher.Has(value) != invert {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalIntLeafBound[T ~int16 | ~int32 | ~int64](v types.Vec, values []T, pred boundNode, input *types.SelectionMask, out *types.SelectionMask) int {
	if v.Encoding == types.EncodingFORBitPack {
		return evalFORBitPackLeafBound(v, pred, input, out)
	}
	return evalFlatIntLeafBound(values, v.Valid, v.Len, pred, input, out)
}

func evalFlatIntLeafBound[T ~int16 | ~int32 | ~int64](values []T, valid types.Validity, rows int, pred boundNode, input *types.SelectionMask, out *types.SelectionMask) int {
	if valid == nil {
		return evalFlatIntLeafBoundAllValid(values, rows, pred, input, out)
	}
	switch pred.op {
	case PredicateOpEq:
		return evalIntEq(values, valid, rows, pred.int64Value, input, out)
	case PredicateOpNotEq:
		return evalIntNotEq(values, valid, rows, pred.int64Value, input, out)
	case PredicateOpLess:
		return evalIntLess(values, valid, rows, pred.int64Value, input, out)
	case PredicateOpLessEqual:
		return evalIntLessEqual(values, valid, rows, pred.int64Value, input, out)
	case PredicateOpGreater:
		return evalIntGreater(values, valid, rows, pred.int64Value, input, out)
	case PredicateOpGreaterEqual:
		return evalIntGreaterEqual(values, valid, rows, pred.int64Value, input, out)
	case PredicateOpBetween:
		return evalIntBetween(values, valid, rows, pred.lo, pred.hi, input, out)
	case PredicateOpIn:
		return evalIntIn(values, valid, rows, pred.intSet, false, input, out)
	case PredicateOpNotIn:
		return evalIntIn(values, valid, rows, pred.intSet, true, input, out)
	default:
		return 0
	}
}

func evalFlatIntLeafBoundAllValid[T ~int16 | ~int32 | ~int64](values []T, rows int, pred boundNode, input *types.SelectionMask, out *types.SelectionMask) int {
	switch pred.op {
	case PredicateOpEq:
		return evalIntEqAllValid(values, rows, pred.int64Value, input, out)
	case PredicateOpNotEq:
		return evalIntNotEqAllValid(values, rows, pred.int64Value, input, out)
	case PredicateOpLess:
		return evalIntLessAllValid(values, rows, pred.int64Value, input, out)
	case PredicateOpLessEqual:
		return evalIntLessEqualAllValid(values, rows, pred.int64Value, input, out)
	case PredicateOpGreater:
		return evalIntGreaterAllValid(values, rows, pred.int64Value, input, out)
	case PredicateOpGreaterEqual:
		return evalIntGreaterEqualAllValid(values, rows, pred.int64Value, input, out)
	case PredicateOpBetween:
		return evalIntBetweenAllValid(values, rows, pred.lo, pred.hi, input, out)
	case PredicateOpIn:
		return evalIntInAllValid(values, rows, pred.intSet, false, input, out)
	case PredicateOpNotIn:
		return evalIntInAllValid(values, rows, pred.intSet, true, input, out)
	default:
		return 0
	}
}

func evalIntEqAllValid[T ~int16 | ~int32 | ~int64](values []T, rows int, want int64, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < rows; row++ {
			if int64(values[row]) == want {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		if int64(values[row]) == want {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalIntNotEqAllValid[T ~int16 | ~int32 | ~int64](values []T, rows int, want int64, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < rows; row++ {
			if int64(values[row]) != want {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		if int64(values[row]) != want {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalIntLessAllValid[T ~int16 | ~int32 | ~int64](values []T, rows int, want int64, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < rows; row++ {
			if int64(values[row]) < want {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		if int64(values[row]) < want {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalIntLessEqualAllValid[T ~int16 | ~int32 | ~int64](values []T, rows int, want int64, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < rows; row++ {
			if int64(values[row]) <= want {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		if int64(values[row]) <= want {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalIntGreaterAllValid[T ~int16 | ~int32 | ~int64](values []T, rows int, want int64, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < rows; row++ {
			if int64(values[row]) > want {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		if int64(values[row]) > want {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalIntGreaterEqualAllValid[T ~int16 | ~int32 | ~int64](values []T, rows int, want int64, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < rows; row++ {
			if int64(values[row]) >= want {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		if int64(values[row]) >= want {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalIntBetweenAllValid[T ~int16 | ~int32 | ~int64](values []T, rows int, lo int64, hi int64, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < rows; row++ {
			value := int64(values[row])
			if lo <= value && value <= hi {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		value := int64(values[row])
		if lo <= value && value <= hi {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalIntInAllValid[T ~int16 | ~int32 | ~int64](values []T, rows int, matcher int64Matcher, invert bool, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < rows; row++ {
			if matcher.Has(int64(values[row])) != invert {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		if matcher.Has(int64(values[row])) != invert {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalIntEq[T ~int16 | ~int32 | ~int64](values []T, valid types.Validity, rows int, want int64, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < rows; row++ {
			if types.IsValid(valid, row) && int64(values[row]) == want {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		if types.IsValid(valid, row) && int64(values[row]) == want {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalIntNotEq[T ~int16 | ~int32 | ~int64](values []T, valid types.Validity, rows int, want int64, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < rows; row++ {
			if types.IsValid(valid, row) && int64(values[row]) != want {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		if types.IsValid(valid, row) && int64(values[row]) != want {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalIntLess[T ~int16 | ~int32 | ~int64](values []T, valid types.Validity, rows int, want int64, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < rows; row++ {
			if types.IsValid(valid, row) && int64(values[row]) < want {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		if types.IsValid(valid, row) && int64(values[row]) < want {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalIntLessEqual[T ~int16 | ~int32 | ~int64](values []T, valid types.Validity, rows int, want int64, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < rows; row++ {
			if types.IsValid(valid, row) && int64(values[row]) <= want {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		if types.IsValid(valid, row) && int64(values[row]) <= want {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalIntGreater[T ~int16 | ~int32 | ~int64](values []T, valid types.Validity, rows int, want int64, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < rows; row++ {
			if types.IsValid(valid, row) && int64(values[row]) > want {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		if types.IsValid(valid, row) && int64(values[row]) > want {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalIntGreaterEqual[T ~int16 | ~int32 | ~int64](values []T, valid types.Validity, rows int, want int64, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < rows; row++ {
			if types.IsValid(valid, row) && int64(values[row]) >= want {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		if types.IsValid(valid, row) && int64(values[row]) >= want {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalIntBetween[T ~int16 | ~int32 | ~int64](values []T, valid types.Validity, rows int, lo int64, hi int64, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < rows; row++ {
			value := int64(values[row])
			if types.IsValid(valid, row) && lo <= value && value <= hi {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		value := int64(values[row])
		if types.IsValid(valid, row) && lo <= value && value <= hi {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalIntIn[T ~int16 | ~int32 | ~int64](values []T, valid types.Validity, rows int, matcher int64Matcher, invert bool, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < rows; row++ {
			if types.IsValid(valid, row) && matcher.Has(int64(values[row])) != invert {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		if types.IsValid(valid, row) && matcher.Has(int64(values[row])) != invert {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalTextLeafBound(v types.Vec, pred boundNode, input *types.SelectionMask, out *types.SelectionMask) (int, error) {
	switch v.Encoding {
	case types.EncodingFlat:
		return evalFlatTextLeafBound(v, pred, input, out), nil
	case types.EncodingDictionary:
		return evalDictTextLeafBound(v, pred, input, out)
	default:
		return 0, fmt.Errorf("predicate unsupported text encoding %s", v.Encoding)
	}
}

func evalFlatTextLeafBound(v types.Vec, pred boundNode, input *types.SelectionMask, out *types.SelectionMask) int {
	switch pred.op {
	case PredicateOpEq:
		return evalTextEq(v, pred.textValue, false, input, out)
	case PredicateOpNotEq:
		return evalTextEq(v, pred.textValue, true, input, out)
	case PredicateOpIn:
		return evalTextIn(v, pred.textSet, false, input, out)
	case PredicateOpNotIn:
		return evalTextIn(v, pred.textSet, true, input, out)
	default:
		return 0
	}
}

func evalTextEq(v types.Vec, want string, invert bool, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < v.Len; row++ {
			if types.IsValid(v.Valid, row) && (v.Var.String(row) == want) != invert {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		if types.IsValid(v.Valid, row) && (v.Var.String(row) == want) != invert {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalTextIn(v types.Vec, matcher textMatcher, invert bool, input *types.SelectionMask, out *types.SelectionMask) int {
	matched := 0
	if input == nil {
		for row := 0; row < v.Len; row++ {
			if types.IsValid(v.Valid, row) && matcher.Has(v.Var.String(row)) != invert {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched
	}
	input.IterSet(func(row int) {
		if types.IsValid(v.Valid, row) && matcher.Has(v.Var.String(row)) != invert {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched
}

func evalDictTextLeafBound(v types.Vec, pred boundNode, input *types.SelectionMask, out *types.SelectionMask) (int, error) {
	if len(v.DictIDs) < v.Len {
		return 0, fmt.Errorf("dictionary ids length %d is shorter than rows %d", len(v.DictIDs), v.Len)
	}
	var ids [256]bool
	wantPresent := true
	matchedIDs := 0
	switch pred.op {
	case PredicateOpEq, PredicateOpNotEq:
		id, ok := dictTextID(v.DictValues, pred.textValue)
		if ok {
			ids[id] = true
			matchedIDs = 1
		}
		wantPresent = pred.op == PredicateOpEq
	case PredicateOpIn, PredicateOpNotIn:
		for row := 0; row < v.DictValues.Rows(); row++ {
			if pred.textSet.Has(v.DictValues.String(row)) {
				ids[row] = true
				matchedIDs++
			}
		}
		wantPresent = pred.op == PredicateOpIn
	default:
		return 0, nil
	}
	if matchedIDs == 0 {
		if wantPresent {
			return 0, nil
		}
		return selectValidRows(v.Valid, v.Len, input, out), nil
	}
	matched := 0
	if input == nil {
		for row := 0; row < v.Len; row++ {
			if types.IsValid(v.Valid, row) && ids[v.DictIDs[row]] == wantPresent {
				out.SetUnsafe(row)
				matched++
			}
		}
		return matched, nil
	}
	input.IterSet(func(row int) {
		if types.IsValid(v.Valid, row) && ids[v.DictIDs[row]] == wantPresent {
			out.SetUnsafe(row)
			matched++
		}
	})
	return matched, nil
}
