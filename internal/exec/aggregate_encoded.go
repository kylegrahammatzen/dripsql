package exec

import (
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func sumInt64VectorSelected(v types.Vec, sel types.SelectionMask, base int64) (int64, int64, bool) {
	switch v.Encoding {
	case types.EncodingFORBitPack:
		return sumFORBitPackInt64Selected(v, sel, base)
	case types.EncodingConstant:
		return sumConstantInt64Selected(v, sel, base)
	default:
		return sumInt64Selected(v.I64, v.Valid, sel, base)
	}
}

func sumInt32VectorSelected(v types.Vec, sel types.SelectionMask) (int64, int64) {
	switch v.Encoding {
	case types.EncodingFORBitPack:
		return sumFORBitPackInt32Selected(v, sel)
	case types.EncodingConstant:
		return sumConstantInt32Selected(v, sel)
	default:
		return sumInt32Selected(v.I32, v.Valid, sel)
	}
}

func minMaxInt64VectorSelected(v types.Vec, sel types.SelectionMask, min bool) (int64, bool) {
	switch v.Encoding {
	case types.EncodingFORBitPack:
		return minMaxFORBitPackInt64Selected(v, sel, min)
	case types.EncodingConstant:
		if v.Encoded == nil || !v.Encoded.ConstantValid || countValidSelected(v.Valid, sel) == 0 {
			return 0, false
		}
		return v.Encoded.ConstantI64, true
	default:
		return minMaxInt64Selected(v.I64, v.Valid, sel, min)
	}
}

func sumConstantInt64Selected(v types.Vec, sel types.SelectionMask, base int64) (int64, int64, bool) {
	if v.Encoded == nil || !v.Encoded.ConstantValid {
		return base, 0, false
	}
	count := countValidSelected(v.Valid, sel)
	sum := base
	value := v.Encoded.ConstantI64
	for range count {
		next, ok := AddInt64(sum, value)
		if !ok {
			return 0, 0, true
		}
		sum = next
	}
	return sum, count, false
}

func sumConstantInt32Selected(v types.Vec, sel types.SelectionMask) (int64, int64) {
	if v.Encoded == nil || !v.Encoded.ConstantValid {
		return 0, 0
	}
	count := countValidSelected(v.Valid, sel)
	return v.Encoded.ConstantI64 * count, count
}

func sumFORBitPackInt64Selected(v types.Vec, sel types.SelectionMask, base int64) (int64, int64, bool) {
	sum := base
	var count int64
	var overflow bool
	forEachFORBitPackSelected(v, sel, func(value int64) {
		if overflow {
			return
		}
		next, ok := AddInt64(sum, value)
		if !ok {
			overflow = true
			return
		}
		sum = next
		count++
	})
	if overflow {
		return 0, 0, true
	}
	return sum, count, false
}

func sumFORBitPackInt32Selected(v types.Vec, sel types.SelectionMask) (int64, int64) {
	var sum int64
	var count int64
	forEachFORBitPackSelected(v, sel, func(value int64) {
		sum += value
		count++
	})
	return sum, count
}

func minMaxFORBitPackInt64Selected(v types.Vec, sel types.SelectionMask, min bool) (int64, bool) {
	var out int64
	set := false
	forEachFORBitPackSelected(v, sel, func(value int64) {
		if !set || (min && value < out) || (!min && value > out) {
			out = value
			set = true
		}
	})
	return out, set
}

func forEachFORBitPackSelected(v types.Vec, sel types.SelectionMask, visit func(int64)) {
	if v.Encoded == nil {
		return
	}
	enc := v.Encoded
	if selectionAll(sel) {
		for row := 0; row < sel.Rows; row++ {
			if types.IsValid(v.Valid, row) {
				visit(enc.FORValue(row))
			}
		}
		return
	}
	sel.IterSet(func(row int) {
		if types.IsValid(v.Valid, row) {
			visit(enc.FORValue(row))
		}
	})
}
