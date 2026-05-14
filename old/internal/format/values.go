package format

import (
	"fmt"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

const (
	secondsPerDay   = int64(24 * time.Hour / time.Second)
	timestampLayout = "2006-01-02T15:04:05.000000000Z"
)

// RowValue returns the display value at row in col, or nil for invalid rows.
// It is the canonical conversion from a columnar Vec cell to an interface{}
// suitable for engine result rows.
func RowValue(col types.Column, row int) (any, error) {
	if !types.IsValid(col.V.Valid, row) {
		return nil, nil
	}
	switch col.V.Kind {
	case types.VecBool:
		return col.V.BoolBits[row>>6]&(uint64(1)<<uint(row&63)) != 0, nil
	case types.VecInt16:
		return col.V.I16[row], nil
	case types.VecInt32:
		return col.V.I32[row], nil
	case types.VecDate:
		return DateDays(col.V.I32[row]), nil
	case types.VecInt64, types.VecDecimal64, types.VecTime:
		return col.V.I64[row], nil
	case types.VecTimestamp:
		return TimestampNanos(col.V.I64[row]), nil
	case types.VecFloat32:
		return col.V.F32[row], nil
	case types.VecFloat64:
		return col.V.F64[row], nil
	case types.VecText, types.VecBytes, types.VecJSON:
		value, ok := col.V.TextCopy(row)
		if !ok {
			return nil, fmt.Errorf("column %q has unsupported text encoding %s", col.Name, col.V.Encoding)
		}
		return value, nil
	case types.VecUUID:
		return types.FormatUUID(col.V.UUID[row]), nil
	case types.VecEnum32:
		label, ok := EnumLabel(col.V.U32[row], col.EnumLabels)
		if !ok {
			return nil, fmt.Errorf("column %q has invalid enum code %d", col.Name, col.V.U32[row])
		}
		return label, nil
	default:
		return nil, fmt.Errorf("column %q has unsupported vector kind %s", col.Name, col.V.Kind)
	}
}

// EnumLabel returns the user-facing label for a 1-based enum code. Code 0 is
// reserved for "unset"; codes beyond len(labels) are invalid.
func EnumLabel(code uint32, labels []string) (string, bool) {
	if code == 0 || int(code) > len(labels) {
		return "", false
	}
	return labels[code-1], true
}

func DateDays(days int32) string {
	return time.Unix(int64(days)*secondsPerDay, 0).UTC().Format("2006-01-02")
}

func TimestampNanos(nanos int64) string {
	return time.Unix(0, nanos).UTC().Format(timestampLayout)
}
