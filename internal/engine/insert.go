package engine

import (
	"fmt"
	"math"
	"time"

	v3sql "github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

var (
	minTimestampLiteral = time.Unix(0, math.MinInt64).UTC()
	maxTimestampLiteral = time.Unix(0, math.MaxInt64).UTC()
)

func batchFromInsert(values v3sql.InsertValues) (types.Batch, error) {
	cols := make([]types.Column, 0, len(values.Columns))
	for _, col := range values.Columns {
		valid := validityFromValues(col.Values, col.NullCount)
		switch col.Type.Kind {
		case types.KindBool:
			bits := make([]uint64, types.ValidityWords(len(col.Values)))
			for i, value := range col.Values {
				if value.Kind != v3sql.ValueNull && value.Bool {
					bits[i>>6] |= uint64(1) << uint(i&63)
				}
			}
			cols = append(cols, types.Column{Name: col.Name, Type: col.Type, V: types.Vec{Kind: types.VecBool, Encoding: types.EncodingFlat, Len: len(col.Values), Valid: valid, BoolBits: bits}})
		case types.KindInt16:
			out := make([]int16, len(col.Values))
			for i, value := range col.Values {
				if value.Kind != v3sql.ValueNull {
					out[i] = int16(value.Int)
				}
			}
			cols = append(cols, types.Column{Name: col.Name, Type: col.Type, V: types.Vec{Kind: types.VecInt16, Encoding: types.EncodingFlat, Len: len(out), Valid: valid, I16: out}})
		case types.KindInt32:
			out := make([]int32, len(col.Values))
			for i, value := range col.Values {
				if value.Kind != v3sql.ValueNull {
					out[i] = int32(value.Int)
				}
			}
			cols = append(cols, types.Column{Name: col.Name, Type: col.Type, V: types.Vec{Kind: types.VecInt32, Encoding: types.EncodingFlat, Len: len(out), Valid: valid, I32: out}})
		case types.KindInt64:
			out := make([]int64, len(col.Values))
			for i, value := range col.Values {
				if value.Kind != v3sql.ValueNull {
					out[i] = value.Int
				}
			}
			cols = append(cols, types.Column{Name: col.Name, Type: col.Type, V: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: len(out), Valid: valid, I64: out}})
		case types.KindFloat32:
			out := make([]float32, len(col.Values))
			for i, value := range col.Values {
				if value.Kind != v3sql.ValueNull {
					out[i] = float32(insertFloatValue(value))
				}
			}
			cols = append(cols, types.Column{Name: col.Name, Type: col.Type, V: types.Vec{Kind: types.VecFloat32, Encoding: types.EncodingFlat, Len: len(out), Valid: valid, F32: out}})
		case types.KindFloat64:
			out := make([]float64, len(col.Values))
			for i, value := range col.Values {
				if value.Kind != v3sql.ValueNull {
					out[i] = insertFloatValue(value)
				}
			}
			cols = append(cols, types.Column{Name: col.Name, Type: col.Type, V: types.Vec{Kind: types.VecFloat64, Encoding: types.EncodingFlat, Len: len(out), Valid: valid, F64: out}})
		case types.KindTimestamp:
			out := make([]int64, len(col.Values))
			for i, value := range col.Values {
				if value.Kind == v3sql.ValueNull {
					continue
				}
				parsed, err := parseTimestampLiteral(value.String)
				if err != nil {
					return types.Batch{}, fmt.Errorf("column %q: %w", col.Name, err)
				}
				out[i] = parsed
			}
			cols = append(cols, types.Column{Name: col.Name, Type: col.Type, V: types.Vec{Kind: types.VecTimestamp, Encoding: types.EncodingFlat, Len: len(out), Valid: valid, I64: out}})
		case types.KindDate:
			out := make([]int32, len(col.Values))
			for i, value := range col.Values {
				if value.Kind == v3sql.ValueNull {
					continue
				}
				parsed, err := parseDateLiteral(value.String)
				if err != nil {
					return types.Batch{}, fmt.Errorf("column %q: %w", col.Name, err)
				}
				out[i] = parsed
			}
			cols = append(cols, types.Column{Name: col.Name, Type: col.Type, V: types.Vec{Kind: types.VecDate, Encoding: types.EncodingFlat, Len: len(out), Valid: valid, I32: out}})
		case types.KindUUID:
			out := make([]types.UUID16, len(col.Values))
			for i, value := range col.Values {
				if value.Kind == v3sql.ValueNull {
					continue
				}
				parsed, err := types.ParseUUID(value.String)
				if err != nil {
					return types.Batch{}, fmt.Errorf("column %q: %w", col.Name, err)
				}
				out[i] = parsed
			}
			cols = append(cols, types.Column{Name: col.Name, Type: col.Type, V: types.Vec{Kind: types.VecUUID, Encoding: types.EncodingFlat, Len: len(out), Valid: valid, UUID: out}})
		case types.KindNamed:
			out := make([]uint32, len(col.Values))
			for i, value := range col.Values {
				if value.Kind == v3sql.ValueNull {
					continue
				}
				code, ok := enumCodeForLabel(value.String, col.Labels)
				if !ok {
					return types.Batch{}, fmt.Errorf("column %q invalid enum label %q", col.Name, value.String)
				}
				out[i] = code
			}
			cols = append(cols, types.Column{Name: col.Name, Type: col.Type, EnumLabels: col.Labels, V: types.Vec{Kind: types.VecEnum32, Encoding: types.EncodingFlat, Len: len(out), Valid: valid, U32: out}})
		case types.KindText, types.KindBytes, types.KindJSON:
			var kind types.VecKind
			switch col.Type.Kind {
			case types.KindBytes:
				kind = types.VecBytes
			case types.KindJSON:
				kind = types.VecJSON
			default:
				kind = types.VecText
			}
			varbytes := types.NewVarBytes(len(col.Values), len(col.Values)*8)
			for i, value := range col.Values {
				if value.Kind != v3sql.ValueNull {
					varbytes.AppendString(i, value.String)
				} else {
					varbytes.Offsets[i+1] = varbytes.Offsets[i]
				}
			}
			cols = append(cols, types.Column{Name: col.Name, Type: col.Type, V: types.Vec{Kind: kind, Encoding: types.EncodingFlat, Len: len(col.Values), Valid: valid, Var: varbytes}})
		default:
			return types.Batch{}, fmt.Errorf("column %q has unsupported INSERT storage type %s", col.Name, col.Type)
		}
	}
	return types.NewBatch(cols)
}

func insertFloatValue(value v3sql.Value) float64 {
	if value.Kind == v3sql.ValueInt {
		return float64(value.Int)
	}
	return value.Float
}

func enumCodeForLabel(value string, labels []string) (uint32, bool) {
	for i, label := range labels {
		if value == label {
			return uint32(i + 1), true
		}
	}
	return 0, false
}

func parseTimestampLiteral(value string) (int64, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return 0, fmt.Errorf("invalid timestamp literal %q", value)
	}
	if parsed.Before(minTimestampLiteral) || parsed.After(maxTimestampLiteral) {
		return 0, fmt.Errorf("timestamp literal %q out of range", value)
	}
	return parsed.UnixNano(), nil
}

func parseDateLiteral(value string) (int32, error) {
	parsed, err := time.Parse("2006-01-02", value)
	if err != nil {
		return 0, fmt.Errorf("invalid date literal %q", value)
	}
	days := parsed.Unix() / int64(24*time.Hour/time.Second)
	if days < math.MinInt32 || days > math.MaxInt32 {
		return 0, fmt.Errorf("date literal %q out of range", value)
	}
	return int32(days), nil
}

func validityFromValues(values []v3sql.Value, nullCount int) types.Validity {
	if nullCount == 0 {
		return nil
	}
	valid := types.NewValidity(len(values))
	for i, value := range values {
		if value.Kind == v3sql.ValueNull {
			types.SetInvalid(valid, i)
		}
	}
	return valid
}
