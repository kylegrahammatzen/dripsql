package engine

import (
	"fmt"
	"math"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/sql/ast"
	"github.com/kylegrahammatzen/dripsql/internal/sql/binder"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

var (
	minTimestampLiteral = time.Unix(0, math.MinInt64).UTC()
	maxTimestampLiteral = time.Unix(0, math.MaxInt64).UTC()
)

func batchFromInsert(values binder.InsertValues) (vector.Batch, error) {
	cols := make([]vector.Column, 0, len(values.Columns))
	for _, col := range values.Columns {
		valid := validityFromValues(col.Values, col.NullCount)
		switch col.Type.Kind {
		case sqltype.KindBool:
			bits := make([]uint64, vector.ValidityWords(len(col.Values)))
			for i, value := range col.Values {
				if value.Kind == ast.ValueNull || !value.Bool {
					continue
				}
				bits[i>>6] |= uint64(1) << uint(i&63)
			}
			cols = append(cols, vector.Column{Name: col.Name, Type: col.Type, V: vector.Vec{Kind: vector.Bool, Len: len(col.Values), Valid: valid, BoolBits: bits}})
		case sqltype.KindInt16:
			out := make([]int16, len(col.Values))
			for i, value := range col.Values {
				if value.Kind == ast.ValueNull {
					continue
				}
				out[i] = int16(value.Int)
			}
			cols = append(cols, vector.Column{Name: col.Name, Type: col.Type, V: vector.Vec{Kind: vector.Int16, Len: len(out), Valid: valid, I16: out}})
		case sqltype.KindInt32:
			out := make([]int32, len(col.Values))
			for i, value := range col.Values {
				if value.Kind == ast.ValueNull {
					continue
				}
				out[i] = int32(value.Int)
			}
			cols = append(cols, vector.Column{Name: col.Name, Type: col.Type, V: vector.Vec{Kind: vector.Int32, Len: len(out), Valid: valid, I32: out}})
		case sqltype.KindInt64:
			out := make([]int64, len(col.Values))
			for i, value := range col.Values {
				if value.Kind == ast.ValueNull {
					continue
				}
				out[i] = value.Int
			}
			cols = append(cols, vector.Column{Name: col.Name, Type: col.Type, V: vector.Vec{Kind: vector.Int64, Len: len(out), Valid: valid, I64: out}})
		case sqltype.KindFloat32:
			out := make([]float32, len(col.Values))
			for i, value := range col.Values {
				if value.Kind == ast.ValueNull {
					continue
				}
				out[i] = float32(insertFloatValue(value))
			}
			cols = append(cols, vector.Column{Name: col.Name, Type: col.Type, V: vector.Vec{Kind: vector.Float32, Len: len(out), Valid: valid, F32: out}})
		case sqltype.KindFloat64:
			out := make([]float64, len(col.Values))
			for i, value := range col.Values {
				if value.Kind == ast.ValueNull {
					continue
				}
				out[i] = insertFloatValue(value)
			}
			cols = append(cols, vector.Column{Name: col.Name, Type: col.Type, V: vector.Vec{Kind: vector.Float64, Len: len(out), Valid: valid, F64: out}})
		case sqltype.KindTimestamp:
			out := make([]int64, len(col.Values))
			for i, value := range col.Values {
				if value.Kind == ast.ValueNull {
					continue
				}
				parsed, err := parseTimestampLiteral(value.String)
				if err != nil {
					return vector.Batch{}, fmt.Errorf("column %q: %w", col.Name, err)
				}
				out[i] = parsed
			}
			cols = append(cols, vector.Column{Name: col.Name, Type: col.Type, V: vector.Vec{Kind: vector.Timestamp, Len: len(out), Valid: valid, I64: out}})
		case sqltype.KindDate:
			out := make([]int32, len(col.Values))
			for i, value := range col.Values {
				if value.Kind == ast.ValueNull {
					continue
				}
				parsed, err := parseDateLiteral(value.String)
				if err != nil {
					return vector.Batch{}, fmt.Errorf("column %q: %w", col.Name, err)
				}
				out[i] = parsed
			}
			cols = append(cols, vector.Column{Name: col.Name, Type: col.Type, V: vector.Vec{Kind: vector.Date, Len: len(out), Valid: valid, I32: out}})
		case sqltype.KindUUID:
			out := make([]vector.UUID16, len(col.Values))
			for i, value := range col.Values {
				if value.Kind == ast.ValueNull {
					continue
				}
				parsed, err := vector.ParseUUID(value.String)
				if err != nil {
					return vector.Batch{}, fmt.Errorf("column %q: %w", col.Name, err)
				}
				out[i] = parsed
			}
			cols = append(cols, vector.Column{Name: col.Name, Type: col.Type, V: vector.Vec{Kind: vector.UUID, Len: len(out), Valid: valid, UUID: out}})
		case sqltype.KindNamed:
			out := make([]uint32, len(col.Values))
			for i, value := range col.Values {
				if value.Kind == ast.ValueNull {
					continue
				}
				code, ok := enumCodeForLabel(value.String, col.Labels)
				if !ok {
					return vector.Batch{}, fmt.Errorf("column %q invalid enum label %q", col.Name, value.String)
				}
				out[i] = code
			}
			cols = append(cols, vector.Column{Name: col.Name, Type: col.Type, EnumLabels: col.Labels, V: vector.Vec{Kind: vector.Enum32, Len: len(out), Valid: valid, U32: out}})
		case sqltype.KindText, sqltype.KindBytes:
			varbytes := vector.NewVarBytes(len(col.Values), len(col.Values)*8)
			for i, value := range col.Values {
				if value.Kind != ast.ValueNull {
					varbytes.Data = append(varbytes.Data, value.String...)
				}
				varbytes.Offsets[i+1] = uint32(len(varbytes.Data))
			}
			kind := vector.Text
			if col.Type.Kind == sqltype.KindBytes {
				kind = vector.Bytes
			}
			cols = append(cols, vector.Column{Name: col.Name, Type: col.Type, V: vector.Vec{Kind: kind, Len: len(col.Values), Valid: valid, Var: varbytes}})
		default:
			return vector.Batch{}, fmt.Errorf("column %q has unsupported INSERT storage type %s", col.Name, col.Type)
		}
	}
	return vector.NewBatch(cols)
}

func insertFloatValue(value ast.Value) float64 {
	if value.Kind == ast.ValueInt {
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

func validityFromValues(values []ast.Value, nullCount int) vector.Validity {
	if nullCount == 0 {
		return nil
	}
	valid := vector.NewValidity(len(values))
	for i, value := range values {
		if value.Kind == ast.ValueNull {
			vector.SetInvalid(valid, i)
		}
	}
	return valid
}
