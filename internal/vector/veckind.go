// VecKind tags every Vec with the physical kind of data it holds.
// Width, FOR-packability, name, and the schema.Type round-trip come from per-kind switches the compiler folds.
package vector

import (
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
)

type VecKind uint8

const (
	VecInvalid VecKind = iota
	VecBool
	VecInt16
	VecInt32
	VecInt64
	VecFloat32
	VecFloat64
	VecDecimal64
	VecText
	VecBytes
	VecUUID
	VecTimestamp
	VecTime
	VecDate
	VecJSON
	VecEnum32
)

const StandardBatchRows = 2048

type Width int

const (
	WidthVarBytes Width = -1
	WidthBool     Width = -2
)

func (k VecKind) String() string {
	switch k {
	case VecBool:
		return "bool"
	case VecInt16:
		return "int16"
	case VecInt32:
		return "int32"
	case VecInt64:
		return "int64"
	case VecFloat32:
		return "float32"
	case VecFloat64:
		return "float64"
	case VecDecimal64:
		return "decimal64"
	case VecText:
		return "text"
	case VecBytes:
		return "bytes"
	case VecUUID:
		return "uuid"
	case VecTimestamp:
		return "timestamp"
	case VecTime:
		return "time"
	case VecDate:
		return "date"
	case VecJSON:
		return "json"
	case VecEnum32:
		return "enum32"
	}
	return "invalid"
}

func (k VecKind) FixedWidth() Width {
	switch k {
	case VecBool:
		return WidthBool
	case VecInt16:
		return 2
	case VecInt32, VecDate, VecEnum32:
		return 4
	case VecInt64, VecFloat64, VecDecimal64, VecTimestamp, VecTime:
		return 8
	case VecFloat32:
		return 4
	case VecUUID:
		return 16
	case VecText, VecBytes, VecJSON:
		return WidthVarBytes
	}
	return 0
}

func (k VecKind) IsVarBytes() bool { return k.FixedWidth() == WidthVarBytes }

func (k VecKind) IsFORPackable() bool {
	switch k {
	case VecInt16, VecInt32, VecInt64, VecDecimal64,
		VecTimestamp, VecTime, VecDate, VecEnum32:
		return true
	}
	return false
}

// TypeFromVecKind reconstructs a logical schema.Type from a physical VecKind and requires enumName when k is VecEnum32.
func TypeFromVecKind(k VecKind, enumName string) (schema.Type, error) {
	switch k {
	case VecBool:
		return schema.Bool, nil
	case VecInt16:
		return schema.Int16, nil
	case VecInt32:
		return schema.Int32, nil
	case VecInt64:
		return schema.Int64, nil
	case VecFloat32:
		return schema.Float32, nil
	case VecFloat64:
		return schema.Float64, nil
	case VecDecimal64:
		return schema.Decimal, nil
	case VecText:
		return schema.Text, nil
	case VecBytes:
		return schema.Bytes, nil
	case VecUUID:
		return schema.UUID, nil
	case VecTimestamp:
		return schema.Timestamp, nil
	case VecTime:
		return schema.Time, nil
	case VecDate:
		return schema.Date, nil
	case VecJSON:
		return schema.JSON, nil
	case VecEnum32:
		if enumName == "" {
			return schema.Type{}, fmt.Errorf("TypeFromVecKind: enum without name")
		}
		return schema.Named(enumName), nil
	}
	return schema.Type{}, fmt.Errorf("TypeFromVecKind: unknown VecKind %v", k)
}

// VecKindOf returns the physical VecKind for a logical schema.Type.
func VecKindOf(t schema.Type) (VecKind, error) {
	if !t.Valid() {
		return VecInvalid, fmt.Errorf("VecKindOf: invalid type %v", t)
	}
	switch t.Kind {
	case schema.KindBool:
		return VecBool, nil
	case schema.KindInt16:
		return VecInt16, nil
	case schema.KindInt32:
		return VecInt32, nil
	case schema.KindInt64:
		return VecInt64, nil
	case schema.KindFloat32:
		return VecFloat32, nil
	case schema.KindFloat64:
		return VecFloat64, nil
	case schema.KindDecimal:
		return VecDecimal64, nil
	case schema.KindText:
		return VecText, nil
	case schema.KindBytes:
		return VecBytes, nil
	case schema.KindUUID:
		return VecUUID, nil
	case schema.KindTimestamp:
		return VecTimestamp, nil
	case schema.KindTime:
		return VecTime, nil
	case schema.KindDate:
		return VecDate, nil
	case schema.KindJSON:
		return VecJSON, nil
	case schema.KindNamed:
		return VecEnum32, nil
	}
	return VecInvalid, fmt.Errorf("VecKindOf: invalid kind %v", t.Kind)
}
