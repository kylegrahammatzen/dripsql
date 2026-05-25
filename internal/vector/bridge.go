// Bridge functions map between logical schema.Kind and physical VecKind.
// Both helpers live in vector because vector owns VecKind and may import schema cycle-free.
package vector

import (
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
)

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
