// VecKind tags every Vec with the physical kind of data it holds.
// Width, FOR-packability, and name come from per-kind switches the compiler folds.
package vector

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
