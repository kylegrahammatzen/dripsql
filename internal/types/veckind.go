package types

import "fmt"

// StandardBatchRows is the canonical batch size for vectorized execution.
const StandardBatchRows = 2048

// VecKind tags a Vec with how its data buffer should be interpreted.
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

var vecKindNames = [...]string{
	VecInvalid:   "invalid",
	VecBool:      "bool",
	VecInt16:     "int16",
	VecInt32:     "int32",
	VecInt64:     "int64",
	VecFloat32:   "float32",
	VecFloat64:   "float64",
	VecDecimal64: "decimal64",
	VecText:      "text",
	VecBytes:     "bytes",
	VecUUID:      "uuid",
	VecTimestamp: "timestamp",
	VecTime:      "time",
	VecDate:      "date",
	VecJSON:      "json",
	VecEnum32:    "enum32",
}

func (k VecKind) String() string {
	if int(k) < len(vecKindNames) {
		if name := vecKindNames[k]; name != "" {
			return name
		}
	}
	return fmt.Sprintf("vec_kind(%d)", k)
}

// IsVarBytes reports whether k stores variable-length payloads via VarBytes.
func (k VecKind) IsVarBytes() bool {
	switch k {
	case VecText, VecBytes, VecJSON:
		return true
	}
	return false
}

// IsFORPackable reports whether k is acceptable input to the FOR+BitPack codec.
func (k VecKind) IsFORPackable() bool {
	switch k {
	case VecInt16, VecInt32, VecDate, VecInt64, VecTimestamp, VecTime, VecEnum32:
		return true
	}
	return false
}

// Width is the per-row physical byte width discriminator.
type Width int

const (
	WidthVarBytes Width = -1
	WidthBool     Width = -2
)

// FixedWidth returns the per-row byte width for fixed kinds, or a sentinel for bool and varbytes.
func (k VecKind) FixedWidth() Width {
	switch k {
	case VecInt16:
		return 2
	case VecInt32, VecDate, VecFloat32, VecEnum32:
		return 4
	case VecInt64, VecDecimal64, VecTimestamp, VecTime, VecFloat64:
		return 8
	case VecUUID:
		return 16
	case VecBool:
		return WidthBool
	case VecText, VecBytes, VecJSON:
		return WidthVarBytes
	}
	return 0
}
