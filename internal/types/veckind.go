package types

import "fmt"

const (
	StandardBatchRows = 2048
	BatchSize         = StandardBatchRows
)

type Row uint16
type Sel []Row

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

type UUID16 [16]byte

func (k VecKind) String() string {
	if int(k) < len(vecKindNames) {
		if name := vecKindNames[k]; name != "" {
			return name
		}
	}
	return fmt.Sprintf("vec_kind(%d)", k)
}

// IsVarBytes reports whether k stores variable-length payloads in Vec.Var
// (text, bytes, JSON). Used by codecs to gate var-length encode/decode paths.
func (k VecKind) IsVarBytes() bool {
	switch k {
	case VecText, VecBytes, VecJSON:
		return true
	default:
		return false
	}
}

// IsFORPackable reports whether k is acceptable input to the FOR+BitPack
// codec. Excludes VecDecimal64 because the decimal scale lives outside the
// vector and cannot be reconstructed from a packed offset alone.
func (k VecKind) IsFORPackable() bool {
	switch k {
	case VecInt16, VecInt32, VecDate, VecInt64, VecTimestamp, VecTime, VecEnum32:
		return true
	default:
		return false
	}
}

func (k VecKind) valid() bool {
	switch k {
	case VecBool,
		VecInt16,
		VecInt32,
		VecDate,
		VecInt64,
		VecDecimal64,
		VecTimestamp,
		VecTime,
		VecFloat32,
		VecFloat64,
		VecUUID,
		VecEnum32,
		VecText,
		VecBytes,
		VecJSON:
		return true
	default:
		return false
	}
}

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

func VecKindOf(t Type) (VecKind, error) {
	switch t.Kind {
	case KindBool:
		return VecBool, nil
	case KindInt16:
		return VecInt16, nil
	case KindInt32:
		return VecInt32, nil
	case KindInt64:
		return VecInt64, nil
	case KindFloat32:
		return VecFloat32, nil
	case KindFloat64:
		return VecFloat64, nil
	case KindDecimal:
		return VecDecimal64, nil
	case KindText:
		return VecText, nil
	case KindBytes:
		return VecBytes, nil
	case KindUUID:
		return VecUUID, nil
	case KindTimestamp:
		return VecTimestamp, nil
	case KindTime:
		return VecTime, nil
	case KindDate:
		return VecDate, nil
	case KindJSON:
		return VecJSON, nil
	case KindNamed:
		if t.Name != "" {
			return VecEnum32, nil
		}
	}
	return VecInvalid, fmt.Errorf("unsupported vector type %s", t)
}
