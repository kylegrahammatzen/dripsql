package codec

import (
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

// fixedKindBytes returns the per-row byte width for fixed-width kinds.
// VecBool is excluded because it packs into validity-style words rather than
// per-row bytes; varlen kinds (text/bytes/json) return (0, false).
func fixedKindBytes(kind types.VecKind) (int, bool) {
	switch kind {
	case types.VecInt16:
		return 2, true
	case types.VecInt32, types.VecDate, types.VecFloat32, types.VecEnum32:
		return 4, true
	case types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime, types.VecFloat64:
		return 8, true
	case types.VecUUID:
		return 16, true
	}
	return 0, false
}

// expectFixedPayload validates that a single-value fixed-width payload has the
// right length for kind. Returns the size on success, or a wrapped error.
func expectFixedPayload(payload []byte, kind types.VecKind, label string) (int, error) {
	size, ok := fixedKindBytes(kind)
	if !ok {
		return 0, fmt.Errorf("%s codec unsupported kind %s", label, kind)
	}
	if len(payload) != size {
		return 0, fmt.Errorf("%s %s payload length %d", label, kind, len(payload))
	}
	return size, nil
}

// clearVecCodecFields zeros every codec-specific field on v, preserving the
// typed slice that matches keepKind so its capacity can be reused on the next
// decode. Pass keepDict=true to keep DictIDs/DictValues for dictionary-decode
// reuse. Constant/FOR/Run fields are always cleared.
func clearVecCodecFields(v *types.Vec, keepKind types.VecKind, keepDict bool) {
	if keepKind != types.VecBool {
		v.BoolBits = nil
	}
	if keepKind != types.VecInt16 {
		v.I16 = nil
	}
	if keepKind != types.VecInt32 && keepKind != types.VecDate {
		v.I32 = nil
	}
	if keepKind != types.VecInt64 && keepKind != types.VecDecimal64 && keepKind != types.VecTimestamp && keepKind != types.VecTime {
		v.I64 = nil
	}
	if keepKind != types.VecFloat32 {
		v.F32 = nil
	}
	if keepKind != types.VecFloat64 {
		v.F64 = nil
	}
	if keepKind != types.VecUUID {
		v.UUID = nil
	}
	if keepKind != types.VecEnum32 {
		v.U32 = nil
	}
	if keepKind != types.VecText && keepKind != types.VecBytes && keepKind != types.VecJSON {
		v.Var = types.VarBytes{}
	}
	if !keepDict {
		v.DictIDs = nil
		v.DictValues = types.VarBytes{}
	}
	v.ConstantBytes = nil
	v.FORBase = 0
	v.FORWidth = 0
	v.FORData = nil
	v.Runs = nil
}
