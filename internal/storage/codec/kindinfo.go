package codec

import (
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

// fixedKindBytes returns the per-row byte width for fixed-width kinds.
// VecBool is excluded because it packs into validity-style words rather than
// per-row bytes; varlen kinds (text/bytes/json) return (0, false). This is a
// thin compatibility shim over types.VecKind.FixedWidth so legacy codec
// callsites don't need to handle the Width sentinel themselves.
func fixedKindBytes(kind types.VecKind) (int, bool) {
	if w := kind.FixedWidth(); w > 0 {
		return int(w), true
	}
	return 0, false
}

// resizeSlice returns a slice of length n that reuses dst's backing array
// when cap(dst) >= n, allocating fresh otherwise. Used by every codec's
// DecodeInto path to amortize allocations across repeated decodes.
func resizeSlice[S ~[]E, E any](dst S, n int) S {
	if cap(dst) < n {
		return make(S, n)
	}
	return dst[:n]
}

// expectFixedPayload validates that a single-value fixed-width payload has the
// right length for kind.
func expectFixedPayload(payload []byte, kind types.VecKind, label string) error {
	size, ok := fixedKindBytes(kind)
	if !ok {
		return fmt.Errorf("%s codec unsupported kind %s", label, kind)
	}
	if len(payload) != size {
		return fmt.Errorf("%s %s payload length %d", label, kind, len(payload))
	}
	return nil
}

// resetVecForDecode prepares a destination Vec for an in-place codec decode.
// Every typed slice that doesn't match keepKind is dropped so the capacity it
// holds can be GC'd, and v.Encoded is dropped unless keepEncoded is set (the
// dictionary codec passes true so its DictIDs/DictValues backing arrays stay
// available for reuse on the next decode).
func resetVecForDecode(v *types.Vec, keepKind types.VecKind, keepEncoded bool) {
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
	if !keepKind.IsVarBytes() {
		v.Var = types.VarBytes{}
	}
	if !keepEncoded {
		v.Encoded = nil
	}
}
