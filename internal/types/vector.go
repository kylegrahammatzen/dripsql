package types

import (
	"fmt"
	"slices"
)

type Vec struct {
	Kind     VecKind
	Encoding Encoding
	Len      int
	Valid    Validity

	BoolBits []uint64
	I16      []int16
	I32      []int32
	I64      []int64
	F32      []float32
	F64      []float64
	UUID     []UUID16
	U32      []uint32
	Var      VarBytes

	DictIDs    []uint8
	DictValues VarBytes

	ConstantBool  bool
	ConstantI64   int64
	ConstantF64   float64
	ConstantUUID  UUID16
	ConstantU32   uint32
	ConstantBytes []byte
	ConstantValid bool

	FORBase  int64
	FORWidth int
	FORData  []byte

	Runs []Run
}

type Run struct {
	Length int
	I64    int64
	F64    float64
	Bool   bool
	U32    uint32
	Bytes  []byte
}

func (v Vec) Clone() Vec {
	out := v
	out.Valid = slices.Clone(v.Valid)
	out.BoolBits = slices.Clone(v.BoolBits)
	out.I16 = slices.Clone(v.I16)
	out.I32 = slices.Clone(v.I32)
	out.I64 = slices.Clone(v.I64)
	out.F32 = slices.Clone(v.F32)
	out.F64 = slices.Clone(v.F64)
	out.UUID = slices.Clone(v.UUID)
	out.U32 = slices.Clone(v.U32)
	out.Var = v.Var.Clone()
	out.DictIDs = slices.Clone(v.DictIDs)
	out.DictValues = v.DictValues.Clone()
	out.ConstantBytes = slices.Clone(v.ConstantBytes)
	out.FORData = slices.Clone(v.FORData)
	out.Runs = cloneRuns(v.Runs)
	return out
}

func (r Run) Clone() Run {
	out := r
	out.Bytes = slices.Clone(r.Bytes)
	return out
}

func cloneRuns(runs []Run) []Run {
	if len(runs) == 0 {
		return nil
	}
	out := make([]Run, len(runs))
	for i, run := range runs {
		out[i] = run.Clone()
	}
	return out
}

func (v Vec) validate() error {
	if v.Len < 0 {
		return fmt.Errorf("negative vector length %d", v.Len)
	}
	if v.Len > StandardBatchRows {
		return fmt.Errorf("vector length %d exceeds batch size %d", v.Len, StandardBatchRows)
	}
	if v.Valid != nil && len(v.Valid) < ValidityWords(v.Len) {
		return fmt.Errorf("validity bitmap has %d words for %d rows", len(v.Valid), v.Len)
	}
	if !v.Kind.valid() {
		return fmt.Errorf("invalid vector kind %s", v.Kind)
	}
	if v.Encoding != EncodingFlat {
		return v.validateNonFlat()
	}
	return v.validateFlat()
}

func (v Vec) validateFlat() error {
	switch v.Kind {
	case VecBool:
		if err := v.validateInactiveFields("BoolBits"); err != nil {
			return err
		}
		return checkLen("bool bits", len(v.BoolBits), ValidityWords(v.Len))
	case VecInt16:
		if err := v.validateInactiveFields("I16"); err != nil {
			return err
		}
		return checkLen("int16 values", len(v.I16), v.Len)
	case VecInt32, VecDate:
		if err := v.validateInactiveFields("I32"); err != nil {
			return err
		}
		return checkLen("int32 values", len(v.I32), v.Len)
	case VecInt64, VecDecimal64, VecTimestamp, VecTime:
		if err := v.validateInactiveFields("I64"); err != nil {
			return err
		}
		return checkLen("int64 values", len(v.I64), v.Len)
	case VecFloat32:
		if err := v.validateInactiveFields("F32"); err != nil {
			return err
		}
		return checkLen("float32 values", len(v.F32), v.Len)
	case VecFloat64:
		if err := v.validateInactiveFields("F64"); err != nil {
			return err
		}
		return checkLen("float64 values", len(v.F64), v.Len)
	case VecUUID:
		if err := v.validateInactiveFields("UUID"); err != nil {
			return err
		}
		return checkLen("uuid values", len(v.UUID), v.Len)
	case VecEnum32:
		if err := v.validateInactiveFields("U32"); err != nil {
			return err
		}
		return checkLen("enum values", len(v.U32), v.Len)
	case VecText, VecBytes, VecJSON:
		if err := v.validateInactiveFields("Var"); err != nil {
			return err
		}
		return validateVarBytes(v.Var, v.Len)
	default:
		return fmt.Errorf("invalid vector kind %s", v.Kind)
	}
}

func (v Vec) validateNonFlat() error {
	switch v.Encoding {
	case EncodingDictionary, EncodingConstant, EncodingSequence, EncodingFORBitPack, EncodingFlate:
		return nil
	default:
		return fmt.Errorf("invalid vector encoding %s", v.Encoding)
	}
}

func (v Vec) validateInactiveFields(active string) error {
	if active != "BoolBits" && len(v.BoolBits) != 0 {
		return fmt.Errorf("inactive bool bits set for %s vector", v.Kind)
	}
	if active != "I16" && len(v.I16) != 0 {
		return fmt.Errorf("inactive int16 values set for %s vector", v.Kind)
	}
	if active != "I32" && len(v.I32) != 0 {
		return fmt.Errorf("inactive int32 values set for %s vector", v.Kind)
	}
	if active != "I64" && len(v.I64) != 0 {
		return fmt.Errorf("inactive int64 values set for %s vector", v.Kind)
	}
	if active != "F32" && len(v.F32) != 0 {
		return fmt.Errorf("inactive float32 values set for %s vector", v.Kind)
	}
	if active != "F64" && len(v.F64) != 0 {
		return fmt.Errorf("inactive float64 values set for %s vector", v.Kind)
	}
	if active != "UUID" && len(v.UUID) != 0 {
		return fmt.Errorf("inactive uuid values set for %s vector", v.Kind)
	}
	if active != "U32" && len(v.U32) != 0 {
		return fmt.Errorf("inactive enum values set for %s vector", v.Kind)
	}
	if active != "Var" && (len(v.Var.Offsets) != 0 || len(v.Var.Data) != 0) {
		return fmt.Errorf("inactive varbytes values set for %s vector", v.Kind)
	}
	if v.FORBase != 0 || v.FORWidth != 0 || len(v.FORData) != 0 {
		return fmt.Errorf("inactive for+bitpack values set for %s vector", v.Kind)
	}
	return nil
}

// checkLen allows over-allocated typed buffers; callers slice to Len.
func checkLen(name string, got int, want int) error {
	if got < want {
		return fmt.Errorf("%s length %d is shorter than %d", name, got, want)
	}
	return nil
}

func validateVarBytes(v VarBytes, rows int) error {
	if rows < 0 {
		return fmt.Errorf("varbytes rows %d is negative", rows)
	}
	if len(v.Offsets) != rows+1 {
		return fmt.Errorf("varbytes offsets length %d does not match rows %d", len(v.Offsets), rows)
	}
	if v.Offsets[0] != 0 {
		return fmt.Errorf("varbytes first offset %d must be 0", v.Offsets[0])
	}
	prev := uint32(0)
	for i := 1; i < len(v.Offsets); i++ {
		offset := v.Offsets[i]
		if offset < prev {
			return fmt.Errorf("varbytes offset %d is less than previous offset", i)
		}
		prev = offset
	}
	last := v.Offsets[len(v.Offsets)-1]
	if uint64(last) != uint64(len(v.Data)) {
		return fmt.Errorf("varbytes last offset %d does not match data length %d", last, len(v.Data))
	}
	return nil
}
