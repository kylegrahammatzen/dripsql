package vector

import "fmt"

type Vec struct {
	Kind  Kind
	Len   int
	Valid Validity

	BoolBits []uint64
	I16      []int16
	I32      []int32
	I64      []int64
	F32      []float32
	F64      []float64
	UUID     []UUID16
	U32      []uint32
	Var      VarBytes
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

	switch v.Kind {
	case Bool:
		if err := v.validateInactiveFields("BoolBits"); err != nil {
			return err
		}
		return checkLen("bool bits", len(v.BoolBits), ValidityWords(v.Len))
	case Int16:
		if err := v.validateInactiveFields("I16"); err != nil {
			return err
		}
		return checkLen("int16 values", len(v.I16), v.Len)
	case Int32, Date:
		if err := v.validateInactiveFields("I32"); err != nil {
			return err
		}
		return checkLen("int32 values", len(v.I32), v.Len)
	case Int64, Decimal64, Timestamp, Time:
		if err := v.validateInactiveFields("I64"); err != nil {
			return err
		}
		return checkLen("int64 values", len(v.I64), v.Len)
	case Float32:
		if err := v.validateInactiveFields("F32"); err != nil {
			return err
		}
		return checkLen("float32 values", len(v.F32), v.Len)
	case Float64:
		if err := v.validateInactiveFields("F64"); err != nil {
			return err
		}
		return checkLen("float64 values", len(v.F64), v.Len)
	case UUID:
		if err := v.validateInactiveFields("UUID"); err != nil {
			return err
		}
		return checkLen("uuid values", len(v.UUID), v.Len)
	case Enum32:
		if err := v.validateInactiveFields("U32"); err != nil {
			return err
		}
		return checkLen("enum values", len(v.U32), v.Len)
	case Text, Bytes, JSON:
		if err := v.validateInactiveFields("Var"); err != nil {
			return err
		}
		return validateVarBytes(v.Var, v.Len)
	default:
		return fmt.Errorf("invalid vector kind %s", v.Kind)
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
	return nil
}

// checkLen allows over-allocated typed buffers; callers slice vectors to Len.
func checkLen(name string, got int, want int) error {
	if got < want {
		return fmt.Errorf("%s length %d is shorter than %d", name, got, want)
	}
	return nil
}

func validateVarBytes(v VarBytes, rows int) error {
	if len(v.Offsets) != rows+1 {
		return fmt.Errorf("varbytes offsets length %d does not match rows %d", len(v.Offsets), rows)
	}
	prev := uint32(0)
	for i, offset := range v.Offsets {
		if offset < prev {
			return fmt.Errorf("varbytes offset %d is less than previous offset", i)
		}
		prev = offset
	}
	if len(v.Offsets) != 0 && int(v.Offsets[len(v.Offsets)-1]) > len(v.Data) {
		return fmt.Errorf("varbytes last offset %d exceeds data length %d", v.Offsets[len(v.Offsets)-1], len(v.Data))
	}
	return nil
}
