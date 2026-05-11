package types

import (
	"math"
	"slices"
	"unsafe"
)

type VarBytes struct {
	Offsets []uint32
	Data    []byte
}

func NewVarBytes(rows int, dataBytes int) VarBytes {
	if rows < 0 {
		rows = 0
	}
	if dataBytes < 0 {
		dataBytes = 0
	}
	return VarBytes{
		Offsets: make([]uint32, rows+1),
		Data:    make([]byte, 0, dataBytes),
	}
}

func (v VarBytes) Rows() int {
	if len(v.Offsets) == 0 {
		return 0
	}
	return len(v.Offsets) - 1
}

func (v VarBytes) Bytes(row int) []byte {
	start := v.Offsets[row]
	end := v.Offsets[row+1]
	return v.Data[start:end:end]
}

// String returns a no-copy view; valid only while v.Data is unmodified.
func (v VarBytes) String(row int) string {
	start := v.Offsets[row]
	end := v.Offsets[row+1]
	if start == end {
		return ""
	}
	b := v.Data[start:end]
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func (v VarBytes) StringCopy(row int) string {
	return string(v.Bytes(row))
}

func (v VarBytes) Clone() VarBytes {
	return VarBytes{Offsets: slices.Clone(v.Offsets), Data: slices.Clone(v.Data)}
}

func (v *VarBytes) AppendBytes(row int, b []byte) {
	v.appendLen(row, len(b))
	v.Data = append(v.Data, b...)
}

func (v *VarBytes) AppendString(row int, s string) {
	v.appendLen(row, len(s))
	v.Data = append(v.Data, s...)
}

func (v *VarBytes) appendLen(row int, n int) {
	if row < 0 || row+1 >= len(v.Offsets) {
		panic("VarBytes append row out of range")
	}
	if len(v.Data) > math.MaxUint32 {
		panic("VarBytes data exceeds uint32 offset capacity")
	}
	if v.Offsets[row] != uint32(len(v.Data)) {
		panic("VarBytes append must be sequential")
	}
	end := len(v.Data) + n
	if end > math.MaxUint32 {
		panic("VarBytes data exceeds uint32 offset capacity")
	}
	v.Offsets[row+1] = uint32(end)
}

func (v *VarBytes) Reset() {
	clear(v.Offsets)
	v.Data = v.Data[:0]
}
