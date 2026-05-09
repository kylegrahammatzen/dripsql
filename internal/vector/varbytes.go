package vector

import "unsafe"

type VarBytes struct {
	Offsets []uint32 // Offsets stores one boundary per row plus a final end offset.
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

func (v VarBytes) Bytes(row int) []byte {
	start := v.Offsets[row]
	end := v.Offsets[row+1]
	return v.Data[start:end]
}

func (v VarBytes) String(row int) string {
	b := v.Bytes(row)
	if len(b) == 0 {
		return ""
	}
	// The returned string is valid only while v.Data remains unmodified.
	return unsafe.String(unsafe.SliceData(b), len(b))
}
