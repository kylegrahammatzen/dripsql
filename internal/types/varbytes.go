package types

import (
	"encoding/binary"
	"math"
	"slices"
	"unsafe"
)

// VarBytes keeps the on-wire offset+data layout and adds a parallel
// Prefixes slice holding the first up to 4 bytes of each row packed as
// uint32 LE. The prefix lets text equality and ordered comparisons
// reject most non-matching rows without dereferencing Data, the same
// short-circuit DuckDB and Velox get from German Strings without
// changing the wire format yet.
type VarBytes struct {
	Offsets  []uint32
	Data     []byte
	Prefixes []uint32
}

func NewVarBytes(rows int, dataBytes int) VarBytes {
	if rows < 0 {
		rows = 0
	}
	if dataBytes < 0 {
		dataBytes = 0
	}
	return VarBytes{
		Offsets:  make([]uint32, rows+1),
		Data:     make([]byte, 0, dataBytes),
		Prefixes: make([]uint32, rows),
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

// Prefix returns the first up to 4 bytes of row packed as uint32 LE,
// suitable for fast rejection in equality and ordered compares.
func (v VarBytes) Prefix(row int) uint32 {
	if row < 0 || row >= len(v.Prefixes) {
		return 0
	}
	return v.Prefixes[row]
}

// Len returns the byte length of row, derived from Offsets.
func (v VarBytes) Len(row int) uint32 {
	return v.Offsets[row+1] - v.Offsets[row]
}

func (v VarBytes) Clone() VarBytes {
	return VarBytes{
		Offsets:  slices.Clone(v.Offsets),
		Data:     slices.Clone(v.Data),
		Prefixes: slices.Clone(v.Prefixes),
	}
}

func (v *VarBytes) AppendBytes(row int, b []byte) {
	v.appendLen(row, len(b))
	v.Data = append(v.Data, b...)
	v.Prefixes[row] = packPrefix(b)
}

func (v *VarBytes) AppendString(row int, s string) {
	v.appendLen(row, len(s))
	v.Data = append(v.Data, s...)
	v.Prefixes[row] = packPrefix([]byte(s))
}

func (v *VarBytes) appendLen(row int, n int) {
	if row < 0 || row+1 >= len(v.Offsets) {
		panic("VarBytes append row out of range")
	}
	if row >= len(v.Prefixes) {
		panic("VarBytes prefix slot missing for row")
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
	clear(v.Prefixes)
	v.Data = v.Data[:0]
}

// RebuildPrefixes (re)populates Prefixes from Offsets+Data; callers use
// it after decoding a wire-format payload that does not carry prefixes.
func (v *VarBytes) RebuildPrefixes() {
	rows := v.Rows()
	if cap(v.Prefixes) < rows {
		v.Prefixes = make([]uint32, rows)
	} else {
		v.Prefixes = v.Prefixes[:rows]
		clear(v.Prefixes)
	}
	for i := range rows {
		v.Prefixes[i] = packPrefix(v.Bytes(i))
	}
}

// PackPrefix returns the prefix encoding used by VarBytes.Prefixes for
// the supplied search needle, so callers compare needle prefix against
// row prefixes without rebuilding the same packing inline.
func PackPrefix(b []byte) uint32 {
	return packPrefix(b)
}

func packPrefix(b []byte) uint32 {
	switch {
	case len(b) >= 4:
		return binary.LittleEndian.Uint32(b[:4])
	case len(b) == 3:
		return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16
	case len(b) == 2:
		return uint32(b[0]) | uint32(b[1])<<8
	case len(b) == 1:
		return uint32(b[0])
	default:
		return 0
	}
}
