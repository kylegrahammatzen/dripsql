// Bitpack: FastLanes 1024-bit transposed layout for shared FOR / Delta packing.
// Each 1024-element block emits W groups of 64 bits (one bit-position per group),
// so a scalar loop processes 64 elements per uint64 without inter-lane carries.
// Reference: Afroozeh et al., PVLDB 18 (2025), https://www.vldb.org/pvldb/vol18/p4629-afroozeh.pdf
package codec

import (
	"encoding/binary"
	"fmt"
)

// FastLanes virtual-SIMD-register width. Pack zero-pads short inputs to a full block.
const BitpackBlockSize = 1024

// Width 0 is invalid. Callers must dispatch to Constant when all residuals are zero.
func PackedSize(rows, width int) int {
	if width < 1 || width > 64 {
		panic(fmt.Sprintf("bitpack: width %d out of range [1, 64]", width))
	}
	if rows <= 0 {
		return 0
	}
	blocks := (rows + BitpackBlockSize - 1) / BitpackBlockSize
	return blocks * width * 128
}

// Bits above `width` are silently truncated. Caller pre-subtracts base so residuals fit.
func Pack(width int, src []uint64, dst []byte) {
	if width < 1 || width > 64 {
		panic(fmt.Sprintf("bitpack Pack: width %d out of range [1, 64]", width))
	}
	rows := len(src)
	if rows == 0 {
		return
	}
	var block [BitpackBlockSize]uint64
	blocks := (rows + BitpackBlockSize - 1) / BitpackBlockSize
	for b := range blocks {
		srcOff := b * BitpackBlockSize
		end := min(srcOff+BitpackBlockSize, rows)
		n := copy(block[:], src[srcOff:end])
		for i := n; i < BitpackBlockSize; i++ {
			block[i] = 0
		}
		dstOff := b * width * 128
		packBlock(width, block[:], dst[dstOff:dstOff+width*128])
	}
}

func Unpack(width int, src []byte, rows int, dst []uint64) {
	if width < 1 || width > 64 {
		panic(fmt.Sprintf("bitpack Unpack: width %d out of range [1, 64]", width))
	}
	if rows == 0 {
		return
	}
	if len(dst) < rows {
		panic(fmt.Sprintf("bitpack Unpack: dst %d < rows %d", len(dst), rows))
	}
	var block [BitpackBlockSize]uint64
	blocks := (rows + BitpackBlockSize - 1) / BitpackBlockSize
	for b := range blocks {
		srcOff := b * width * 128
		unpackBlock(width, src[srcOff:srcOff+width*128], block[:])
		dstOff := b * BitpackBlockSize
		end := min(dstOff+BitpackBlockSize, rows)
		copy(dst[dstOff:end], block[:end-dstOff])
	}
}

var unpackBytePlaneTable = buildUnpackBytePlaneTable()

func buildUnpackBytePlaneTable() [8][256]uint64 {
	var table [8][256]uint64
	for plane := range table {
		weight := uint64(1 << plane)
		for b := range 256 {
			var packed uint64
			for bit := range 8 {
				if b&(1<<bit) != 0 {
					packed |= weight << (uint(bit) * 8)
				}
			}
			table[plane][b] = packed
		}
	}
	return table
}

// packBlock emits 16 groups of width uint64s for a full 1024-element block.
// Within each group, output word i holds bit i of all 64 input elements.
func packBlock(width int, block []uint64, dst []byte) {
	for j := range 16 {
		base := j * 64
		groupOff := j * width * 8
		for i := range width {
			var word uint64
			for k := range 64 {
				word |= ((block[base+k] >> uint(i)) & 1) << uint(k)
			}
			binary.LittleEndian.PutUint64(dst[groupOff+i*8:groupOff+i*8+8], word)
		}
	}
}

// unpackBlock reverses packBlock: for each (j, k), gather bit k from each of the
// width words in group j and assemble the element.
func unpackBlock(width int, src []byte, block []uint64) {
	if width <= 8 {
		unpackBlockSmallWidth(width, src, block)
		return
	}
	var words [64]uint64
	for j := range 16 {
		groupOff := j * width * 8
		base := j * 64
		for i := range width {
			words[i] = binary.LittleEndian.Uint64(src[groupOff+i*8 : groupOff+i*8+8])
		}
		for k := range 64 {
			var elem uint64
			for i := range width {
				elem |= ((words[i] >> uint(k)) & 1) << uint(i)
			}
			block[base+k] = elem
		}
	}
}

func unpackBlockSmallWidth(width int, src []byte, block []uint64) {
	var words [8]uint64
	for j := range 16 {
		groupOff := j * width * 8
		base := j * 64
		for i := range width {
			words[i] = binary.LittleEndian.Uint64(src[groupOff+i*8 : groupOff+i*8+8])
		}
		for lane := range 8 {
			shift := uint(lane * 8)
			var packed uint64
			for i := range width {
				packed |= unpackBytePlaneTable[i][byte(words[i]>>shift)]
			}
			out := block[base+lane*8 : base+lane*8+8]
			out[0] = uint64(byte(packed))
			out[1] = uint64(byte(packed >> 8))
			out[2] = uint64(byte(packed >> 16))
			out[3] = uint64(byte(packed >> 24))
			out[4] = uint64(byte(packed >> 32))
			out[5] = uint64(byte(packed >> 40))
			out[6] = uint64(byte(packed >> 48))
			out[7] = uint64(byte(packed >> 56))
		}
	}
}
