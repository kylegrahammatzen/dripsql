// Bitpack implements the FastLanes 1024-bit transposed layout for shared FOR and Delta packing (Afroozeh et al., PVLDB 2025).
// Each 1024-element block emits W groups of 64 bits so a scalar loop handles 64 elements per uint64 without inter-lane carries.
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
	// Full blocks unpack straight into dst so only a trailing partial block pays the copy.
	var block [BitpackBlockSize]uint64
	blocks := (rows + BitpackBlockSize - 1) / BitpackBlockSize
	for b := range blocks {
		srcOff := b * width * 128
		dstOff := b * BitpackBlockSize
		if rows-dstOff >= BitpackBlockSize {
			unpackBlock(width, src[srcOff:srcOff+width*128], dst[dstOff:dstOff+BitpackBlockSize])
			continue
		}
		unpackBlock(width, src[srcOff:srcOff+width*128], block[:])
		copy(dst[dstOff:rows], block[:rows-dstOff])
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

// unpackBlock reverses packBlock through byte-plane table kernels. Each width band
// keeps its accumulators in registers, which a single unified loop fails to do.
func unpackBlock(width int, src []byte, block []uint64) {
	switch {
	case width <= 8:
		unpackBlockW8(width, src, block)
	case width <= 16:
		unpackBlockW16(width, src, block)
	default:
		unpackBlockWide(width, src, block)
	}
}

func unpackBlockW8(width int, src []byte, block []uint64) {
	var wordsBuf [8]uint64
	words := wordsBuf[:width]
	tbl := unpackBytePlaneTable[:width]
	for j := range 16 {
		group := src[j*width*8 : (j+1)*width*8]
		base := j * 64
		for i := range words {
			words[i] = binary.LittleEndian.Uint64(group[i*8 : i*8+8])
		}
		for lane := range 8 {
			shift := uint(lane * 8)
			var packed uint64
			for i, w := range words {
				packed |= tbl[i][byte(w>>shift)]
			}
			out := block[base+lane*8 : base+lane*8+8 : base+lane*8+8]
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

// unpackBlockW16 runs one byte-plane pass for bits 0-7 and a second for bits 8-15,
// merging the two packed lanes per element.
func unpackBlockW16(width int, src []byte, block []uint64) {
	var wordsBuf [16]uint64
	words := wordsBuf[:width]
	tbl := unpackBytePlaneTable[:]
	for j := range 16 {
		group := src[j*width*8 : (j+1)*width*8]
		base := j * 64
		for i := range words {
			words[i] = binary.LittleEndian.Uint64(group[i*8 : i*8+8])
		}
		for lane := range 8 {
			shift := uint(lane * 8)
			var lo, hi uint64
			for i := range 8 {
				lo |= tbl[i][byte(words[i]>>shift)]
			}
			for i := 8; i < width; i++ {
				hi |= tbl[i-8][byte(words[i]>>shift)]
			}
			out := block[base+lane*8 : base+lane*8+8 : base+lane*8+8]
			out[0] = uint64(byte(lo)) | uint64(byte(hi))<<8
			out[1] = uint64(byte(lo>>8)) | uint64(byte(hi>>8))<<8
			out[2] = uint64(byte(lo>>16)) | uint64(byte(hi>>16))<<8
			out[3] = uint64(byte(lo>>24)) | uint64(byte(hi>>24))<<8
			out[4] = uint64(byte(lo>>32)) | uint64(byte(hi>>32))<<8
			out[5] = uint64(byte(lo>>40)) | uint64(byte(hi>>40))<<8
			out[6] = uint64(byte(lo>>48)) | uint64(byte(hi>>48))<<8
			out[7] = uint64(byte(lo>>56)) | uint64(byte(hi>>56))<<8
		}
	}
}

// unpackBlockWide generalizes the byte-plane pass to one group per 8 bit positions.
func unpackBlockWide(width int, src []byte, block []uint64) {
	var wordsBuf [64]uint64
	words := wordsBuf[:width]
	for j := range 16 {
		group := src[j*width*8 : (j+1)*width*8]
		base := j * 64
		for i := range words {
			words[i] = binary.LittleEndian.Uint64(group[i*8 : i*8+8])
		}
		for lane := range 8 {
			shift := uint(lane * 8)
			var elems [8]uint64
			for g := 0; g < width; g += 8 {
				planes := min(width-g, 8)
				var packed uint64
				for i := range planes {
					packed |= unpackBytePlaneTable[i][byte(words[g+i]>>shift)]
				}
				sh := uint(g)
				elems[0] |= uint64(byte(packed)) << sh
				elems[1] |= uint64(byte(packed>>8)) << sh
				elems[2] |= uint64(byte(packed>>16)) << sh
				elems[3] |= uint64(byte(packed>>24)) << sh
				elems[4] |= uint64(byte(packed>>32)) << sh
				elems[5] |= uint64(byte(packed>>40)) << sh
				elems[6] |= uint64(byte(packed>>48)) << sh
				elems[7] |= uint64(byte(packed>>56)) << sh
			}
			out := block[base+lane*8 : base+lane*8+8 : base+lane*8+8]
			out[0] = elems[0]
			out[1] = elems[1]
			out[2] = elems[2]
			out[3] = elems[3]
			out[4] = elems[4]
			out[5] = elems[5]
			out[6] = elems[6]
			out[7] = elems[7]
		}
	}
}
