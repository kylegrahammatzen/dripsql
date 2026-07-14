//go:build amd64

package codec

import (
	"internal/cpu"
	"runtime"
)

var avx2Enabled = cpu.X86.HasAVX2

// UnpackAVX2 unpacks using AVX2 byte-plane kernels when available.
func UnpackAVX2(width int, src []byte, rows int, dst []uint64) {
	if !avx2Enabled || len(dst) < rows {
		Unpack(width, src, rows, dst)
		return
	}
	switch width {
	case 8:
		unpackAVX2W8(src, rows, dst)
	case 16:
		unpackAVX2W16(src, rows, dst)
	case 32:
		unpackAVX2W32(src, rows, dst)
	case 64:
		unpackAVX2W64(src, rows, dst)
	default:
		Unpack(width, src, rows, dst)
	}
}

// These are implemented in bitpack_avx2_amd64.s
func unpackAVX2W8(src []byte, rows int, dst []uint64)
func unpackAVX2W16(src []byte, rows int, dst []uint64)
func unpackAVX2W32(src []byte, rows int, dst []uint64)
func unpackAVX2W64(src []byte, rows int, dst []uint64)

func init() {
	if runtime.GOARCH == "amd64" && avx2Enabled {
		// Override the scalar Unpack for common widths
	}
}