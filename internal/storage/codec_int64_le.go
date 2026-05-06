//go:build 386 || amd64 || arm || arm64 || loong64 || mipsle || mips64le || ppc64le || riscv64 || wasm

package storage

import "unsafe"

func copyInt64ToBytes(dst []byte, src []int64) {
	// unsafe: vector.Int64 stores values in one contiguous []int64 and this file only builds on little-endian targets.
	bytes := unsafe.Slice((*byte)(unsafe.Pointer(&src[0])), len(src)*8)
	copy(dst, bytes)
}

func copyBytesToInt64(dst []int64, src []byte) {
	// unsafe: dst is one contiguous []int64 and this file only builds on little-endian targets.
	bytes := unsafe.Slice((*byte)(unsafe.Pointer(&dst[0])), len(dst)*8)
	copy(bytes, src)
}
