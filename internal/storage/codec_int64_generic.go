//go:build !(386 || amd64 || arm || arm64 || loong64 || mipsle || mips64le || ppc64le || riscv64 || wasm)

package storage

import "encoding/binary"

func copyInt64ToBytes(dst []byte, src []int64) {
	for i, value := range src {
		binary.LittleEndian.PutUint64(dst[i*8:], uint64(value))
	}
}

func copyBytesToInt64(dst []int64, src []byte) {
	for i := range dst {
		dst[i] = int64(binary.LittleEndian.Uint64(src[i*8:]))
	}
}
