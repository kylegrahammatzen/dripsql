// Wire format: magic DRIPV4S1, column-major page payloads, footer with column directory
// and fixed-size 32B page entries. Page field order is u64s first so an unsafe.Pointer cast
// from a 32B wire slice yields the same bytes as a binary.LittleEndian round-trip.
package storage

import (
	"encoding/binary"
	"unsafe"
)

const Magic = "DRIPV4S2"

const MagicLen = 8

// 32 pinned so the directory is pointer-castable via *(*Page)(unsafe.Pointer(&buf[i*32])).
const PageEntrySize = 32

// Field order: u64 fields first so the in-memory layout matches the wire bytes on 64-bit LE.
type Page struct {
	PayloadOffset uint64
	PayloadLength uint64
	RowStart      uint32
	Rows          uint32
	NullCount     uint32
	Kind          uint8
	Encoding      uint8
	Flags         uint8
	Reserved      uint8
}

// Page.Flags bits. Bits 4-7 reserved.
const (
	PageFlagAllValid      uint8 = 1 << 0
	PageFlagAllNull       uint8 = 1 << 1
	PageFlagInMembership  uint8 = 1 << 2
	PageFlagEncodedEvalOK uint8 = 1 << 3
)

const FooterSuffixSize = 16

func WritePage(dst []byte, p Page) {
	_ = dst[PageEntrySize-1]
	binary.LittleEndian.PutUint64(dst[0:8], p.PayloadOffset)
	binary.LittleEndian.PutUint64(dst[8:16], p.PayloadLength)
	binary.LittleEndian.PutUint32(dst[16:20], p.RowStart)
	binary.LittleEndian.PutUint32(dst[20:24], p.Rows)
	binary.LittleEndian.PutUint32(dst[24:28], p.NullCount)
	dst[28] = p.Kind
	dst[29] = p.Encoding
	dst[30] = p.Flags
	dst[31] = p.Reserved
}

// Pointer cast is safe on 64-bit LE (amd64/arm64) where struct layout matches wire bytes.
func DecodePageEntry(src []byte) Page {
	_ = src[PageEntrySize-1]
	return *(*Page)(unsafe.Pointer(&src[0]))
}

func WriteFooterSuffix(dst []byte, footerLength uint64) {
	_ = dst[FooterSuffixSize-1]
	binary.LittleEndian.PutUint64(dst[0:8], footerLength)
	copy(dst[8:16], Magic)
}

func ReadFooterSuffix(src []byte) (footerLength uint64, magicOK bool) {
	_ = src[FooterSuffixSize-1]
	return binary.LittleEndian.Uint64(src[0:8]), string(src[8:16]) == Magic
}
