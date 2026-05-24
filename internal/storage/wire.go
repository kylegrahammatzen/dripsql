// Wire format magic, page entry layout, footer suffix, and the little-endian
// wireBuffer / wireReader helpers shared by the segment encoder and Sidecar[T].
package storage

import (
	"encoding/binary"
	"fmt"
	"unsafe"
)

const Magic = "DRIPV4S3"

const MagicLen = 8

// 32 pinned so the directory is pointer-castable via *(*Page)(unsafe.Pointer(&buf[i*32])).
const PageEntrySize = 32

// Footer suffix v3 carries sidecarLen + footerLen so the sidecar container can live
// inline in the segment file (between footer and suffix) instead of a separate .sc
// file. One tmp+fsync+rename+dirsync writes everything together.

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

const FooterSuffixSize = 24

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

func WriteFooterSuffix(dst []byte, footerLength, sidecarLength uint64) {
	_ = dst[FooterSuffixSize-1]
	binary.LittleEndian.PutUint64(dst[0:8], sidecarLength)
	binary.LittleEndian.PutUint64(dst[8:16], footerLength)
	copy(dst[16:24], Magic)
}

func ReadFooterSuffix(src []byte) (footerLength, sidecarLength uint64, magicOK bool) {
	_ = src[FooterSuffixSize-1]
	sidecarLength = binary.LittleEndian.Uint64(src[0:8])
	footerLength = binary.LittleEndian.Uint64(src[8:16])
	magicOK = string(src[16:24]) == Magic
	return
}

type wireBuffer struct {
	buf []byte
}

func newWireBuffer(cap int) *wireBuffer {
	return &wireBuffer{buf: make([]byte, 0, cap)}
}

func (w *wireBuffer) Bytes() []byte { return w.buf }

func (w *wireBuffer) U8(v uint8) { w.buf = append(w.buf, v) }

func (w *wireBuffer) U16(v uint16) {
	w.buf = binary.LittleEndian.AppendUint16(w.buf, v)
}

func (w *wireBuffer) U32(v uint32) {
	w.buf = binary.LittleEndian.AppendUint32(w.buf, v)
}

func (w *wireBuffer) U64(v uint64) {
	w.buf = binary.LittleEndian.AppendUint64(w.buf, v)
}

func (w *wireBuffer) Raw(b []byte) { w.buf = append(w.buf, b...) }

func (w *wireBuffer) LenPrefixedString(s string) {
	if len(s) > 0xFFFF {
		panic(fmt.Sprintf("wireBuffer: string length %d exceeds u16 wire field", len(s)))
	}
	w.U16(uint16(len(s)))
	w.buf = append(w.buf, s...)
}

type wireReader struct {
	buf []byte
	pos int
	err error
}

func newWireReader(buf []byte) *wireReader { return &wireReader{buf: buf} }

func (r *wireReader) Err() error { return r.err }

func (r *wireReader) Remaining() int { return len(r.buf) - r.pos }

func (r *wireReader) AtEnd() bool { return r.err == nil && r.pos == len(r.buf) }

func (r *wireReader) need(n int) bool {
	if r.err != nil {
		return false
	}
	if n < 0 {
		r.err = fmt.Errorf("wireReader: negative read length %d", n)
		return false
	}
	if r.pos+n > len(r.buf) {
		r.err = fmt.Errorf("wireReader: short read at pos %d, need %d, have %d", r.pos, n, len(r.buf)-r.pos)
		return false
	}
	return true
}

func (r *wireReader) U8() uint8 {
	if !r.need(1) {
		return 0
	}
	v := r.buf[r.pos]
	r.pos++
	return v
}

func (r *wireReader) U16() uint16 {
	if !r.need(2) {
		return 0
	}
	v := binary.LittleEndian.Uint16(r.buf[r.pos : r.pos+2])
	r.pos += 2
	return v
}

func (r *wireReader) U32() uint32 {
	if !r.need(4) {
		return 0
	}
	v := binary.LittleEndian.Uint32(r.buf[r.pos : r.pos+4])
	r.pos += 4
	return v
}

func (r *wireReader) U64() uint64 {
	if !r.need(8) {
		return 0
	}
	v := binary.LittleEndian.Uint64(r.buf[r.pos : r.pos+8])
	r.pos += 8
	return v
}

func (r *wireReader) Raw(n int) []byte {
	if !r.need(n) {
		return nil
	}
	v := r.buf[r.pos : r.pos+n]
	r.pos += n
	return v
}

func (r *wireReader) LenPrefixedString() string {
	n := int(r.U16())
	b := r.Raw(n)
	return string(b)
}
