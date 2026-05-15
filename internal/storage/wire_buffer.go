// wireBuffer and wireReader are little-endian helpers for the footer encoder/decoder.
// Keep the segment writer/reader free of repeated binary.LittleEndian boilerplate.
package storage

import (
	"encoding/binary"
	"fmt"
)

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
