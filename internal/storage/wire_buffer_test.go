// Tests for the wireBuffer / wireReader helpers used by the footer encoder / decoder.
package storage

import (
	"strings"
	"testing"
)

func TestWireBuffer_LenPrefixedString_RejectsOverlong(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("LenPrefixedString must panic when len(s) > 65535")
		}
	}()
	w := newWireBuffer(64)
	w.LenPrefixedString(strings.Repeat("x", 65536))
}

func TestWireReader_AtEndTrueAfterFullConsume(t *testing.T) {
	w := newWireBuffer(8)
	w.U32(7)
	w.U32(11)
	r := newWireReader(w.Bytes())
	_ = r.U32()
	_ = r.U32()
	if !r.AtEnd() {
		t.Fatal("AtEnd must be true after consuming the full buffer")
	}
}

func TestWireReader_AtEndFalseWithTrailingBytes(t *testing.T) {
	r := newWireReader([]byte{0, 0, 0, 0, 99})
	_ = r.U32()
	if r.AtEnd() {
		t.Fatal("AtEnd must be false when bytes remain")
	}
}

func TestWireReader_ShortReadSetsErr(t *testing.T) {
	r := newWireReader([]byte{0, 0})
	_ = r.U32()
	if r.Err() == nil {
		t.Fatal("Err must be set on short read")
	}
}
