// VarBytes is German Strings for text/bytes/JSON: 16-byte StringView per row.
// bufId 0 references own data, bufId >= 1 references extras[bufId-1] (dict-shared).
package types

import (
	"encoding/binary"
	"fmt"
	"math"
	"unsafe"
)

const StringViewInlineMax = 12

// Length+Prefix+Body. Body is inline tail bytes when Length <= 12, otherwise (bufId, offset).
type StringView struct {
	Length uint32
	Prefix [4]byte
	Body   [8]byte
}

type VarBytes struct {
	views  []StringView
	data   []byte
	extras [][]byte
}

func NewVarBytes(rows int, dataBytes int) VarBytes {
	return VarBytes{
		views: make([]StringView, rows),
		data:  make([]byte, 0, dataBytes),
	}
}

func (v VarBytes) Rows() int { return len(v.views) }

func (v VarBytes) Len(row int) int { return int(v.views[row].Length) }

func (v VarBytes) Prefix(row int) uint32 {
	return binary.LittleEndian.Uint32(v.views[row].Prefix[:])
}

func (v VarBytes) Bytes(row int) []byte {
	view := &v.views[row]
	length := int(view.Length)
	if length <= StringViewInlineMax {
		return unsafe.Slice((*byte)(unsafe.Pointer(&view.Prefix[0])), length)
	}
	bufID := binary.LittleEndian.Uint32(view.Body[0:4])
	offset := binary.LittleEndian.Uint32(view.Body[4:8])
	buf := v.bufferFor(bufID)
	return buf[offset : offset+uint32(length)]
}

func (v VarBytes) String(row int) string {
	return string(v.Bytes(row))
}

func (v *VarBytes) AppendBytes(row int, value []byte) {
	v.views[row] = v.materialize(value)
}

func (v *VarBytes) AppendString(row int, value string) {
	v.AppendBytes(row, unsafe.Slice(unsafe.StringData(value), len(value)))
}

func (v *VarBytes) Broadcast(value []byte) {
	proto := v.materialize(value)
	for i := range v.views {
		v.views[i] = proto
	}
}

func (v *VarBytes) PrepareDictEntry(value []byte) StringView {
	return v.materialize(value)
}

func (v *VarBytes) SetView(row int, view StringView) {
	v.views[row] = view
}

func (v *VarBytes) AttachBuffer(buf []byte) uint32 {
	v.extras = append(v.extras, buf)
	return uint32(len(v.extras))
}

// One-shot constructor so callers cannot leave Body unset between MakeLongView and SetBufID.
func MakeLongView(buf []byte, bufID, offset, length uint32) StringView {
	if length <= StringViewInlineMax {
		panic(fmt.Sprintf("MakeLongView: length %d must exceed inline max %d", length, StringViewInlineMax))
	}
	end := uint64(offset) + uint64(length)
	if end > uint64(len(buf)) {
		panic(fmt.Sprintf("MakeLongView: [%d,%d) exceeds buffer len %d", offset, end, len(buf)))
	}
	sv := StringView{Length: length}
	copy(sv.Prefix[:], buf[offset:offset+4])
	sv.SetBufID(bufID, offset)
	return sv
}

func (sv *StringView) SetBufID(bufID, offset uint32) {
	binary.LittleEndian.PutUint32(sv.Body[0:4], bufID)
	binary.LittleEndian.PutUint32(sv.Body[4:8], offset)
}

func (v VarBytes) Clone() VarBytes {
	out := VarBytes{
		views: make([]StringView, len(v.views)),
		data:  make([]byte, len(v.data)),
	}
	copy(out.views, v.views)
	copy(out.data, v.data)
	if len(v.extras) > 0 {
		out.extras = make([][]byte, len(v.extras))
		for i, e := range v.extras {
			out.extras[i] = append([]byte(nil), e...)
		}
	}
	return out
}

func (v *VarBytes) materialize(value []byte) StringView {
	if uint64(len(value)) > math.MaxUint32 {
		panic(fmt.Sprintf("varbytes: value length %d exceeds uint32", len(value)))
	}
	sv := StringView{Length: uint32(len(value))}
	if len(value) <= StringViewInlineMax {
		viewBytes := unsafe.Slice((*byte)(unsafe.Pointer(&sv.Prefix[0])), StringViewInlineMax)
		copy(viewBytes, value)
		return sv
	}
	if uint64(len(v.data))+uint64(len(value)) > math.MaxUint32 {
		panic(fmt.Sprintf("varbytes: buffer would overflow uint32 at offset %d adding %d bytes", len(v.data), len(value)))
	}
	offset := uint32(len(v.data))
	v.data = append(v.data, value...)
	copy(sv.Prefix[:], value[:4])
	sv.SetBufID(0, offset)
	return sv
}

func (v VarBytes) bufferFor(bufID uint32) []byte {
	if bufID == 0 {
		return v.data
	}
	return v.extras[bufID-1]
}
