// Varbytes wire helpers shared by plain, compressed_text, and dictionary codecs.
// Format is <u32 len><bytes> per row. Total size is the source of truth for payload sizing.
package codec

import (
	"encoding/binary"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func varbytesWireSize(v types.Vec) int {
	rows := int(v.Len)
	n := 4 * rows
	vb := v.Var()
	for i := range rows {
		n += vb.Len(i)
	}
	return n
}

func writeVarbytesWire(dst []byte, v types.Vec) {
	rows := int(v.Len)
	vb := v.Var()
	pos := 0
	for i := range rows {
		bs := vb.Bytes(i)
		binary.LittleEndian.PutUint32(dst[pos:pos+4], uint32(len(bs)))
		pos += 4
		copy(dst[pos:pos+len(bs)], bs)
		pos += len(bs)
	}
}

func readVarbytesWire(src []byte, kind types.VecKind, rows int, dst *types.Vec) error {
	dataHint := max(len(src)-4*rows, 0)
	*dst = types.NewVarVec(kind, rows, dataHint)
	vb := dst.Var()
	pos := 0
	for i := range rows {
		if pos+4 > len(src) {
			return fmt.Errorf("varbytes wire: header truncated at row %d", i)
		}
		length := int(binary.LittleEndian.Uint32(src[pos : pos+4]))
		pos += 4
		if pos+length > len(src) {
			return fmt.Errorf("varbytes wire: value truncated at row %d", i)
		}
		vb.AppendBytes(i, src[pos:pos+length])
		pos += length
	}
	if pos != len(src) {
		return fmt.Errorf("varbytes wire: %d trailing bytes after %d rows", len(src)-pos, rows)
	}
	return nil
}
