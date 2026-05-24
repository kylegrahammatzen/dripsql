// Dictionary codec for varbytes kinds: stores each distinct value once, indexed by u8 per row.
// Wire: [u16 LE dictCount][u32 LE len + bytes per entry][u8 indices x rows]. Rejects above DictMaxValues.
package codec

import (
	"encoding/binary"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

const DictMaxValues = 256

const dictHeaderSize = 2

type dictionaryCodec struct{}

func init() {
	Register(dictionaryCodec{})
}

func (dictionaryCodec) Encoding() types.Encoding { return types.EncodingDictionary }

func (c dictionaryCodec) Encode(v types.Vec, ctx *EncodeContext) ([]byte, error) {
	if !v.Kind.IsVarBytes() || v.Len == 0 {
		return nil, ErrSkip
	}
	var (
		indices   []byte
		entries   [][]byte
		dictBytes int
	)
	if ctx != nil && ctx.Facts != nil && ctx.Facts.VarBytes != nil && ctx.Facts.VarBytes.DictFits {
		f := ctx.Facts.VarBytes
		indices = f.DictIndices
		entries = f.DictEntries
		dictBytes = f.DictBytes
	} else {
		var ok bool
		indices, entries, dictBytes, ok = buildDict(v)
		if !ok {
			return nil, ErrSkip
		}
	}
	rows := int(v.Len)
	n := dictHeaderSize + dictBytes + rows
	scratch := ctxTrial(ctx)
	if cap(scratch) < n {
		scratch = make([]byte, n)
	} else {
		scratch = scratch[:n]
	}
	binary.LittleEndian.PutUint16(scratch[0:2], uint16(len(entries)))
	pos := dictHeaderSize
	for _, entry := range entries {
		binary.LittleEndian.PutUint32(scratch[pos:pos+4], uint32(len(entry)))
		pos += 4
		copy(scratch[pos:pos+len(entry)], entry)
		pos += len(entry)
	}
	copy(scratch[pos:pos+rows], indices)
	return scratch, nil
}

func (dictionaryCodec) Decode(payload []byte, kind types.VecKind, rows, nullCount int, dst *types.Vec) error {
	if err := validateDecodeArgs(rows, nullCount); err != nil {
		return fmt.Errorf("dictionary decode: %w", err)
	}
	if !kind.IsVarBytes() {
		return fmt.Errorf("dictionary decode: kind %v not varbytes", kind)
	}
	if rows == 0 {
		if len(payload) != 0 {
			return fmt.Errorf("dictionary decode: zero rows but %d-byte payload", len(payload))
		}
		*dst = types.NewVarVec(kind, 0, 0)
		return nil
	}
	if len(payload) < dictHeaderSize {
		return fmt.Errorf("dictionary decode: header truncated, have %d", len(payload))
	}
	dictCount := int(binary.LittleEndian.Uint16(payload[0:2]))
	if dictCount == 0 || dictCount > DictMaxValues {
		return fmt.Errorf("dictionary decode: dictCount %d out of range [1, %d]", dictCount, DictMaxValues)
	}
	pos := dictHeaderSize
	entryStarts := make([]int, dictCount)
	entryLens := make([]int, dictCount)
	totalLong := 0
	for i := range dictCount {
		if pos+4 > len(payload) {
			return fmt.Errorf("dictionary decode: entry %d length truncated", i)
		}
		length := int(binary.LittleEndian.Uint32(payload[pos : pos+4]))
		pos += 4
		if pos+length > len(payload) {
			return fmt.Errorf("dictionary decode: entry %d bytes truncated", i)
		}
		entryStarts[i] = pos
		entryLens[i] = length
		if length > types.StringViewInlineMax {
			totalLong += length
		}
		pos += length
	}
	if pos+rows != len(payload) {
		return fmt.Errorf("dictionary decode: indices payload %d != expected %d", len(payload)-pos, rows)
	}
	indices := payload[pos : pos+rows]
	*dst = types.NewVarVec(kind, rows, 0)
	vb := dst.Var()
	dictViews := make([]types.StringView, dictCount)
	if totalLong > 0 {
		sharedBuf := make([]byte, 0, totalLong)
		offsets := make([]uint32, dictCount)
		for i := range dictCount {
			length := entryLens[i]
			if length <= types.StringViewInlineMax {
				continue
			}
			offsets[i] = uint32(len(sharedBuf))
			sharedBuf = append(sharedBuf, payload[entryStarts[i]:entryStarts[i]+length]...)
		}
		bufID := vb.AttachBuffer(sharedBuf)
		for i := range dictCount {
			length := entryLens[i]
			entry := payload[entryStarts[i] : entryStarts[i]+length]
			if length <= types.StringViewInlineMax {
				dictViews[i] = vb.PrepareDictEntry(entry)
				continue
			}
			dictViews[i] = types.MakeLongView(sharedBuf, bufID, offsets[i], uint32(length))
		}
	} else {
		for i := range dictCount {
			entry := payload[entryStarts[i] : entryStarts[i]+entryLens[i]]
			dictViews[i] = vb.PrepareDictEntry(entry)
		}
	}
	for row := range rows {
		idx := int(indices[row])
		if idx >= dictCount {
			return fmt.Errorf("dictionary decode: row %d index %d >= dictCount %d", row, idx, dictCount)
		}
		vb.SetView(row, dictViews[idx])
	}
	return nil
}

func buildDict(v types.Vec) (indices []byte, entries [][]byte, dictBytes int, ok bool) {
	rows := int(v.Len)
	vb := v.Var()
	indices = make([]byte, rows)
	entries = make([][]byte, 0, 16)
	seen := make(map[string]uint8, DictMaxValues)
	for i := range rows {
		val := vb.Bytes(i)
		key := string(val)
		if idx, exists := seen[key]; exists {
			indices[i] = idx
			continue
		}
		if len(entries) >= DictMaxValues {
			return nil, nil, 0, false
		}
		idx := uint8(len(entries))
		seen[key] = idx
		indices[i] = idx
		entries = append(entries, val)
		dictBytes += 4 + len(val)
	}
	return indices, entries, dictBytes, true
}
