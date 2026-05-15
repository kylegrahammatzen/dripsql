// Predicate-on-encoded paths. EvalEncoded narrows a SelectionMask directly from the
// encoded page bytes so a predicate-only column never has to be fully decoded. Returns
// handled=false when the encoding or predicate shape is not supported and the scan layer
// must fall back to full decode + Eval.
package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type EncodedEvaluator interface {
	EvalEncoded(seg *Segment, pageIdx int, sel *types.SelectionMask, scratch []byte) (handled bool, scratchOut []byte, err error)
}

func (b boundEqBytes) EvalEncoded(seg *Segment, pageIdx int, sel *types.SelectionMask, scratch []byte) (bool, []byte, error) {
	colIdx, ok := findSegmentColumnIdx(seg, b.column)
	if !ok {
		return false, scratch, nil
	}
	page := seg.Cols[colIdx].Pages[pageIdx]
	enc, ok := types.EncodingFromWire(page.Encoding)
	if !ok || enc != types.EncodingDictionary {
		return false, scratch, nil
	}
	rows := int(page.Rows)
	ensureMaskSize(sel, rows)
	if page.Flags&PageFlagAllNull != 0 {
		return true, scratch, nil
	}
	payload, valid, allNull, newScratch, err := seg.ReadPagePayload(colIdx, pageIdx, scratch)
	if err != nil {
		return false, newScratch, err
	}
	if allNull {
		return true, newScratch, nil
	}
	code, indices, found, err := resolveDictCode(payload, rows, b.value)
	if err != nil {
		return false, newScratch, err
	}
	if !found {
		return true, newScratch, nil
	}
	narrowDictEq(indices, code, valid, sel)
	return true, newScratch, nil
}

func findSegmentColumnIdx(seg *Segment, name string) (int, bool) {
	for i := range seg.Cols {
		if equalFoldFast(seg.Cols[i].Name, name) {
			return i, true
		}
	}
	return -1, false
}

func equalFoldFast(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range len(a) {
		ca, cb := a[i], b[i]
		if ca == cb {
			continue
		}
		if 'A' <= ca && ca <= 'Z' {
			ca += 32
		}
		if 'A' <= cb && cb <= 'Z' {
			cb += 32
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// resolveDictCode walks a dictionary-encoded payload, finds the entry that matches lit, and
// returns its u8 code plus the indices slice. Layout matches codec/dictionary.go:
// [u16 LE dictCount][u32 LE len + bytes per entry][u8 indices x rows].
func resolveDictCode(payload []byte, rows int, lit []byte) (code uint8, indices []byte, found bool, err error) {
	const dictHeaderSize = 2
	if len(payload) < dictHeaderSize {
		return 0, nil, false, fmt.Errorf("EvalEncoded dict: header truncated")
	}
	dictCount := int(binary.LittleEndian.Uint16(payload[0:2]))
	if dictCount == 0 || dictCount > 256 {
		return 0, nil, false, fmt.Errorf("EvalEncoded dict: dictCount %d out of range", dictCount)
	}
	pos := dictHeaderSize
	for i := range dictCount {
		if pos+4 > len(payload) {
			return 0, nil, false, fmt.Errorf("EvalEncoded dict: entry %d length truncated", i)
		}
		length := int(binary.LittleEndian.Uint32(payload[pos : pos+4]))
		pos += 4
		if pos+length > len(payload) {
			return 0, nil, false, fmt.Errorf("EvalEncoded dict: entry %d bytes truncated", i)
		}
		if !found && length == len(lit) && bytesEqual(payload[pos:pos+length], lit) {
			code = uint8(i)
			found = true
		}
		pos += length
	}
	if pos+rows != len(payload) {
		return 0, nil, false, fmt.Errorf("EvalEncoded dict: indices payload mismatch")
	}
	indices = payload[pos : pos+rows]
	return code, indices, found, nil
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range len(a) {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// narrowDictEq sets sel[i] when indices[i] == code, gated by validity. The indices are
// uint8 per row; the inner loop is a byte compare so 8 rows fit in a SIMDable lane.
func narrowDictEq(indices []byte, code uint8, valid types.Validity, sel *types.SelectionMask) {
	sel.Clear()
	rows := len(indices)
	for i := range rows {
		if indices[i] == code {
			sel.Set(i)
		}
	}
	if valid != nil {
		applyValidity(valid, sel, rows)
	}
}

func applyValidity(valid types.Validity, sel *types.SelectionMask, rows int) {
	for i := range rows {
		if !valid.IsValid(i) {
			sel.Unset(i)
		}
	}
}
