// BoundPredicate plus the bound types are internal eval helpers reached only via Pred.toBound.
// All shape and binding live in pred.go; helpers below resolve segment columns, stats, and encoded paths.
package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/storage/codec"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type BoundPredicate interface {
	Eval(batch vector.Batch, sel *vector.SelectionMask)
	PruneSegment(seg *Segment) bool
	PrunePage(seg *Segment, pageIdx int) bool
}

type SchemaLookup func(name string) (vector.VecKind, bool)

func BatchSchema(batch vector.Batch) SchemaLookup {
	return func(name string) (vector.VecKind, bool) {
		c, ok := batch.ColumnByName(name)
		if !ok {
			return 0, false
		}
		return c.V.Kind, true
	}
}

func SegmentSchema(seg *Segment) SchemaLookup {
	return func(name string) (vector.VecKind, bool) {
		for i := range seg.Cols {
			if strings.EqualFold(seg.Cols[i].Name, name) {
				return seg.Cols[i].Kind, true
			}
		}
		return 0, false
	}
}

func ColumnsSchema(cols []vector.Column) SchemaLookup {
	return func(name string) (vector.VecKind, bool) {
		for i := range cols {
			if strings.EqualFold(cols[i].Name, name) {
				k, err := vector.VecKindOf(cols[i].Type)
				if err != nil {
					return 0, false
				}
				return k, true
			}
		}
		return 0, false
	}
}

func checkKind(lookup SchemaLookup, name string, want vector.VecKind) error {
	kind, ok := lookup(name)
	if !ok {
		return fmt.Errorf("column %q not in schema", name)
	}
	if kind != want {
		return fmt.Errorf("column %q kind %v != predicate kind %v", name, kind, want)
	}
	return nil
}

func ensureMaskSize(sel *vector.SelectionMask, rows int) {
	if sel.Rows() != rows {
		sel.Resize(rows)
		return
	}
	sel.Clear()
}

func findSegmentColumn(seg *Segment, name string) (*SegmentColumn, bool) {
	for i := range seg.Cols {
		if strings.EqualFold(seg.Cols[i].Name, name) {
			return &seg.Cols[i], true
		}
	}
	return nil, false
}

func numericStatsFromCol(c *SegmentColumn) (NumericStats[int64], bool) {
	if c.Rows == 0 {
		return NumericStats[int64]{}, false
	}
	return UnmarshalNumericStats[int64](c.Stats[:], true), true
}

// pageMinMaxInt returns the int64 min/max for a page when the column kind populates
// page stats with integral values. int16/int32/date are stored as NumericStats[int32]
// in 16B (4+4+pad), int64/timestamp/time/decimal64 as NumericStats[int64] (8+8).
// Reading the wrong shape would silently misinterpret bytes, so kind dispatch matters.
func pageMinMaxInt(c *SegmentColumn, pageIdx int) (min, max int64, ok bool) {
	if pageIdx < 0 || pageIdx >= len(c.PageStats) {
		return 0, 0, false
	}
	page := c.Pages[pageIdx]
	if page.Rows == 0 || page.Flags&PageFlagAllNull != 0 {
		return 0, 0, false
	}
	switch c.Kind {
	case vector.VecInt16, vector.VecInt32, vector.VecDate:
		s := UnmarshalNumericStats[int32](c.PageStats[pageIdx][:], true)
		return int64(s.Min), int64(s.Max), true
	case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
		s := UnmarshalNumericStats[int64](c.PageStats[pageIdx][:], true)
		return s.Min, s.Max, true
	}
	return 0, 0, false
}

type boundEqInt64 struct {
	column string
	value  int64
}

func (b boundEqInt64) Eval(batch vector.Batch, sel *vector.SelectionMask) {
	ensureMaskSize(sel, batch.Len)
	col, ok := batch.ColumnByName(b.column)
	if !ok {
		return
	}
	sel.FillAll()
	vector.FilterOrdered(col.V.I64(), col.V.Valid, b.value, vector.FilterEqual, *sel, sel)
}

func (b boundEqInt64) PruneSegment(seg *Segment) bool {
	c, ok := findSegmentColumn(seg, b.column)
	if !ok {
		return false
	}
	stats, present := numericStatsFromCol(c)
	if !present {
		return true
	}
	if !stats.HasNonNull {
		return false
	}
	if b.value < stats.Min || b.value > stats.Max {
		return true
	}
	filters, err := seg.IntFilterSet()
	if err != nil {
		return false
	}
	if f, ok := filters[schema.NormalizeName(b.column)]; ok && !f.Contains(b.value) {
		return true
	}
	return false
}

func (b boundEqInt64) PrunePage(seg *Segment, pageIdx int) bool {
	c, ok := findSegmentColumn(seg, b.column)
	if !ok {
		return false
	}
	min, max, ok := pageMinMaxInt(c, pageIdx)
	if !ok {
		return false
	}
	return b.value < min || b.value > max
}

type boundLtInt64 struct {
	column string
	value  int64
}

func (b boundLtInt64) Eval(batch vector.Batch, sel *vector.SelectionMask) {
	ensureMaskSize(sel, batch.Len)
	col, ok := batch.ColumnByName(b.column)
	if !ok {
		return
	}
	sel.FillAll()
	vector.FilterOrdered(col.V.I64(), col.V.Valid, b.value, vector.FilterLess, *sel, sel)
}

func (b boundLtInt64) PruneSegment(seg *Segment) bool {
	c, ok := findSegmentColumn(seg, b.column)
	if !ok {
		return false
	}
	stats, present := numericStatsFromCol(c)
	if !present {
		return true
	}
	if !stats.HasNonNull {
		return false
	}
	return stats.Min >= b.value
}

func (b boundLtInt64) PrunePage(seg *Segment, pageIdx int) bool {
	c, ok := findSegmentColumn(seg, b.column)
	if !ok {
		return false
	}
	min, _, ok := pageMinMaxInt(c, pageIdx)
	if !ok {
		return false
	}
	return min >= b.value
}

type boundGtInt64 struct {
	column string
	value  int64
}

func (b boundGtInt64) Eval(batch vector.Batch, sel *vector.SelectionMask) {
	ensureMaskSize(sel, batch.Len)
	col, ok := batch.ColumnByName(b.column)
	if !ok {
		return
	}
	sel.FillAll()
	vector.FilterOrdered(col.V.I64(), col.V.Valid, b.value, vector.FilterGreater, *sel, sel)
}

func (b boundGtInt64) PruneSegment(seg *Segment) bool {
	c, ok := findSegmentColumn(seg, b.column)
	if !ok {
		return false
	}
	stats, present := numericStatsFromCol(c)
	if !present {
		return true
	}
	if !stats.HasNonNull {
		return false
	}
	return stats.Max <= b.value
}

func (b boundGtInt64) PrunePage(seg *Segment, pageIdx int) bool {
	c, ok := findSegmentColumn(seg, b.column)
	if !ok {
		return false
	}
	_, max, ok := pageMinMaxInt(c, pageIdx)
	if !ok {
		return false
	}
	return max <= b.value
}

type boundEqBytes struct {
	column string
	value  []byte
}

func (b boundEqBytes) Eval(batch vector.Batch, sel *vector.SelectionMask) {
	ensureMaskSize(sel, batch.Len)
	col, ok := batch.ColumnByName(b.column)
	if !ok {
		return
	}
	sel.FillAll()
	vector.FilterBytes(col.V.Var(), col.V.Valid, b.value, vector.FilterEqual, *sel, sel)
}

func (b boundEqBytes) PruneSegment(seg *Segment) bool {
	c, ok := findSegmentColumn(seg, b.column)
	if !ok {
		return false
	}
	if c.Rows == 0 {
		return true
	}
	hists, err := seg.DictHistograms()
	if err != nil || hists == nil {
		return false
	}
	hist, ok := hists[schema.NormalizeName(b.column)]
	if !ok {
		return false
	}
	if _, present := hist[string(b.value)]; present {
		return false
	}
	return true
}

func (b boundEqBytes) PrunePage(seg *Segment, pageIdx int) bool {
	c, ok := findSegmentColumn(seg, b.column)
	if !ok {
		return false
	}
	if pageIdx < 0 || pageIdx >= len(c.Pages) {
		return false
	}
	page := c.Pages[pageIdx]
	if page.Flags&PageFlagAllNull != 0 {
		return true
	}
	blooms, err := seg.VarBlooms()
	if err != nil || blooms == nil {
		return false
	}
	vb, ok := blooms[schema.NormalizeName(b.column)]
	if !ok || vb == nil || pageIdx >= len(vb.Pages) {
		return false
	}
	pb := vb.Pages[pageIdx]
	if pb == nil {
		return false
	}
	return !pb.contains(hashBytesFNV(b.value))
}

type boundLtBytes struct {
	column    string
	value     []byte
	inclusive bool
}

func (b boundLtBytes) Eval(batch vector.Batch, sel *vector.SelectionMask) {
	ensureMaskSize(sel, batch.Len)
	col, ok := batch.ColumnByName(b.column)
	if !ok {
		return
	}
	sel.FillAll()
	op := vector.FilterLess
	if b.inclusive {
		op = vector.FilterLessEqual
	}
	vector.FilterBytes(col.V.Var(), col.V.Valid, b.value, op, *sel, sel)
}

func (boundLtBytes) PruneSegment(*Segment) bool   { return false }
func (boundLtBytes) PrunePage(*Segment, int) bool { return false }

type boundGtBytes struct {
	column    string
	value     []byte
	inclusive bool
}

func (b boundGtBytes) Eval(batch vector.Batch, sel *vector.SelectionMask) {
	ensureMaskSize(sel, batch.Len)
	col, ok := batch.ColumnByName(b.column)
	if !ok {
		return
	}
	sel.FillAll()
	op := vector.FilterGreater
	if b.inclusive {
		op = vector.FilterGreaterEqual
	}
	vector.FilterBytes(col.V.Var(), col.V.Valid, b.value, op, *sel, sel)
}

func (boundGtBytes) PruneSegment(*Segment) bool   { return false }
func (boundGtBytes) PrunePage(*Segment, int) bool { return false }

type boundIsNull struct{ column string }

func (b boundIsNull) Eval(batch vector.Batch, sel *vector.SelectionMask) {
	ensureMaskSize(sel, batch.Len)
	col, ok := batch.ColumnByName(b.column)
	if !ok || col.V.Valid == nil {
		return
	}
	rows := int(col.V.Len)
	valid := col.V.Valid
	for i := range rows {
		if !valid.IsValid(i) {
			sel.Set(i)
		}
	}
}

func (b boundIsNull) PruneSegment(seg *Segment) bool {
	c, ok := findSegmentColumn(seg, b.column)
	if !ok {
		return false
	}
	return c.NullCount == 0
}

func (b boundIsNull) PrunePage(seg *Segment, pageIdx int) bool {
	c, ok := findSegmentColumn(seg, b.column)
	if !ok {
		return false
	}
	if pageIdx < 0 || pageIdx >= len(c.Pages) {
		return false
	}
	return c.Pages[pageIdx].NullCount == 0
}

type boundAnd struct {
	children []BoundPredicate
	tmp      vector.SelectionMask
}

func (b *boundAnd) Eval(batch vector.Batch, sel *vector.SelectionMask) {
	if len(b.children) == 0 {
		ensureMaskSize(sel, batch.Len)
		sel.FillAll()
		return
	}
	b.children[0].Eval(batch, sel)
	for _, c := range b.children[1:] {
		c.Eval(batch, &b.tmp)
		sel.AndCount(b.tmp)
	}
}

func (b *boundAnd) PruneSegment(seg *Segment) bool {
	for _, c := range b.children {
		if c.PruneSegment(seg) {
			return true
		}
	}
	return false
}

func (b *boundAnd) PrunePage(seg *Segment, pageIdx int) bool {
	for _, c := range b.children {
		if c.PrunePage(seg, pageIdx) {
			return true
		}
	}
	return false
}

type boundOr struct {
	children []BoundPredicate
	tmp      vector.SelectionMask
}

func (b *boundOr) Eval(batch vector.Batch, sel *vector.SelectionMask) {
	if len(b.children) == 0 {
		ensureMaskSize(sel, batch.Len)
		return
	}
	b.children[0].Eval(batch, sel)
	for _, c := range b.children[1:] {
		c.Eval(batch, &b.tmp)
		sel.OrCount(b.tmp)
	}
}

func (b boundOr) PruneSegment(seg *Segment) bool {
	for _, c := range b.children {
		if !c.PruneSegment(seg) {
			return false
		}
	}
	return true
}

func (b boundOr) PrunePage(seg *Segment, pageIdx int) bool {
	for _, c := range b.children {
		if !c.PrunePage(seg, pageIdx) {
			return false
		}
	}
	return true
}

type boundNot struct{ child BoundPredicate }

func (b boundNot) Eval(batch vector.Batch, sel *vector.SelectionMask) {
	b.child.Eval(batch, sel)
	sel.NotCount()
}

func (b boundNot) PruneSegment(seg *Segment) bool {
	return childAlwaysMatchesSegment(b.child, seg)
}

func (b boundNot) PrunePage(seg *Segment, pageIdx int) bool {
	return childAlwaysMatchesPage(b.child, seg, pageIdx)
}

// childAlwaysMatchesSegment returns true when child is guaranteed to select every
// row in the segment, which lets NOT(child) prune. Sound but conservative: any
// undetermined case returns false.
func childAlwaysMatchesSegment(child BoundPredicate, seg *Segment) bool {
	switch c := child.(type) {
	case boundIsNull:
		col, ok := findSegmentColumn(seg, c.column)
		if !ok || col.Rows == 0 {
			return false
		}
		return col.NullCount == col.Rows
	case boundEqInt64:
		col, ok := findSegmentColumn(seg, c.column)
		if !ok || col.NullCount != 0 {
			return false
		}
		stats, present := numericStatsFromCol(col)
		if !present || !stats.HasNonNull {
			return false
		}
		return stats.Min == c.value && stats.Max == c.value
	case boundLtInt64:
		col, ok := findSegmentColumn(seg, c.column)
		if !ok || col.NullCount != 0 {
			return false
		}
		stats, present := numericStatsFromCol(col)
		if !present || !stats.HasNonNull {
			return false
		}
		return stats.Max < c.value
	case boundGtInt64:
		col, ok := findSegmentColumn(seg, c.column)
		if !ok || col.NullCount != 0 {
			return false
		}
		stats, present := numericStatsFromCol(col)
		if !present || !stats.HasNonNull {
			return false
		}
		return stats.Min > c.value
	}
	return false
}

func childAlwaysMatchesPage(child BoundPredicate, seg *Segment, pageIdx int) bool {
	switch c := child.(type) {
	case boundIsNull:
		col, ok := findSegmentColumn(seg, c.column)
		if !ok || pageIdx < 0 || pageIdx >= len(col.Pages) {
			return false
		}
		p := col.Pages[pageIdx]
		return p.Rows > 0 && p.Flags&PageFlagAllNull != 0
	case boundEqInt64:
		col, ok := findSegmentColumn(seg, c.column)
		if !ok || pageIdx < 0 || pageIdx >= len(col.Pages) {
			return false
		}
		if col.Pages[pageIdx].NullCount != 0 {
			return false
		}
		min, max, ok := pageMinMaxInt(col, pageIdx)
		if !ok {
			return false
		}
		return min == c.value && max == c.value
	case boundLtInt64:
		col, ok := findSegmentColumn(seg, c.column)
		if !ok || pageIdx < 0 || pageIdx >= len(col.Pages) {
			return false
		}
		if col.Pages[pageIdx].NullCount != 0 {
			return false
		}
		_, max, ok := pageMinMaxInt(col, pageIdx)
		if !ok {
			return false
		}
		return max < c.value
	case boundGtInt64:
		col, ok := findSegmentColumn(seg, c.column)
		if !ok || pageIdx < 0 || pageIdx >= len(col.Pages) {
			return false
		}
		if col.Pages[pageIdx].NullCount != 0 {
			return false
		}
		min, _, ok := pageMinMaxInt(col, pageIdx)
		if !ok {
			return false
		}
		return min > c.value
	}
	return false
}

type EncodedEvaluator interface {
	EvalEncoded(seg *Segment, pageIdx int, sel *vector.SelectionMask, scratch []byte) (handled bool, scratchOut []byte, err error)
}

func (b boundEqBytes) EvalEncoded(seg *Segment, pageIdx int, sel *vector.SelectionMask, scratch []byte) (bool, []byte, error) {
	colIdx, ok := findSegmentColumnIdx(seg, b.column)
	if !ok {
		return false, scratch, nil
	}
	page := seg.Cols[colIdx].Pages[pageIdx]
	enc := schema.Encoding(page.Encoding)
	if !enc.Valid() {
		return false, scratch, nil
	}
	switch enc {
	case schema.EncodingDictionary:
		return evalEncodedDictEq(seg, colIdx, pageIdx, sel, scratch, b.value)
	case schema.EncodingFSST:
		return evalEncodedFSSTEq(seg, colIdx, pageIdx, sel, scratch, b.value)
	}
	return false, scratch, nil
}

func evalEncodedDictEq(seg *Segment, colIdx, pageIdx int, sel *vector.SelectionMask, scratch []byte, target []byte) (bool, []byte, error) {
	page := seg.Cols[colIdx].Pages[pageIdx]
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
	code, indices, found, err := resolveDictCode(payload, rows, target)
	if err != nil {
		return false, newScratch, err
	}
	if !found {
		return true, newScratch, nil
	}
	narrowDictEq(indices, code, valid, sel)
	return true, newScratch, nil
}

// evalEncodedFSSTEq encodes target against the page symbol table once and byte-compares each row.
// FSST encoding is deterministic against a fixed symbol table so equality requires no decode.
func evalEncodedFSSTEq(seg *Segment, colIdx, pageIdx int, sel *vector.SelectionMask, scratch []byte, target []byte) (bool, []byte, error) {
	page := seg.Cols[colIdx].Pages[pageIdx]
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
	symbols, rowsOff, encRows, err := codec.ParseFSSTHeader(payload)
	if err != nil {
		return false, newScratch, err
	}
	if encRows != rows {
		return false, newScratch, fmt.Errorf("EvalEncoded FSST: row mismatch wire=%d want=%d", encRows, rows)
	}
	encodedLit := codec.EncodeFSSTLiteral(symbols, target)
	sel.Clear()
	pos := rowsOff
	for i := range rows {
		if pos+4 > len(payload) {
			return false, newScratch, fmt.Errorf("EvalEncoded FSST: row %d length truncated", i)
		}
		encLen := int(binary.LittleEndian.Uint32(payload[pos : pos+4]))
		pos += 4
		if pos+encLen > len(payload) {
			return false, newScratch, fmt.Errorf("EvalEncoded FSST: row %d payload truncated", i)
		}
		if encLen == len(encodedLit) && bytes.Equal(payload[pos:pos+encLen], encodedLit) {
			sel.Set(i)
		}
		pos += encLen
	}
	if valid != nil {
		applyValidity(valid, sel, rows)
	}
	return true, newScratch, nil
}

func (b boundLtBytes) EvalEncoded(seg *Segment, pageIdx int, sel *vector.SelectionMask, scratch []byte) (bool, []byte, error) {
	return evalEncodedDictOrdered(seg, pageIdx, sel, scratch, b.column, b.value, false, b.inclusive)
}

func (b boundGtBytes) EvalEncoded(seg *Segment, pageIdx int, sel *vector.SelectionMask, scratch []byte) (bool, []byte, error) {
	return evalEncodedDictOrdered(seg, pageIdx, sel, scratch, b.column, b.value, true, b.inclusive)
}

// evalEncodedDictOrdered narrows sel for dictionary-encoded text under a LT/GT comparison.
// It builds a 256-bit accept mask by comparing every dict entry against target, then walks
// the index stream once. Same allocation profile as the eq path.
func evalEncodedDictOrdered(seg *Segment, pageIdx int, sel *vector.SelectionMask, scratch []byte, column string, target []byte, greater, inclusive bool) (bool, []byte, error) {
	colIdx, ok := findSegmentColumnIdx(seg, column)
	if !ok {
		return false, scratch, nil
	}
	page := seg.Cols[colIdx].Pages[pageIdx]
	enc := schema.Encoding(page.Encoding)
	if !enc.Valid() || enc != schema.EncodingDictionary {
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
	accept, indices, err := dictAcceptMask(payload, rows, target, greater, inclusive)
	if err != nil {
		return false, newScratch, err
	}
	narrowDictByMask(indices, accept, valid, sel)
	return true, newScratch, nil
}

// dictAcceptMask walks the dictionary header once and returns a 256-bit acceptance mask
// plus the indices slice. Bit i of accept[i>>6]>>(i&63) is set when dict entry i passes
// the LT/GT comparison.
func dictAcceptMask(payload []byte, rows int, target []byte, greater, inclusive bool) (accept [4]uint64, indices []byte, err error) {
	const dictHeaderSize = 2
	if len(payload) < dictHeaderSize {
		return accept, nil, fmt.Errorf("EvalEncoded dict: header truncated")
	}
	dictCount := int(binary.LittleEndian.Uint16(payload[0:2]))
	if dictCount == 0 || dictCount > 256 {
		return accept, nil, fmt.Errorf("EvalEncoded dict: dictCount %d out of range", dictCount)
	}
	pos := dictHeaderSize
	for i := range dictCount {
		if pos+4 > len(payload) {
			return accept, nil, fmt.Errorf("EvalEncoded dict: entry %d length truncated", i)
		}
		length := int(binary.LittleEndian.Uint32(payload[pos : pos+4]))
		pos += 4
		if pos+length > len(payload) {
			return accept, nil, fmt.Errorf("EvalEncoded dict: entry %d bytes truncated", i)
		}
		cmp := bytes.Compare(payload[pos:pos+length], target)
		var pass bool
		if greater {
			pass = cmp > 0 || (inclusive && cmp == 0)
		} else {
			pass = cmp < 0 || (inclusive && cmp == 0)
		}
		if pass {
			accept[i>>6] |= 1 << uint(i&63)
		}
		pos += length
	}
	if pos+rows != len(payload) {
		return accept, nil, fmt.Errorf("EvalEncoded dict: indices payload mismatch")
	}
	indices = payload[pos : pos+rows]
	return accept, indices, nil
}

// narrowDictByMask sets sel[i] when accept[indices[i]] is set, gated by validity.
func narrowDictByMask(indices []byte, accept [4]uint64, valid vector.Validity, sel *vector.SelectionMask) {
	sel.Clear()
	rows := len(indices)
	for i := range rows {
		c := indices[i]
		if accept[c>>6]&(1<<uint(c&63)) != 0 {
			sel.Set(i)
		}
	}
	if valid != nil {
		applyValidity(valid, sel, rows)
	}
}

func (b boundEqInt64) EvalEncoded(seg *Segment, pageIdx int, sel *vector.SelectionMask, scratch []byte) (bool, []byte, error) {
	colIdx, ok := findSegmentColumnIdx(seg, b.column)
	if !ok {
		return false, scratch, nil
	}
	page := seg.Cols[colIdx].Pages[pageIdx]
	enc := schema.Encoding(page.Encoding)
	if !enc.Valid() {
		return false, scratch, nil
	}
	switch enc {
	case schema.EncodingFORBitPack:
		return evalEncodedEqFOR(seg, colIdx, pageIdx, sel, scratch, b.value)
	case schema.EncodingDeltaBitPack:
		return evalEncodedEqDelta(seg, colIdx, pageIdx, sel, scratch, b.value)
	}
	return false, scratch, nil
}

func evalEncodedEqFOR(seg *Segment, colIdx, pageIdx int, sel *vector.SelectionMask, scratch []byte, target int64) (bool, []byte, error) {
	page := seg.Cols[colIdx].Pages[pageIdx]
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
	const forHeaderSize = 9
	if len(payload) < forHeaderSize {
		return false, newScratch, fmt.Errorf("EvalEncoded FOR: header truncated")
	}
	base := int64(binary.LittleEndian.Uint64(payload[0:8]))
	width := int(payload[8])
	if width < 1 || width > 64 {
		return false, newScratch, fmt.Errorf("EvalEncoded FOR: width %d out of range", width)
	}
	residualSigned := target - base
	if residualSigned < 0 {
		sel.Clear()
		return true, newScratch, nil
	}
	residual := uint64(residualSigned)
	if width < 64 && residual >= (uint64(1)<<uint(width)) {
		sel.Clear()
		return true, newScratch, nil
	}
	residuals := make([]uint64, rows)
	codec.Unpack(width, payload[forHeaderSize:], rows, residuals)
	narrowFOREq(residuals, residual, valid, sel)
	return true, newScratch, nil
}

func evalEncodedEqDelta(seg *Segment, colIdx, pageIdx int, sel *vector.SelectionMask, scratch []byte, target int64) (bool, []byte, error) {
	page := seg.Cols[colIdx].Pages[pageIdx]
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
	const deltaHeaderSize = 17
	if len(payload) < deltaHeaderSize {
		return false, newScratch, fmt.Errorf("EvalEncoded delta: header truncated")
	}
	first := int64(binary.LittleEndian.Uint64(payload[0:8]))
	base := int64(binary.LittleEndian.Uint64(payload[8:16]))
	width := int(payload[16])
	if width < 1 || width > 64 {
		return false, newScratch, fmt.Errorf("EvalEncoded delta: width %d out of range", width)
	}
	residuals := make([]uint64, rows-1)
	codec.Unpack(width, payload[deltaHeaderSize:], rows-1, residuals)
	narrowDeltaEq(first, base, residuals, target, valid, sel)
	return true, newScratch, nil
}

func narrowDeltaEq(first, base int64, residuals []uint64, target int64, valid vector.Validity, sel *vector.SelectionMask) {
	sel.Clear()
	cur := first
	if cur == target {
		sel.Set(0)
	}
	for i, r := range residuals {
		cur += int64(r) + base
		if cur == target {
			sel.Set(i + 1)
		}
	}
	if valid != nil {
		applyValidity(valid, sel, len(residuals)+1)
	}
}

func narrowFOREq(residuals []uint64, target uint64, valid vector.Validity, sel *vector.SelectionMask) {
	sel.Clear()
	for i, r := range residuals {
		if r == target {
			sel.Set(i)
		}
	}
	if valid != nil {
		applyValidity(valid, sel, len(residuals))
	}
}

func (b boundLtInt64) EvalEncoded(seg *Segment, pageIdx int, sel *vector.SelectionMask, scratch []byte) (bool, []byte, error) {
	return evalEncodedFORRange(seg, pageIdx, sel, scratch, b.column, b.value, forRangeLT)
}

func (b boundGtInt64) EvalEncoded(seg *Segment, pageIdx int, sel *vector.SelectionMask, scratch []byte) (bool, []byte, error) {
	return evalEncodedFORRange(seg, pageIdx, sel, scratch, b.column, b.value, forRangeGT)
}

type forRangeOp uint8

const (
	forRangeLT forRangeOp = iota
	forRangeGT
)

func evalEncodedFORRange(seg *Segment, pageIdx int, sel *vector.SelectionMask, scratch []byte, column string, limit int64, op forRangeOp) (bool, []byte, error) {
	colIdx, ok := findSegmentColumnIdx(seg, column)
	if !ok {
		return false, scratch, nil
	}
	page := seg.Cols[colIdx].Pages[pageIdx]
	enc := schema.Encoding(page.Encoding)
	if !enc.Valid() || enc != schema.EncodingFORBitPack {
		return false, scratch, nil
	}
	rows := int(page.Rows)
	ensureMaskSize(sel, rows)
	if page.Flags&PageFlagAllNull != 0 {
		sel.Clear()
		return true, scratch, nil
	}
	payload, valid, allNull, newScratch, err := seg.ReadPagePayload(colIdx, pageIdx, scratch)
	if err != nil {
		return false, newScratch, err
	}
	if allNull {
		sel.Clear()
		return true, newScratch, nil
	}
	const forHeaderSize = 9
	if len(payload) < forHeaderSize {
		return false, newScratch, fmt.Errorf("EvalEncoded FOR range: header truncated")
	}
	base := int64(binary.LittleEndian.Uint64(payload[0:8]))
	width := int(payload[8])
	if width < 1 || width > 64 {
		return false, newScratch, fmt.Errorf("EvalEncoded FOR range: width %d out of range", width)
	}
	threshold := limit - base
	var maxResidual uint64
	if width >= 64 {
		maxResidual = ^uint64(0)
	} else {
		maxResidual = (uint64(1) << uint(width)) - 1
	}
	switch op {
	case forRangeLT:
		if threshold <= 0 {
			sel.Clear()
			return true, newScratch, nil
		}
		if uint64(threshold) > maxResidual {
			sel.FillAll()
			if valid != nil {
				applyValidity(valid, sel, rows)
			}
			return true, newScratch, nil
		}
		residuals := make([]uint64, rows)
		codec.Unpack(width, payload[forHeaderSize:], rows, residuals)
		narrowFORLT(residuals, uint64(threshold), valid, sel)
		return true, newScratch, nil
	case forRangeGT:
		if threshold < 0 {
			sel.FillAll()
			if valid != nil {
				applyValidity(valid, sel, rows)
			}
			return true, newScratch, nil
		}
		if uint64(threshold) >= maxResidual {
			sel.Clear()
			return true, newScratch, nil
		}
		residuals := make([]uint64, rows)
		codec.Unpack(width, payload[forHeaderSize:], rows, residuals)
		narrowFORGT(residuals, uint64(threshold), valid, sel)
		return true, newScratch, nil
	}
	return false, newScratch, nil
}

func narrowFORLT(residuals []uint64, threshold uint64, valid vector.Validity, sel *vector.SelectionMask) {
	sel.Clear()
	for i, r := range residuals {
		if r < threshold {
			sel.Set(i)
		}
	}
	if valid != nil {
		applyValidity(valid, sel, len(residuals))
	}
}

func narrowFORGT(residuals []uint64, threshold uint64, valid vector.Validity, sel *vector.SelectionMask) {
	sel.Clear()
	for i, r := range residuals {
		if r > threshold {
			sel.Set(i)
		}
	}
	if valid != nil {
		applyValidity(valid, sel, len(residuals))
	}
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
func narrowDictEq(indices []byte, code uint8, valid vector.Validity, sel *vector.SelectionMask) {
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

func applyValidity(valid vector.Validity, sel *vector.SelectionMask, rows int) {
	for i := range rows {
		if !valid.IsValid(i) {
			sel.Unset(i)
		}
	}
}
