// WriteSegment serializes pages into a segment file column-major, one Batch per page.
// Atomic: writes to path.tmp, fsyncs the file, renames to path, fsyncs the parent directory on POSIX.
package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/kylegrahammatzen/dripsql/internal/storage/codec"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

const (
	columnMarkerMixed    uint8 = 0
	columnMarkerAllValid uint8 = 1
	columnMarkerAllNull  uint8 = 2
)

const maxSegmentRows = 1<<32 - 1

type writerColumn struct {
	Schema    types.Column
	Kind      types.VecKind
	Pages     []Page
	Rows      uint32
	NullCount uint32
	Marker    uint8
	Stats     [StatsWireSize]byte
	PageStats [][StatsWireSize]byte
}

func WriteSegment(path string, pages []types.Batch) error {
	return WriteSegmentWithCodecs(path, pages, nil)
}

// An override of EncodingAuto (or absent) keeps the cascade. Any other Encoding
// bypasses Pick. Codec/kind mismatches surface as an Encode error at write time.
func WriteSegmentWithCodecs(path string, pages []types.Batch, codecs map[string]types.Encoding) error {
	tmpPath := path + ".tmp"
	_ = os.Remove(tmpPath)

	cols, err := prepareWriterColumns(pages)
	if err != nil {
		return err
	}
	if err := writeSegmentBody(tmpPath, pages, cols, codecs); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return err
	}
	// Sidecars are best-effort: a failed write doesn't fail the segment, the relevant
	// metadata path just falls back to the operator scan for this segment.
	if hist := buildDictHistograms(pages); hist != nil {
		_ = writeDictHistogramSidecar(path, hist)
	}
	if blooms := buildIntBlooms(pages); blooms != nil {
		_ = writeBloomSidecar(path, blooms)
	}
	return nil
}

func writeSegmentBody(path string, pages []types.Batch, cols []writerColumn, codecs map[string]types.Encoding) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			f.Close()
		}
	}()

	if _, err := f.Write([]byte(Magic)); err != nil {
		return err
	}
	if err := writePayloads(f, pages, cols, codecs); err != nil {
		return err
	}
	footer, err := encodeFooter(cols)
	if err != nil {
		return err
	}
	if _, err := f.Write(footer); err != nil {
		return err
	}
	suffix := make([]byte, FooterSuffixSize)
	WriteFooterSuffix(suffix, uint64(len(footer)))
	if _, err := f.Write(suffix); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	closed = true
	return f.Close()
}

func prepareWriterColumns(pages []types.Batch) ([]writerColumn, error) {
	if len(pages) == 0 {
		return nil, fmt.Errorf("WriteSegment: at least one page required")
	}
	first := pages[0]
	if len(first.Columns) == 0 {
		return nil, fmt.Errorf("WriteSegment: first batch has no columns")
	}
	cols := make([]writerColumn, len(first.Columns))
	for i, col := range first.Columns {
		k, err := types.VecKindOf(col.Type)
		if err != nil {
			return nil, fmt.Errorf("WriteSegment: col %q: %w", col.Name, err)
		}
		cols[i] = writerColumn{
			Schema: types.Column{Name: col.Name, Type: col.Type, EnumLabels: slices.Clone(col.EnumLabels)},
			Kind:   k,
		}
	}

	var rows uint64
	for pageIdx, batch := range pages {
		if batch.Len < 0 {
			return nil, fmt.Errorf("WriteSegment: page %d has negative length %d", pageIdx, batch.Len)
		}
		rows += uint64(batch.Len)
		if rows > maxSegmentRows {
			return nil, fmt.Errorf("WriteSegment: total rows %d exceed uint32 footer limit", rows)
		}
		if len(batch.Columns) != len(cols) {
			return nil, fmt.Errorf("WriteSegment: page %d has %d columns, want %d", pageIdx, len(batch.Columns), len(cols))
		}
		for ci, col := range batch.Columns {
			expect := cols[ci].Schema
			if col.Name != expect.Name {
				return nil, fmt.Errorf("WriteSegment: page %d col %d name %q != %q", pageIdx, ci, col.Name, expect.Name)
			}
			if col.Type != expect.Type {
				return nil, fmt.Errorf("WriteSegment: page %d col %q type %v != first batch %v", pageIdx, col.Name, col.Type, expect.Type)
			}
			if !slices.Equal(col.EnumLabels, expect.EnumLabels) {
				return nil, fmt.Errorf("WriteSegment: page %d col %q enum labels drift from first batch", pageIdx, col.Name)
			}
			if err := col.V.Validate(); err != nil {
				return nil, fmt.Errorf("WriteSegment: page %d col %q: %w", pageIdx, col.Name, err)
			}
			if col.V.Kind != cols[ci].Kind {
				return nil, fmt.Errorf("WriteSegment: page %d col %q Vec kind %v != type %v expects %v", pageIdx, col.Name, col.V.Kind, expect.Type, cols[ci].Kind)
			}
			if int(col.V.Len) != batch.Len {
				return nil, fmt.Errorf("WriteSegment: page %d col %q Vec.Len %d != Batch.Len %d", pageIdx, col.Name, col.V.Len, batch.Len)
			}
		}
	}
	for ci := range cols {
		cols[ci].Rows = uint32(rows)
		cols[ci].NullCount = totalNullCount(pages, ci)
		switch {
		case cols[ci].Rows > 0 && cols[ci].NullCount == cols[ci].Rows:
			cols[ci].Marker = columnMarkerAllNull
		case cols[ci].NullCount == 0:
			cols[ci].Marker = columnMarkerAllValid
		default:
			cols[ci].Marker = columnMarkerMixed
		}
		marshalColumnStats(cols[ci].Kind, pages, ci, cols[ci].Stats[:])
		cols[ci].PageStats = make([][StatsWireSize]byte, len(pages))
		for pi := range pages {
			marshalPageStats(cols[ci].Kind, pages[pi].Columns[ci].V, cols[ci].PageStats[pi][:])
		}
	}
	return cols, nil
}

func marshalPageStats(k types.VecKind, v types.Vec, dst []byte) {
	switch k {
	case types.VecInt16:
		var s NumericStats[int32]
		for i, x := range v.I16() {
			if isValidRow(v, i) {
				s.Update(int32(x))
			}
		}
		s.MarshalWire(dst)
	case types.VecInt32, types.VecDate:
		var s NumericStats[int32]
		for i, x := range v.I32() {
			if isValidRow(v, i) {
				s.Update(x)
			}
		}
		s.MarshalWire(dst)
	case types.VecInt64, types.VecTimestamp, types.VecTime, types.VecDecimal64:
		var s NumericStats[int64]
		for i, x := range v.I64() {
			if isValidRow(v, i) {
				s.Update(x)
			}
		}
		s.MarshalWire(dst)
	default:
		clear(dst[:StatsWireSize])
	}
}

func totalNullCount(pages []types.Batch, ci int) uint32 {
	var n uint32
	for _, page := range pages {
		v := page.Columns[ci].V
		if v.Valid == nil {
			continue
		}
		n += uint32(v.Valid.NullCount(int(v.Len)))
	}
	return n
}

func isValidRow(v types.Vec, row int) bool {
	if v.Valid == nil {
		return true
	}
	return v.Valid.IsValid(row)
}

func marshalColumnStats(k types.VecKind, pages []types.Batch, colIdx int, dst []byte) {
	switch k {
	case types.VecInt16, types.VecInt32, types.VecDate, types.VecEnum32:
		var stats NumericStats[int32]
		for _, page := range pages {
			v := page.Columns[colIdx].V
			switch k {
			case types.VecInt16:
				for i, x := range v.I16() {
					if isValidRow(v, i) {
						stats.Update(int32(x))
					}
				}
			case types.VecInt32, types.VecDate:
				for i, x := range v.I32() {
					if isValidRow(v, i) {
						stats.Update(x)
					}
				}
			case types.VecEnum32:
				for i, x := range v.U32() {
					if isValidRow(v, i) {
						stats.Update(int32(x))
					}
				}
			}
		}
		stats.MarshalWire(dst)
	case types.VecInt64, types.VecTimestamp, types.VecTime, types.VecDecimal64:
		var stats NumericStats[int64]
		for _, page := range pages {
			v := page.Columns[colIdx].V
			for i, x := range v.I64() {
				if isValidRow(v, i) {
					stats.Update(x)
				}
			}
		}
		stats.MarshalWire(dst)
	case types.VecFloat32, types.VecFloat64:
		var stats FloatStats
		for _, page := range pages {
			v := page.Columns[colIdx].V
			if k == types.VecFloat32 {
				for i, x := range v.F32() {
					if isValidRow(v, i) {
						stats.Update(float64(x))
					}
				}
				continue
			}
			for i, x := range v.F64() {
				if isValidRow(v, i) {
					stats.Update(x)
				}
			}
		}
		stats.MarshalWire(dst)
	case types.VecBool:
		var stats BoolStats
		for _, page := range pages {
			v := page.Columns[colIdx].V
			bits := v.BoolBits()
			for i := range int(v.Len) {
				if !isValidRow(v, i) {
					continue
				}
				if bits[i>>3]&(1<<uint(i&7)) != 0 {
					stats.TrueCount++
				} else {
					stats.FalseCount++
				}
			}
		}
		stats.MarshalWire(dst)
	case types.VecText, types.VecBytes, types.VecJSON:
		var stats VarBytesStats
		for _, page := range pages {
			v := page.Columns[colIdx].V
			vb := v.Var()
			for i := range int(v.Len) {
				if isValidRow(v, i) {
					stats.Update(vb.Len(i))
				}
			}
		}
		stats.MarshalWire(dst)
	default:
		clear(dst[:StatsWireSize])
	}
}

func writePayloads(f *os.File, pages []types.Batch, cols []writerColumn, codecs map[string]types.Encoding) error {
	bodyOff := uint64(MagicLen)
	var scratch []byte
	for ci := range cols {
		override := types.EncodingAuto
		if codecs != nil {
			override = codecs[types.NormalizeName(cols[ci].Schema.Name)]
		}
		var rowStart uint32
		for pageIdx, batch := range pages {
			col := batch.Columns[ci]
			pageRows := uint32(batch.Len)
			pageNulls := uint32(0)
			if col.V.Valid != nil {
				pageNulls = uint32(col.V.Valid.NullCount(int(col.V.Len)))
			}

			page := Page{
				PayloadOffset: bodyOff,
				RowStart:      rowStart,
				Rows:          pageRows,
				NullCount:     pageNulls,
				Kind:          uint8(col.V.Kind),
			}

			switch {
			case pageRows > 0 && pageNulls == pageRows:
				// All-null page. No codec payload; encoding marked Flat.
				page.PayloadLength = 0
				page.Encoding = types.EncodingFlat.Wire()
				page.Flags = PageFlagAllNull
			case pageNulls == 0:
				// All-valid: cascade chooser by default, user override when set.
				var codecImpl codec.Codec
				if override != types.EncodingAuto {
					c, err := codec.Lookup(override)
					if err != nil {
						return fmt.Errorf("WriteSegment: col %q codec override %v: %w", col.Name, override, err)
					}
					codecImpl = c
				} else {
					c, _, ok := codec.Pick(col.V)
					if !ok {
						return fmt.Errorf("WriteSegment: no codec accepts col %q page %d kind %v", col.Name, pageIdx, col.V.Kind)
					}
					codecImpl = c
				}
				payload, err := codecImpl.Encode(col.V, scratch[:0])
				if err != nil {
					return fmt.Errorf("WriteSegment: encode col %q page %d: %w", col.Name, pageIdx, err)
				}
				if _, err := f.Write(payload); err != nil {
					return err
				}
				page.PayloadLength = uint64(len(payload))
				page.Encoding = codecImpl.Encoding().Wire()
				page.Flags = PageFlagAllValid
				bodyOff += uint64(len(payload))
				scratch = payload
			default:
				// Mixed page layout is <validity bytes><plain payload>.
				// Plain ignores null slot values. Reader strips validity
				// before handing the inner payload to the codec.
				validityBytes := make([]byte, types.ValidityWords(int(col.V.Len))*8)
				col.V.Valid.MarshalLE(validityBytes)
				plain, err := codec.Lookup(types.EncodingFlat)
				if err != nil {
					return err
				}
				inner, err := plain.Encode(col.V, scratch[:0])
				if err != nil {
					return fmt.Errorf("WriteSegment: encode col %q page %d: %w", col.Name, pageIdx, err)
				}
				if _, err := f.Write(validityBytes); err != nil {
					return err
				}
				if _, err := f.Write(inner); err != nil {
					return err
				}
				page.PayloadLength = uint64(len(validityBytes) + len(inner))
				page.Encoding = types.EncodingFlat.Wire()
				page.Flags = 0
				bodyOff += uint64(len(validityBytes) + len(inner))
				scratch = inner
			}

			cols[ci].Pages = append(cols[ci].Pages, page)
			rowStart += pageRows
		}
	}
	return nil
}

func encodeFooter(cols []writerColumn) ([]byte, error) {
	w := newWireBuffer(256)

	w.U32(uint32(len(cols)))
	for _, c := range cols {
		w.LenPrefixedString(c.Schema.Name)
		w.U8(byte(c.Kind))
		w.U8(0)
		w.U32(uint32(len(c.Schema.EnumLabels)))
		for _, label := range c.Schema.EnumLabels {
			w.LenPrefixedString(label)
		}
		w.U32(c.Rows)
		w.U32(c.NullCount)
		w.U8(c.Marker)
		w.Raw(c.Stats[:])
		w.U64(0) // heavy_blob_offset (reserved)
		w.U64(0) // heavy_blob_length (reserved)
		w.U32(uint32(len(c.Pages)))
	}
	pageEntry := make([]byte, PageEntrySize)
	for _, c := range cols {
		for _, p := range c.Pages {
			WritePage(pageEntry, p)
			w.Raw(pageEntry)
		}
	}
	for _, c := range cols {
		for _, ps := range c.PageStats {
			w.Raw(ps[:])
		}
	}
	return w.Bytes(), nil
}
