// WriteSegment serializes pages column-major as one Batch per page.
// Atomic via tmp file, fsync, rename, and a parent-directory fsync on POSIX.
package storage

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/storage/codec"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// syncDir fsyncs a directory so a rename inside it survives a crash. NTFS journals metadata so Windows is a no-op.
func syncDir(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

const segmentWriteBufferSize = 1 << 16

const (
	columnMarkerMixed    uint8 = 0
	columnMarkerAllValid uint8 = 1
	columnMarkerAllNull  uint8 = 2
)

const maxSegmentRows = 1<<32 - 1

type writerColumn struct {
	Schema    vector.Column
	Kind      vector.VecKind
	Pages     []Page
	Rows      uint32
	NullCount uint32
	Marker    uint8
	Stats     [StatsWireSize]byte
	PageStats [][StatsWireSize]byte
}

// WriteSegmentWithIdentity stamps a SegmentIdentity sidecar into the file so the
// reader can resolve columns by stable catalog identity rather than ordinal.
// Callers that have no identity to stamp should keep calling WriteSegment.
func WriteSegmentWithIdentity(path string, pages []vector.Batch, codecs map[string]schema.Encoding, id SegmentIdentity) error {
	return writeSegmentImpl(path, pages, codecs, id)
}

// codecs may be nil to use the cascade. A non-nil entry per column overrides
// EncInvalid and bypasses Pick.
func WriteSegment(path string, pages []vector.Batch, codecs map[string]schema.Encoding) error {
	return writeSegmentImpl(path, pages, codecs, SegmentIdentity{})
}

func writeSegmentImpl(path string, pages []vector.Batch, codecs map[string]schema.Encoding, id SegmentIdentity) error {
	tmpPath := path + ".tmp"
	_ = os.Remove(tmpPath)

	cols, err := prepareWriterColumns(pages)
	if err != nil {
		return err
	}

	totalRows := 0
	for _, p := range pages {
		totalRows += int(p.Len)
	}
	sinks := make([]*colSink, len(cols))
	for i, c := range cols {
		s := &colSink{}
		if kindEligibleForIntFilter(c.Kind) {
			// The 1024 cap keeps huge distinct columns from oversizing the opener, the map still grows past it.
			hint := max(min(totalRows, 1024), 8)
			s.intDedup = make(map[uint64]struct{}, hint)
			s.intKeys = make([]uint64, 0, hint)
		}
		if c.Kind.IsVarBytes() {
			s.varHist = make(DictHistogram, 8)
		}
		sinks[i] = s
	}

	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(f, segmentWriteBufferSize)
	if err := writeSegmentStream(bw, pages, cols, codecs, sinks, id); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}
	if err := finishSegmentFile(f, bw); err != nil {
		os.Remove(tmpPath)
		return err
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return syncDir(filepath.Dir(path))
}

// materializeSidecarBytes builds the sidecar container body to be appended inside the
// segment file. Returns nil when there's nothing to write. All work is done now so the
// segment write streams it in one bufio path with the column data and footer.
func materializeSidecarBytes(cols []writerColumn, sinks []*colSink, id SegmentIdentity, groupSums GroupSumsMap) []byte {
	hists, filters, sums, varBlooms := materializeSidecarsFromSinks(cols, sinks)
	identityBytes := encodeIdentitySidecar(id)
	if hists == nil && filters == nil && sums == nil && varBlooms == nil && identityBytes == nil && groupSums == nil {
		return nil
	}
	sections := make(map[uint8][]byte, 6)
	if groupSums != nil {
		if b, err := groupSumsSidecar.Encode(map[string]GroupSumsCol(groupSums)); err == nil && b != nil {
			sections[sidecarSectionGroupSums] = b
		}
	}
	if identityBytes != nil {
		sections[sidecarSectionIdentity] = identityBytes
	}
	if hists != nil {
		if b, err := dictHistSidecar.Encode(map[string]DictHistogram(hists)); err == nil && b != nil {
			sections[sidecarSectionDictHist] = b
		}
	}
	if filters != nil {
		if b, err := intFilterSidecar.Encode(map[string]*IntFilter(filters)); err == nil && b != nil {
			sections[sidecarSectionIntFilter] = b
		}
	}
	if sums != nil {
		if b, err := numSumSidecar.Encode(map[string]NumericSum(sums)); err == nil && b != nil {
			sections[sidecarSectionNumSum] = b
		}
	}
	if varBlooms != nil {
		if b, err := varBloomSidecar.Encode(map[string]*VarBloom(varBlooms)); err == nil && b != nil {
			sections[sidecarSectionVarBloom] = b
		}
	}
	if len(sections) == 0 {
		return nil
	}
	return EncodeSidecarContainer(sections)
}

func writeSegmentStream(bw *bufio.Writer, pages []vector.Batch, cols []writerColumn, codecs map[string]schema.Encoding, sinks []*colSink, id SegmentIdentity) error {
	if _, err := bw.Write([]byte(Magic)); err != nil {
		return err
	}
	if err := writePayloads(bw, pages, cols, codecs, sinks); err != nil {
		return err
	}
	for ci := range cols {
		if !marshalColumnStatsFromSink(cols[ci].Kind, sinks[ci], cols[ci].Stats[:]) {
			marshalColumnStats(cols[ci].Kind, pages, ci, cols[ci].Stats[:])
		}
	}
	footer, err := encodeFooter(cols)
	if err != nil {
		return err
	}
	if _, err := bw.Write(footer); err != nil {
		return err
	}
	// Sidecars must materialize AFTER writePayloads since the colSinks are filled
	// during the analyzer pass that walks each page during body encoding.
	sidecarBytes := materializeSidecarBytes(cols, sinks, id, buildGroupSums(pages, cols, sinks))
	if len(sidecarBytes) > 0 {
		if _, err := bw.Write(sidecarBytes); err != nil {
			return err
		}
	}
	suffix := make([]byte, FooterSuffixSize)
	WriteFooterSuffix(suffix, uint64(len(footer)), uint64(len(sidecarBytes)))
	if _, err := bw.Write(suffix); err != nil {
		return err
	}
	return nil
}

// Flush then Sync then Close. The first error wins, and Close runs even on
// earlier failure so handles never leak.
func finishSegmentFile(f *os.File, bw *bufio.Writer) error {
	var err error
	if e := bw.Flush(); e != nil {
		err = e
	}
	if err == nil {
		if e := f.Sync(); e != nil {
			err = e
		}
	}
	if e := f.Close(); err == nil && e != nil {
		err = e
	}
	return err
}

func prepareWriterColumns(pages []vector.Batch) ([]writerColumn, error) {
	if len(pages) == 0 {
		return nil, fmt.Errorf("WriteSegment: at least one page required")
	}
	first := pages[0]
	if len(first.Columns) == 0 {
		return nil, fmt.Errorf("WriteSegment: first batch has no columns")
	}
	cols := make([]writerColumn, len(first.Columns))
	for i, col := range first.Columns {
		k, err := vector.VecKindOf(col.Type)
		if err != nil {
			return nil, fmt.Errorf("WriteSegment: col %q: %w", col.Name, err)
		}
		cols[i] = writerColumn{
			Schema: vector.Column{Name: col.Name, Type: col.Type, EnumLabels: slices.Clone(col.EnumLabels)},
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
		var nulls uint32
		for _, page := range pages {
			v := page.Columns[ci].V
			if v.Valid != nil {
				nulls += uint32(v.Valid.NullCount(int(v.Len)))
			}
		}
		cols[ci].NullCount = nulls
		switch {
		case cols[ci].Rows > 0 && cols[ci].NullCount == cols[ci].Rows:
			cols[ci].Marker = columnMarkerAllNull
		case cols[ci].NullCount == 0:
			cols[ci].Marker = columnMarkerAllValid
		default:
			cols[ci].Marker = columnMarkerMixed
		}
		cols[ci].PageStats = make([][StatsWireSize]byte, len(pages))
	}
	return cols, nil
}

func marshalPageStats(k vector.VecKind, v vector.Vec, dst []byte) {
	switch k {
	case vector.VecInt16:
		var s NumericStats[int32]
		for i, x := range v.I16() {
			if isValidRow(v, i) {
				s.Update(int32(x))
			}
		}
		s.MarshalWire(dst)
	case vector.VecInt32, vector.VecDate:
		var s NumericStats[int32]
		for i, x := range v.I32() {
			if isValidRow(v, i) {
				s.Update(x)
			}
		}
		s.MarshalWire(dst)
	case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
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

func isValidRow(v vector.Vec, row int) bool {
	if v.Valid == nil {
		return true
	}
	return v.Valid.IsValid(row)
}

func marshalColumnStats(k vector.VecKind, pages []vector.Batch, colIdx int, dst []byte) {
	switch k {
	case vector.VecInt16, vector.VecInt32, vector.VecDate, vector.VecEnum32:
		var stats NumericStats[int32]
		for _, page := range pages {
			v := page.Columns[colIdx].V
			switch k {
			case vector.VecInt16:
				for i, x := range v.I16() {
					if isValidRow(v, i) {
						stats.Update(int32(x))
					}
				}
			case vector.VecInt32, vector.VecDate:
				for i, x := range v.I32() {
					if isValidRow(v, i) {
						stats.Update(x)
					}
				}
			case vector.VecEnum32:
				for i, x := range v.U32() {
					if isValidRow(v, i) {
						stats.Update(int32(x))
					}
				}
			}
		}
		stats.MarshalWire(dst)
	case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
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
	case vector.VecFloat32, vector.VecFloat64:
		var stats FloatStats
		for _, page := range pages {
			v := page.Columns[colIdx].V
			if k == vector.VecFloat32 {
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
	case vector.VecBool:
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
	case vector.VecText, vector.VecBytes, vector.VecJSON:
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

func writePayloads(w io.Writer, pages []vector.Batch, cols []writerColumn, codecs map[string]schema.Encoding, sinks []*colSink) error {
	bodyOff := uint64(MagicLen)
	var facts codec.PageFacts
	ctx := &codec.EncodeContext{Scratch: codec.NewScratchPool(), Facts: &facts}
	for ci := range cols {
		ctx.Scratch.ResetColumn()
		override := schema.EncInvalid
		if codecs != nil {
			override = codecs[schema.NormalizeName(cols[ci].Schema.Name)]
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
				// An all-null page carries no codec payload.
				page.PayloadLength = 0
				page.Encoding = schema.EncPlain.Wire()
				page.Flags = PageFlagAllNull
			case pageNulls == 0:
				// All-valid pages run the cascade chooser by default and the user override when set.
				analyzePage(col.V, &facts, sinks[ci], pageIdx)
				marshalPageStatsFromFacts(col.V.Kind, &facts, col.V, cols[ci].PageStats[pageIdx][:])
				var (
					enc     schema.Encoding
					payload []byte
					err     error
				)
				if override != schema.EncInvalid {
					c, lookupErr := codec.Lookup(override)
					if lookupErr != nil {
						return fmt.Errorf("WriteSegment: col %q codec override %v: %w", col.Name, override, lookupErr)
					}
					ctx.Scratch.SaveTrial(ctx.Scratch.Trial())
					payload, err = c.Encode(col.V, ctx)
					enc = c.Encoding()
				} else {
					enc, payload, err = codec.Encode(col.V, ctx)
				}
				if err != nil {
					return fmt.Errorf("WriteSegment: encode col %q page %d: %w", col.Name, pageIdx, err)
				}
				if _, err := w.Write(payload); err != nil {
					return err
				}
				page.PayloadLength = uint64(len(payload))
				page.Encoding = enc.Wire()
				page.Flags = PageFlagAllValid
				bodyOff += uint64(len(payload))
			default:
				// Mixed page runs sink-only accumulation so sidecars stay exact.
				analyzePage(col.V, nil, sinks[ci], pageIdx)
				marshalPageStats(col.V.Kind, col.V, cols[ci].PageStats[pageIdx][:])
				// Mixed page layout is <validity bytes><plain payload>.
				// Plain ignores null slot values. Reader strips validity
				// before handing the inner payload to the codec.
				validityBytes := make([]byte, vector.ValidityWords(int(col.V.Len))*8)
				col.V.Valid.MarshalLE(validityBytes)
				plain, err := codec.Lookup(schema.EncPlain)
				if err != nil {
					return err
				}
				inner, err := plain.Encode(col.V, ctx)
				if err != nil {
					return fmt.Errorf("WriteSegment: encode col %q page %d: %w", col.Name, pageIdx, err)
				}
				if _, err := w.Write(validityBytes); err != nil {
					return err
				}
				if _, err := w.Write(inner); err != nil {
					return err
				}
				page.PayloadLength = uint64(len(validityBytes) + len(inner))
				page.Encoding = schema.EncPlain.Wire()
				page.Flags = 0
				bodyOff += uint64(len(validityBytes) + len(inner))
				ctx.Scratch.SaveTrial(inner)
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
