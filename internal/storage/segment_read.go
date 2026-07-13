// OpenSegment cold-opens a segment via targeted ReadAt: only the footer suffix and body are read up front.
// Page payloads stream on demand through ReadPage. The file handle stays open until Close.
package storage

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"sync"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/storage/codec"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// SegmentIdentity is the optional sidecar that records the table_id, schema_generation, and per-chunk column_id list.
// Absence means a legacy dsv4 segment that the engine still resolves positionally.
type SegmentIdentity struct {
	TableID          uint64
	SchemaGeneration uint64
	ColumnIDs        []uint64
}

const identitySidecarMagic = "DSID"

const sidecarSectionIdentity uint8 = 5

func encodeIdentitySidecar(id SegmentIdentity) []byte {
	if id.TableID == 0 && id.SchemaGeneration == 0 && len(id.ColumnIDs) == 0 {
		return nil
	}
	size := len(identitySidecarMagic) + 8 + 8 + 4 + 8*len(id.ColumnIDs)
	buf := make([]byte, 0, size)
	buf = append(buf, identitySidecarMagic...)
	buf = binary.LittleEndian.AppendUint64(buf, id.TableID)
	buf = binary.LittleEndian.AppendUint64(buf, id.SchemaGeneration)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(id.ColumnIDs)))
	for _, c := range id.ColumnIDs {
		buf = binary.LittleEndian.AppendUint64(buf, c)
	}
	return buf
}

func decodeIdentitySidecar(data []byte) (SegmentIdentity, error) {
	hdr := len(identitySidecarMagic) + 8 + 8 + 4
	if len(data) < hdr {
		return SegmentIdentity{}, fmt.Errorf("identity sidecar: short header (%d bytes)", len(data))
	}
	if string(data[:len(identitySidecarMagic)]) != identitySidecarMagic {
		return SegmentIdentity{}, fmt.Errorf("identity sidecar: bad magic")
	}
	pos := len(identitySidecarMagic)
	tableID := binary.LittleEndian.Uint64(data[pos : pos+8])
	pos += 8
	gen := binary.LittleEndian.Uint64(data[pos : pos+8])
	pos += 8
	n := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
	pos += 4
	if pos+n*8 != len(data) {
		return SegmentIdentity{}, fmt.Errorf("identity sidecar: body length mismatch (have %d, want %d)", len(data)-pos, n*8)
	}
	ids := make([]uint64, n)
	for i := range ids {
		ids[i] = binary.LittleEndian.Uint64(data[pos : pos+8])
		pos += 8
	}
	return SegmentIdentity{TableID: tableID, SchemaGeneration: gen, ColumnIDs: ids}, nil
}

type SegmentColumn struct {
	Name       string
	Kind       vector.VecKind
	EnumLabels []string
	Rows       uint32
	NullCount  uint32
	Marker     uint8
	Stats      [StatsWireSize]byte
	Pages      []Page
	PageStats  [][StatsWireSize]byte

	// ColumnID is the stable catalog id for this column. Zero on legacy dsv4
	// segments written before the identity sidecar landed; callers must fall back
	// to positional mapping when zero.
	ColumnID uint64

	// Lazily inflated into PageStats on first prune call. Skips the per-column
	// alloc on cold open when no predicate ever runs.
	pageStatsRaw []byte
}

type Segment struct {
	f          *os.File
	path       string
	sidecarOff int64
	sidecarLen int64
	Cols       []SegmentColumn
	DV         vector.Validity
	// CommitTs is the manifest record's commit timestamp; set by the engine when opening
	// the segment so the scan visibility filter can skip segments newer than a reader's
	// ReadTs. Zero on standalone OpenSegment paths (tests, tooling), which treat the
	// segment as committed at time 0 -- always visible.
	CommitTs uint64

	// TableID and SchemaGeneration are populated from the identity sidecar when present.
	// Zero on legacy dsv4 segments and signals the engine to use positional mapping.
	TableID          uint64
	SchemaGeneration uint64

	containerOnce sync.Once
	container     map[uint8][]byte
	containerErr  error

	pageStatsOnce sync.Once

	dictHistsOnce sync.Once
	dictHists     DictHistograms
	dictHistsErr  error

	intFiltersOnce sync.Once
	intFilters     IntFilters
	intFiltersErr  error

	numSumsOnce sync.Once
	numSums     NumericSums
	numSumsErr  error

	varBloomsOnce sync.Once
	varBlooms     VarBlooms
	varBloomsErr  error

	validateOnce sync.Once
	validateErr  error
	bodyEndCache int64
}

func (s *Segment) sidecarSection(tag uint8) ([]byte, error) {
	s.containerOnce.Do(func() {
		if s.sidecarLen == 0 {
			return
		}
		buf, err := readExactAt(s.f, s.sidecarOff, int(s.sidecarLen), "sidecar")
		if err != nil {
			s.containerErr = err
			return
		}
		s.container, s.containerErr = DecodeSidecarContainer(buf)
	})
	if s.containerErr != nil {
		return nil, s.containerErr
	}
	return s.container[tag], nil
}

func (s *Segment) DictHistograms() (DictHistograms, error) {
	s.dictHistsOnce.Do(func() {
		body, err := s.sidecarSection(sidecarSectionDictHist)
		if err != nil {
			s.dictHistsErr = err
			return
		}
		m, err := dictHistSidecar.Decode(body)
		if err != nil {
			s.dictHistsErr = err
			return
		}
		s.dictHists = DictHistograms(m)
	})
	return s.dictHists, s.dictHistsErr
}

func (s *Segment) IntFilterSet() (IntFilters, error) {
	s.intFiltersOnce.Do(func() {
		body, err := s.sidecarSection(sidecarSectionIntFilter)
		if err != nil {
			s.intFiltersErr = err
			return
		}
		m, err := intFilterSidecar.Decode(body)
		if err != nil {
			s.intFiltersErr = err
			return
		}
		s.intFilters = IntFilters(m)
	})
	return s.intFilters, s.intFiltersErr
}

func (s *Segment) NumericSums() (NumericSums, error) {
	s.numSumsOnce.Do(func() {
		body, err := s.sidecarSection(sidecarSectionNumSum)
		if err != nil {
			s.numSumsErr = err
			return
		}
		m, err := numSumSidecar.Decode(body)
		if err != nil {
			s.numSumsErr = err
			return
		}
		s.numSums = NumericSums(m)
	})
	return s.numSums, s.numSumsErr
}

func (s *Segment) VarBlooms() (VarBlooms, error) {
	s.varBloomsOnce.Do(func() {
		body, err := s.sidecarSection(sidecarSectionVarBloom)
		if err != nil {
			s.varBloomsErr = err
			return
		}
		m, err := varBloomSidecar.Decode(body)
		if err != nil {
			s.varBloomsErr = err
			return
		}
		s.varBlooms = VarBlooms(m)
	})
	return s.varBlooms, s.varBloomsErr
}

func (s *Segment) LoadPageStats() {
	s.pageStatsOnce.Do(func() {
		for i := range s.Cols {
			c := &s.Cols[i]
			if c.PageStats != nil || len(c.pageStatsRaw) == 0 {
				continue
			}
			n := len(c.pageStatsRaw) / StatsWireSize
			c.PageStats = make([][StatsWireSize]byte, n)
			for j := range n {
				copy(c.PageStats[j][:], c.pageStatsRaw[j*StatsWireSize:(j+1)*StatsWireSize])
			}
		}
	})
}

func (s *Segment) Path() string { return s.path }
func (s *Segment) Rows() uint32 {
	if len(s.Cols) == 0 {
		return 0
	}
	return s.Cols[0].Rows
}

func OpenSegment(path string) (*Segment, error) {
	return OpenSegmentWithDV(path, "")
}

// OpenSegmentWithDV opens a segment with an explicit DV file path. An empty dvPath falls
// back to the default convention (<path>.dv); a manifest-driven path lets transaction
// commits switch the visible DV atomically with the manifest record.
func OpenSegmentWithDV(path, dvPath string) (*Segment, error) {
	f, err := OpenRandomAccess(path)
	if err != nil {
		return nil, err
	}
	layout, err := readFooter(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	rows := uint32(0)
	if len(layout.cols) > 0 {
		rows = layout.cols[0].Rows
	}
	if dvPath == "" {
		dvPath = DVPath(path)
	}
	dv, err := loadDVAtPath(dvPath, int(rows))
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("OpenSegment: load DV: %w", err)
	}
	// Sidecar container lives inline between footer and suffix; lazily read on first access.
	seg := &Segment{
		f:            f,
		path:         path,
		bodyEndCache: layout.bodyEnd,
		sidecarOff:   layout.sidecarStart,
		sidecarLen:   layout.sidecarLen,
		Cols:         layout.cols,
		DV:           dv,
	}
	if err := seg.hydrateIdentity(); err != nil {
		f.Close()
		return nil, err
	}
	return seg, nil
}

// hydrateIdentity pulls the identity sidecar section if present and stamps TableID,
// SchemaGeneration on the segment plus ColumnID on each column. Missing section is
// fine and leaves zero values that signal positional fallback to the engine.
func (s *Segment) hydrateIdentity() error {
	body, err := s.sidecarSection(sidecarSectionIdentity)
	if err != nil {
		return err
	}
	if body == nil {
		return nil
	}
	id, err := decodeIdentitySidecar(body)
	if err != nil {
		return fmt.Errorf("OpenSegment: %w", err)
	}
	s.TableID = id.TableID
	s.SchemaGeneration = id.SchemaGeneration
	if len(id.ColumnIDs) != len(s.Cols) {
		return fmt.Errorf("OpenSegment: identity column count %d != footer column count %d", len(id.ColumnIDs), len(s.Cols))
	}
	for i, cid := range id.ColumnIDs {
		s.Cols[i].ColumnID = cid
	}
	return nil
}

// ValidateColumns runs structural checks on all column directories. Called once
// lazily before the first ReadPage so OpenSegment stays a thin footer read.
func (s *Segment) ValidateColumns() error {
	s.validateOnce.Do(func() {
		for i := range s.Cols {
			if err := validateColumnDirectory(&s.Cols[i], s.bodyEndCache); err != nil {
				s.validateErr = fmt.Errorf("validateColumns: col %q: %w", s.Cols[i].Name, err)
				return
			}
		}
	})
	return s.validateErr
}

const knownPageFlags = PageFlagAllValid | PageFlagAllNull | PageFlagInMembership | PageFlagEncodedEvalOK

func validateColumnDirectory(c *SegmentColumn, bodyEnd int64) error {
	switch c.Marker {
	case columnMarkerMixed, columnMarkerAllValid, columnMarkerAllNull:
	default:
		return fmt.Errorf("column marker %d not in {Mixed, AllValid, AllNull}", c.Marker)
	}
	if c.NullCount > c.Rows {
		return fmt.Errorf("column NullCount %d > rows %d", c.NullCount, c.Rows)
	}
	bodyEndU := uint64(bodyEnd)
	var rowSum, nullSum uint64
	var nextRowStart uint32
	for i := range c.Pages {
		p := c.Pages[i]
		allValid := p.Flags&PageFlagAllValid != 0
		allNull := p.Flags&PageFlagAllNull != 0
		if allValid && allNull {
			return fmt.Errorf("page %d has both AllValid and AllNull flags", i)
		}
		if p.Flags & ^knownPageFlags != 0 {
			return fmt.Errorf("page %d flags %08b contain reserved bits", i, p.Flags)
		}
		if p.Reserved != 0 {
			return fmt.Errorf("page %d reserved byte %d != 0", i, p.Reserved)
		}
		if allValid && p.NullCount != 0 {
			return fmt.Errorf("page %d AllValid but NullCount %d != 0", i, p.NullCount)
		}
		if allNull && p.NullCount != p.Rows {
			return fmt.Errorf("page %d AllNull but NullCount %d != Rows %d", i, p.NullCount, p.Rows)
		}
		if !allValid && !allNull && (p.NullCount == 0 || p.NullCount == p.Rows) {
			return fmt.Errorf("page %d mixed flag set but NullCount %d collapses to AllValid/AllNull", i, p.NullCount)
		}
		if p.NullCount > p.Rows {
			return fmt.Errorf("page %d NullCount %d > Rows %d", i, p.NullCount, p.Rows)
		}
		if vector.VecKind(p.Kind) != c.Kind {
			return fmt.Errorf("page %d kind %v != column kind %v", i, vector.VecKind(p.Kind), c.Kind)
		}
		if p.RowStart != nextRowStart {
			return fmt.Errorf("page %d RowStart %d != expected %d (non-contiguous)", i, p.RowStart, nextRowStart)
		}
		if p.PayloadOffset < uint64(MagicLen) && p.PayloadLength > 0 {
			return fmt.Errorf("page %d PayloadOffset %d inside head magic", i, p.PayloadOffset)
		}
		if p.PayloadOffset > bodyEndU || p.PayloadLength > bodyEndU-p.PayloadOffset {
			return fmt.Errorf("page %d payload [%d, %d) escapes body end %d", i, p.PayloadOffset, p.PayloadOffset+p.PayloadLength, bodyEnd)
		}
		rowSum += uint64(p.Rows)
		nullSum += uint64(p.NullCount)
		nextRow := uint64(p.RowStart) + uint64(p.Rows)
		if nextRow > maxSegmentRows {
			return fmt.Errorf("page %d row range overflows uint32", i)
		}
		nextRowStart = uint32(nextRow)
	}
	if rowSum != uint64(c.Rows) {
		return fmt.Errorf("sum(page.Rows) %d != column.Rows %d", rowSum, c.Rows)
	}
	if nullSum != uint64(c.NullCount) {
		return fmt.Errorf("sum(page.NullCount) %d != column.NullCount %d", nullSum, c.NullCount)
	}
	return nil
}

func (s *Segment) Close() error { return s.f.Close() }

// Caller-supplied scratch is grown if too small and returned for reuse on the next call.
func (s *Segment) ReadPageInto(colIdx, pageIdx int, scratch []byte) (vector.Vec, []byte, error) {
	if err := s.ValidateColumns(); err != nil {
		return vector.Vec{}, scratch, err
	}
	var v vector.Vec
	scratch, err := s.readPageIntoValidated(colIdx, pageIdx, scratch, &v)
	return v, scratch, err
}

// ReadPageIntoVec decodes a page into dst and returns reusable scratch.
func (s *Segment) ReadPageIntoVec(colIdx, pageIdx int, scratch []byte, dst *vector.Vec) ([]byte, error) {
	if err := s.ValidateColumns(); err != nil {
		return scratch, err
	}
	return s.readPageIntoValidated(colIdx, pageIdx, scratch, dst)
}

func (s *Segment) readPageIntoValidated(colIdx, pageIdx int, scratch []byte, dst *vector.Vec) ([]byte, error) {
	if colIdx < 0 || colIdx >= len(s.Cols) {
		return scratch, fmt.Errorf("ReadPage: col %d out of range [0, %d)", colIdx, len(s.Cols))
	}
	col := &s.Cols[colIdx]
	if pageIdx < 0 || pageIdx >= len(col.Pages) {
		return scratch, fmt.Errorf("ReadPage: page %d out of range [0, %d)", pageIdx, len(col.Pages))
	}
	page := col.Pages[pageIdx]

	if page.Flags&PageFlagAllNull != 0 {
		return scratch, decodePageBytes(page, nil, dst)
	}

	if cap(scratch) < int(page.PayloadLength) {
		scratch = make([]byte, page.PayloadLength)
	} else {
		scratch = scratch[:page.PayloadLength]
	}
	ioStart := nowNanos()
	if _, err := s.f.ReadAt(scratch, int64(page.PayloadOffset)); err != nil {
		return scratch, fmt.Errorf("ReadPage: read payload: %w", err)
	}
	addIORead(nowNanos() - ioStart)
	return scratch, decodePageBytes(page, scratch, dst)
}

// decodePageBytes decodes one page whose raw payload bytes are already in memory.
func decodePageBytes(page Page, raw []byte, dst *vector.Vec) error {
	rows := int(page.Rows)
	kind := vector.VecKind(page.Kind)
	if page.Flags&PageFlagAllNull != 0 {
		*dst = allocVecForKind(kind, rows)
		dst.Valid = allInvalidValidity(rows)
		return nil
	}
	innerPayload, validity, _, err := parsePageBytes(page, raw)
	if err != nil {
		return err
	}
	enc := schema.Encoding(page.Encoding)
	if !enc.Valid() {
		return fmt.Errorf("ReadPage: unknown encoding wire byte %d", page.Encoding)
	}
	c, err := codec.Lookup(enc)
	if err != nil {
		return err
	}
	decStart := nowNanos()
	if err := c.Decode(innerPayload, kind, rows, int(page.NullCount), dst); err != nil {
		return fmt.Errorf("ReadPage: decode: %w", err)
	}
	addDecode(nowNanos() - decStart)
	dst.Valid = validity
	return nil
}

// parsePageBytes splits raw payload bytes into the validity prefix and inner codec payload.
func parsePageBytes(page Page, raw []byte) (inner []byte, validity vector.Validity, allNull bool, err error) {
	if page.Flags&PageFlagAllNull != 0 {
		return nil, nil, true, nil
	}
	rows := int(page.Rows)
	inner = raw
	if page.NullCount > 0 {
		words := vector.ValidityWords(rows)
		need := words * 8
		if len(raw) < need {
			return nil, nil, false, fmt.Errorf("ReadPage: validity prefix truncated: have %d need %d", len(raw), need)
		}
		v, _, verr := vector.UnmarshalValidity(raw[:need], rows, int(page.NullCount), nil)
		if verr != nil {
			return nil, nil, false, fmt.Errorf("ReadPage: validity: %w", verr)
		}
		validity = v
		inner = raw[need:]
	}
	return inner, validity, false, nil
}

// ReadPagePayload reads a page's raw payload bytes and parses the validity prefix without
// invoking the codec decoder. Predicate-on-encoded evaluation hooks in here to inspect the
// encoded bytes directly so a column that exists only for filtering does not have to pay
// the full decode + materialize cost.
func (s *Segment) ReadPagePayload(colIdx, pageIdx int, scratch []byte) (innerPayload []byte, validity vector.Validity, allNull bool, scratchOut []byte, err error) {
	if colIdx < 0 || colIdx >= len(s.Cols) {
		return nil, nil, false, scratch, fmt.Errorf("ReadPagePayload: col %d out of range [0, %d)", colIdx, len(s.Cols))
	}
	col := &s.Cols[colIdx]
	if pageIdx < 0 || pageIdx >= len(col.Pages) {
		return nil, nil, false, scratch, fmt.Errorf("ReadPagePayload: page %d out of range [0, %d)", pageIdx, len(col.Pages))
	}
	page := col.Pages[pageIdx]
	if page.Flags&PageFlagAllNull != 0 {
		return nil, nil, true, scratch, nil
	}
	if cap(scratch) < int(page.PayloadLength) {
		scratch = make([]byte, page.PayloadLength)
	} else {
		scratch = scratch[:page.PayloadLength]
	}
	ioStart := nowNanos()
	if _, err := s.f.ReadAt(scratch, int64(page.PayloadOffset)); err != nil {
		return nil, nil, false, scratch, fmt.Errorf("ReadPagePayload: read payload: %w", err)
	}
	addIORead(nowNanos() - ioStart)
	innerPayload, validity, allNull, err = parsePageBytes(page, scratch)
	return innerPayload, validity, allNull, scratch, err
}

func allocVecForKind(k vector.VecKind, rows int) vector.Vec {
	if k.IsVarBytes() {
		return vector.NewVarVec(k, rows, 0)
	}
	return vector.NewVec(k, rows)
}

func allInvalidValidity(rows int) vector.Validity {
	if rows == 0 {
		return nil
	}
	return make(vector.Validity, vector.ValidityWords(rows))
}

// footerLayout captures the trailer offsets parsed from the suffix: where the
// column footer starts, where (and how big) the optional sidecar trailer is, and
// the segment body's exclusive end (also footerStart).
type footerLayout struct {
	cols         []SegmentColumn
	bodyEnd      int64
	sidecarStart int64
	sidecarLen   int64
}

func readFooter(f *os.File) (footerLayout, error) {
	var out footerLayout
	fi, err := f.Stat()
	if err != nil {
		return out, err
	}
	size := fi.Size()
	if size < int64(MagicLen+FooterSuffixSize) {
		return out, fmt.Errorf("readFooter: file too small (%d bytes)", size)
	}
	suffix, err := readExactAt(f, size-int64(FooterSuffixSize), FooterSuffixSize, "suffix")
	if err != nil {
		return out, err
	}
	footerLen, sidecarLen, ok := ReadFooterSuffix(suffix)
	if !ok {
		return out, fmt.Errorf("readFooter: tail magic mismatch")
	}
	if footerLen > uint64(math.MaxInt32) {
		return out, fmt.Errorf("readFooter: footer length %d exceeds int32 range", footerLen)
	}
	if sidecarLen > uint64(math.MaxInt32) {
		return out, fmt.Errorf("readFooter: sidecar length %d exceeds int32 range", sidecarLen)
	}
	sidecarStart := size - int64(FooterSuffixSize) - int64(sidecarLen)
	footerStart := sidecarStart - int64(footerLen)
	if footerStart < int64(MagicLen) {
		return out, fmt.Errorf("readFooter: footer+sidecar lengths imply negative offset")
	}
	head, err := readExactAt(f, 0, MagicLen, "head magic")
	if err != nil {
		return out, err
	}
	if string(head) != Magic {
		return out, fmt.Errorf("readFooter: head magic mismatch")
	}
	body, err := readExactAt(f, footerStart, int(footerLen), "body")
	if err != nil {
		return out, err
	}
	cols, err := parseFooter(body)
	if err != nil {
		return out, err
	}
	out.cols = cols
	out.bodyEnd = footerStart
	out.sidecarStart = sidecarStart
	out.sidecarLen = int64(sidecarLen)
	return out, nil
}

func readExactAt(f *os.File, off int64, n int, label string) ([]byte, error) {
	buf := make([]byte, n)
	if _, err := f.ReadAt(buf, off); err != nil {
		return nil, fmt.Errorf("readFooter: read %s: %w", label, err)
	}
	return buf, nil
}

// 53 = name_len(2) + kind(1) + enc_flags(1) + label_count(4) + rows(4) + null_count(4) + marker(1) + stats(16) + heavy_blob(16) + page_count(4).
const minColumnEntrySize = 53

func parseFooter(body []byte) ([]SegmentColumn, error) {
	r := newWireReader(body)
	colCount := int(r.U32())
	if r.Err() != nil {
		return nil, r.Err()
	}
	if colCount < 0 || colCount > (len(body)-4)/minColumnEntrySize {
		return nil, fmt.Errorf("parseFooter: column count %d exceeds body bound for %d-byte footer", colCount, len(body))
	}
	cols := make([]SegmentColumn, colCount)
	for i := range cols {
		c := &cols[i]
		c.Name = r.LenPrefixedString()
		c.Kind = vector.VecKind(r.U8())
		encodingFlags := r.U8()
		if encodingFlags != 0 {
			return nil, fmt.Errorf("parseFooter: col %d encoding_flags %d != 0", i, encodingFlags)
		}
		labelCount := int(r.U32())
		if labelCount < 0 || labelCount > r.Remaining()/2 {
			return nil, fmt.Errorf("parseFooter: col %d label count %d exceeds remaining footer", i, labelCount)
		}
		if labelCount > 0 {
			c.EnumLabels = make([]string, labelCount)
			for j := range labelCount {
				c.EnumLabels[j] = r.LenPrefixedString()
			}
		}
		c.Rows = r.U32()
		c.NullCount = r.U32()
		c.Marker = r.U8()
		copy(c.Stats[:], r.Raw(StatsWireSize))
		heavyBlobOffset := r.U64()
		heavyBlobLength := r.U64()
		if heavyBlobOffset != 0 || heavyBlobLength != 0 {
			return nil, fmt.Errorf("parseFooter: col %d heavy blob reserved fields %d/%d != 0", i, heavyBlobOffset, heavyBlobLength)
		}
		pageCount := int(r.U32())
		if pageCount < 0 || pageCount > r.Remaining()/PageEntrySize {
			return nil, fmt.Errorf("parseFooter: col %d page count %d exceeds remaining footer", i, pageCount)
		}
		c.Pages = make([]Page, pageCount)
	}
	for i := range cols {
		for j := range cols[i].Pages {
			cols[i].Pages[j] = DecodePageEntry(r.Raw(PageEntrySize))
		}
	}
	for i := range cols {
		n := len(cols[i].Pages)
		if n == 0 {
			continue
		}
		cols[i].pageStatsRaw = r.Raw(n * StatsWireSize)
	}
	if err := r.Err(); err != nil {
		return nil, err
	}
	if !r.AtEnd() {
		return nil, fmt.Errorf("parseFooter: %d trailing bytes after page directory", r.Remaining())
	}
	return cols, nil
}
