package storage

import (
	"fmt"
	"math"
	"os"

	"github.com/kylegrahammatzen/dripsql/internal/storage/codec"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type SegmentReadPlan struct {
	Path       string
	Meta       *SegmentMeta
	Size       int64
	Indexes    []int
	AllColumns bool
	Encoded    map[string]struct{}
	PageInfos  []SegmentPageInfo
	Cache      *segmentFileCache

	scratch          []byte
	decodeScratch    []types.Vec
	EncodedConstants bool
}

type SegmentPageInfo struct {
	RowStart     uint32
	Rows         uint32
	PayloadBytes int
}

// ReadSegmentBatch reads the same page index from every column and assembles a batch.
func ReadSegmentBatch(path string, meta SegmentMeta, pageIndex int) (types.Batch, int, error) {
	return ReadSegmentBatchColumns(path, meta, pageIndex, nil)
}

// ReadSegmentBatchColumns reads the same page index from selected columns and assembles a batch.
// Column names are returned in segment order. An empty column list reads every column.
func ReadSegmentBatchColumns(path string, meta SegmentMeta, pageIndex int, columns []string) (types.Batch, int, error) {
	plan, err := NewSegmentReadPlan(path, &meta, columns, nil)
	if err != nil {
		return types.Batch{}, 0, err
	}
	return plan.ReadBatch(pageIndex)
}

func NewSegmentReadPlan(path string, meta *SegmentMeta, columns []string, cache *segmentFileCache) (*SegmentReadPlan, error) {
	return newSegmentReadPlan(path, meta, columns, 0, nil, cache)
}

func newSegmentReadPlan(path string, meta *SegmentMeta, columns []string, segmentSize int64, pageInfos []SegmentPageInfo, cache *segmentFileCache) (*SegmentReadPlan, error) {
	if meta == nil {
		return nil, fmt.Errorf("segment metadata is nil")
	}
	indexes, err := segmentColumnIndexes(*meta, columns)
	if err != nil {
		return nil, err
	}
	if segmentSize < 0 {
		return nil, fmt.Errorf("segment size is negative")
	}
	if segmentSize == 0 {
		file, closeFile, err := openSegmentFile(path, cache)
		if err != nil {
			return nil, err
		}
		defer closeFile()
		info, err := file.Stat()
		if err != nil {
			return nil, err
		}
		segmentSize = info.Size()
	}

	allColumns := indexes == nil
	infos := pageInfos
	if allColumns && len(infos) != 0 {
		pages, err := segmentPageCount(*meta)
		if err != nil {
			return nil, err
		}
		if len(infos) != pages {
			return nil, fmt.Errorf("segment page info count %d does not match %d", len(infos), pages)
		}
	} else {
		infos, err = buildSegmentPageInfos(*meta, indexes)
		if err != nil {
			return nil, err
		}
	}

	return &SegmentReadPlan{Path: path, Meta: meta, Size: segmentSize, Indexes: indexes, AllColumns: allColumns, PageInfos: infos, Cache: cache}, nil
}

func (p *SegmentReadPlan) ReadBatch(pageIndex int) (types.Batch, int, error) {
	return p.readBatch(pageIndex, false)
}

func (p *SegmentReadPlan) ReadBatchReused(pageIndex int) (types.Batch, int, error) {
	return p.readBatch(pageIndex, true)
}

func (p *SegmentReadPlan) ReadBatchSelected(pageIndex int, sel types.SelectionMask) (types.Batch, int, error) {
	if p == nil {
		return types.Batch{}, 0, fmt.Errorf("segment read plan is nil")
	}
	if p.Meta == nil {
		return types.Batch{}, 0, fmt.Errorf("segment metadata is nil")
	}
	if pageIndex < 0 || pageIndex >= len(p.PageInfos) {
		return types.Batch{}, 0, fmt.Errorf("page index %d out of range", pageIndex)
	}
	colCount := len(p.Meta.Columns)
	if !p.AllColumns {
		colCount = len(p.Indexes)
	}
	if colCount == 0 {
		return types.Batch{}, 0, fmt.Errorf("segment read requires at least one column")
	}
	info := p.PageInfos[pageIndex]
	if sel.Rows != int(info.Rows) {
		return types.Batch{}, 0, fmt.Errorf("selection rows %d do not match page rows %d", sel.Rows, info.Rows)
	}

	file, closeFile, err := openSegmentFile(p.Path, p.Cache)
	if err != nil {
		return types.Batch{}, 0, err
	}
	defer closeFile()

	cols := make([]types.Column, colCount)
	payloadBytes := 0
	for outIndex := 0; outIndex < colCount; outIndex++ {
		colIndex := outIndex
		if !p.AllColumns {
			colIndex = p.Indexes[outIndex]
		}
		colMeta := p.Meta.Columns[colIndex]
		page := colMeta.Pages[pageIndex]
		if page.RowStart != info.RowStart || page.Rows != info.Rows {
			return types.Batch{}, 0, fmt.Errorf("page %d column %q row range mismatch", pageIndex, colMeta.Name)
		}
		col, bytesRead, err := p.readSelectedColumnPage(file, colIndex, pageIndex, sel)
		if err != nil {
			return types.Batch{}, 0, err
		}
		if col.V.Len != int(info.Rows) {
			return types.Batch{}, 0, fmt.Errorf("page %d column %q decoded length %d does not match metadata rows %d", pageIndex, col.Name, col.V.Len, info.Rows)
		}
		payloadBytes += bytesRead
		cols[outIndex] = col
	}
	batch, err := types.NewBatchNoClone(cols)
	if err != nil {
		return types.Batch{}, 0, err
	}
	return batch, payloadBytes, nil
}

func (p *SegmentReadPlan) readBatch(pageIndex int, reuseDecodeBuffers bool) (types.Batch, int, error) {
	if p == nil {
		return types.Batch{}, 0, fmt.Errorf("segment read plan is nil")
	}
	if p.Meta == nil {
		return types.Batch{}, 0, fmt.Errorf("segment metadata is nil")
	}
	if pageIndex < 0 || pageIndex >= len(p.PageInfos) {
		return types.Batch{}, 0, fmt.Errorf("page index %d out of range", pageIndex)
	}
	colCount := len(p.Meta.Columns)
	if !p.AllColumns {
		colCount = len(p.Indexes)
	}
	if colCount == 0 {
		return types.Batch{}, 0, fmt.Errorf("segment read requires at least one column")
	}

	file, closeFile, err := openSegmentFile(p.Path, p.Cache)
	if err != nil {
		return types.Batch{}, 0, err
	}
	defer closeFile()

	info := p.PageInfos[pageIndex]
	cols := make([]types.Column, colCount)
	if reuseDecodeBuffers && len(p.decodeScratch) != colCount {
		p.decodeScratch = make([]types.Vec, colCount)
	}
	for outIndex := 0; outIndex < colCount; outIndex++ {
		colIndex := outIndex
		if !p.AllColumns {
			colIndex = p.Indexes[outIndex]
		}
		colMeta := p.Meta.Columns[colIndex]
		page := colMeta.Pages[pageIndex]
		if page.RowStart != info.RowStart || page.Rows != info.Rows {
			return types.Batch{}, 0, fmt.Errorf("page %d column %q row range mismatch", pageIndex, colMeta.Name)
		}
		var dst *types.Vec
		if reuseDecodeBuffers {
			dst = &p.decodeScratch[outIndex]
		}
		col, err := p.readColumnPage(file, colIndex, pageIndex, dst)
		if err != nil {
			return types.Batch{}, 0, err
		}
		if col.V.Len != int(info.Rows) {
			return types.Batch{}, 0, fmt.Errorf("page %d column %q decoded length %d does not match metadata rows %d", pageIndex, col.Name, col.V.Len, info.Rows)
		}
		cols[outIndex] = col
	}

	batch, err := types.NewBatchNoClone(cols)
	if err != nil {
		return types.Batch{}, 0, err
	}
	return batch, info.PayloadBytes, nil
}

func (p *SegmentReadPlan) readColumnPage(file *os.File, columnIndex int, pageIndex int, dst *types.Vec) (types.Column, error) {
	if p.shouldReadEncoded(columnIndex, pageIndex) {
		col, scratch, err := readEncodedColumnPageFromFile(file, *p.Meta, columnIndex, pageIndex, p.Size, p.scratch)
		p.scratch = scratch
		return col, err
	}
	col, scratch, err := readColumnPageFromFileInto(file, *p.Meta, columnIndex, pageIndex, p.Size, p.scratch, dst)
	p.scratch = scratch
	return col, err
}

func (p *SegmentReadPlan) readSelectedColumnPage(file *os.File, columnIndex int, pageIndex int, sel types.SelectionMask) (types.Column, int, error) {
	colMeta := p.Meta.Columns[columnIndex]
	pageMeta := colMeta.Pages[pageIndex]
	if p.shouldReadEncoded(columnIndex, pageIndex) {
		encoded, scratch, err := readEncodedColumnPageFromFile(file, *p.Meta, columnIndex, pageIndex, p.Size, p.scratch)
		p.scratch = scratch
		return encoded, int(pageMeta.Length), err
	}
	if pageMeta.Encoding == types.EncodingFORBitPack {
		encoded, scratch, err := readEncodedColumnPageFromFile(file, *p.Meta, columnIndex, pageIndex, p.Size, p.scratch)
		p.scratch = scratch
		if err != nil {
			return types.Column{}, 0, err
		}
		col, err := materializeFORBitPackSelected(encoded, sel)
		return col, int(pageMeta.Length), err
	}
	payload, scratch, err := readPagePayload(file, pageMeta, p.Size, p.scratch)
	p.scratch = scratch
	if err != nil {
		return types.Column{}, 0, fmt.Errorf("page %d for column %q: %w", pageIndex, colMeta.Name, err)
	}
	c, err := pageCodec(pageMeta.Encoding)
	if err != nil {
		return types.Column{}, 0, err
	}
	if selected, ok := c.(codec.SelectedCodec); ok {
		page := codec.Page{Kind: pageMeta.Kind, Encoding: pageMeta.Encoding, Rows: int(pageMeta.Rows), NullCount: int(pageMeta.NullCount), Payload: payload}
		vec, err := selected.DecodeSelected(page, sel)
		if err != nil {
			return types.Column{}, 0, err
		}
		return types.Column{Name: colMeta.Name, Type: colMeta.Type, EnumLabels: append([]string(nil), colMeta.EnumLabels...), V: vec}, int(pageMeta.Length), nil
	}
	col, err := decodeColumnPageInto(colMeta, pageMeta, payload, nil)
	return col, int(pageMeta.Length), err
}

func (p *SegmentReadPlan) shouldReadEncoded(columnIndex int, pageIndex int) bool {
	if len(p.Encoded) == 0 || p.Meta == nil || columnIndex < 0 || columnIndex >= len(p.Meta.Columns) {
		return false
	}
	colMeta := p.Meta.Columns[columnIndex]
	if _, ok := p.Encoded[colMeta.Name]; !ok {
		return false
	}
	if pageIndex < 0 || pageIndex >= len(colMeta.Pages) {
		return false
	}
	encoding := colMeta.Pages[pageIndex].Encoding
	return encoding == types.EncodingFORBitPack || (encoding == types.EncodingConstant && p.EncodedConstants)
}

func ReadColumnPage(path string, meta SegmentMeta, columnIndex int, pageIndex int) (types.Column, error) {
	file, closeFile, err := openSegmentFile(path, nil)
	if err != nil {
		return types.Column{}, err
	}
	defer closeFile()
	col, _, err := readColumnPageFromFileInto(file, meta, columnIndex, pageIndex, 0, nil, nil)
	return col, err
}

func openSegmentFile(path string, cache *segmentFileCache) (*os.File, func(), error) {
	if cache != nil {
		file, release, err := cache.Open(path)
		if err != nil {
			return nil, nil, err
		}
		return file, release, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	return file, func() { _ = file.Close() }, nil
}

// resolveColumnPage validates the file/index/size triple and resolves the
// column+page metadata used by both the in-place and encoded-passthrough
// readers. Returning the segment size lets the caller fill it in lazily via
// Stat() when the writer didn't pre-cache it.
func resolveColumnPage(file *os.File, meta SegmentMeta, columnIndex int, pageIndex int, segmentSize int64) (ColumnMeta, PageMeta, int64, error) {
	if file == nil {
		return ColumnMeta{}, PageMeta{}, 0, fmt.Errorf("segment file is nil")
	}
	if columnIndex < 0 || columnIndex >= len(meta.Columns) {
		return ColumnMeta{}, PageMeta{}, 0, fmt.Errorf("column index %d out of range", columnIndex)
	}
	colMeta := meta.Columns[columnIndex]
	if pageIndex < 0 || pageIndex >= len(colMeta.Pages) {
		return ColumnMeta{}, PageMeta{}, 0, fmt.Errorf("page index %d out of range", pageIndex)
	}
	if segmentSize < 0 {
		return ColumnMeta{}, PageMeta{}, 0, fmt.Errorf("segment size is negative")
	}
	if segmentSize == 0 {
		info, err := file.Stat()
		if err != nil {
			return ColumnMeta{}, PageMeta{}, 0, err
		}
		segmentSize = info.Size()
	}
	return colMeta, colMeta.Pages[pageIndex], segmentSize, nil
}

func readColumnPageFromFileInto(file *os.File, meta SegmentMeta, columnIndex int, pageIndex int, segmentSize int64, scratch []byte, dst *types.Vec) (types.Column, []byte, error) {
	colMeta, pageMeta, segmentSize, err := resolveColumnPage(file, meta, columnIndex, pageIndex, segmentSize)
	if err != nil {
		return types.Column{}, scratch, err
	}
	payload, scratch, err := readPagePayload(file, pageMeta, segmentSize, scratch)
	if err != nil {
		return types.Column{}, scratch, fmt.Errorf("page %d for column %q: %w", pageIndex, colMeta.Name, err)
	}
	col, err := decodeColumnPageInto(colMeta, pageMeta, payload, dst)
	if err != nil {
		return types.Column{}, scratch, err
	}
	return col, scratch, nil
}

func readEncodedColumnPageFromFile(file *os.File, meta SegmentMeta, columnIndex int, pageIndex int, segmentSize int64, scratch []byte) (types.Column, []byte, error) {
	colMeta, pageMeta, segmentSize, err := resolveColumnPage(file, meta, columnIndex, pageIndex, segmentSize)
	if err != nil {
		return types.Column{}, scratch, err
	}
	payload, scratch, err := readPagePayload(file, pageMeta, segmentSize, scratch)
	if err != nil {
		return types.Column{}, scratch, fmt.Errorf("page %d for column %q: %w", pageIndex, colMeta.Name, err)
	}
	c, err := pageCodec(pageMeta.Encoding)
	if err != nil {
		return types.Column{}, scratch, err
	}
	encoded, ok := c.(codec.EncodedCodec)
	if !ok {
		return types.Column{}, scratch, fmt.Errorf("codec %s cannot expose encoded pages", pageMeta.Encoding)
	}
	page := codec.Page{Kind: pageMeta.Kind, Encoding: pageMeta.Encoding, Rows: int(pageMeta.Rows), NullCount: int(pageMeta.NullCount), Payload: payload}
	vec, err := encoded.DecodeEncoded(page)
	if err != nil {
		return types.Column{}, scratch, err
	}
	return types.Column{Name: colMeta.Name, Type: colMeta.Type, EnumLabels: append([]string(nil), colMeta.EnumLabels...), V: vec}, scratch, nil
}

func materializeFORBitPackSelected(col types.Column, sel types.SelectionMask) (types.Column, error) {
	v := col.V
	out := types.Vec{Kind: v.Kind, Encoding: types.EncodingFlat, Len: v.Len}
	if v.Valid != nil {
		out.Valid = append(types.Validity(nil), v.Valid...)
	}
	switch v.Kind {
	case types.VecInt16:
		out.I16 = make([]int16, v.Len)
		sel.IterSet(func(row int) {
			if types.IsValid(v.Valid, row) {
				out.I16[row] = int16(forBitPackValue(v, row))
			}
		})
	case types.VecInt32, types.VecDate:
		out.I32 = make([]int32, v.Len)
		sel.IterSet(func(row int) {
			if types.IsValid(v.Valid, row) {
				out.I32[row] = int32(forBitPackValue(v, row))
			}
		})
	case types.VecInt64, types.VecTimestamp, types.VecTime:
		out.I64 = make([]int64, v.Len)
		sel.IterSet(func(row int) {
			if types.IsValid(v.Valid, row) {
				out.I64[row] = forBitPackValue(v, row)
			}
		})
	default:
		return types.Column{}, fmt.Errorf("for+bitpack selected materialization unsupported kind %s", v.Kind)
	}
	col.V = out
	return col, nil
}

func readPagePayload(file *os.File, page PageMeta, segmentSize int64, scratch []byte) ([]byte, []byte, error) {
	readOffset, err := pageReadOffset(page)
	if err != nil {
		return nil, scratch, err
	}
	segmentBytes := uint64(segmentSize)
	if page.Offset > segmentBytes || page.Length > segmentBytes-page.Offset {
		return nil, scratch, fmt.Errorf("extends beyond segment")
	}
	n := int(page.Length)
	if cap(scratch) < n {
		scratch = make([]byte, n)
	}
	payload := scratch[:n]
	if _, err := file.ReadAt(payload, readOffset); err != nil {
		return nil, scratch, err
	}
	return payload, scratch, nil
}

func decodeColumnPageInto(colMeta ColumnMeta, pageMeta PageMeta, payload []byte, dst *types.Vec) (types.Column, error) {
	c, err := pageCodec(pageMeta.Encoding)
	if err != nil {
		return types.Column{}, err
	}
	page := codec.Page{Kind: pageMeta.Kind, Encoding: pageMeta.Encoding, Rows: int(pageMeta.Rows), NullCount: int(pageMeta.NullCount), Payload: payload}
	var vec types.Vec
	if dst != nil {
		if reusable, ok := c.(codec.ReusableCodec); ok {
			if err := reusable.DecodeInto(page, dst); err != nil {
				return types.Column{}, err
			}
			vec = *dst
		} else {
			decoded, err := c.Decode(page)
			if err != nil {
				return types.Column{}, err
			}
			*dst = decoded
			vec = decoded
		}
	} else {
		decoded, err := c.Decode(page)
		if err != nil {
			return types.Column{}, err
		}
		vec = decoded
	}
	return types.Column{Name: colMeta.Name, Type: colMeta.Type, EnumLabels: append([]string(nil), colMeta.EnumLabels...), V: vec}, nil
}

func pageCodec(enc types.Encoding) (codec.Codec, error) {
	if c, ok := codec.Lookup(enc); ok {
		return c, nil
	}
	switch enc {
	case types.EncodingFlat:
		return codec.Plain{}, nil
	case types.EncodingDictionary:
		return codec.Dictionary{}, nil
	case types.EncodingConstant:
		return codec.Constant{}, nil
	case types.EncodingFORBitPack:
		return codec.FORBitPack{}, nil
	default:
		return nil, fmt.Errorf("missing codec %s", enc)
	}
}

func pageReadOffset(page PageMeta) (int64, error) {
	if page.Length > uint64(math.MaxInt) {
		return 0, fmt.Errorf("payload size exceeds int capacity")
	}
	if page.Offset > math.MaxInt64 || page.Length > math.MaxInt64-page.Offset {
		return 0, fmt.Errorf("offset exceeds int64 capacity")
	}
	return int64(page.Offset), nil
}

// segmentColumnIndexes returns nil for all columns. Requested duplicates are deduplicated;
// returned indexes always follow segment column order.
func segmentColumnIndexes(meta SegmentMeta, columns []string) ([]int, error) {
	if len(columns) == 0 {
		return nil, nil
	}
	wanted := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		if column == "" {
			return nil, fmt.Errorf("column name is required")
		}
		wanted[column] = struct{}{}
	}
	indexes := make([]int, 0, len(wanted))
	for i, col := range meta.Columns {
		if _, ok := wanted[col.Name]; ok {
			indexes = append(indexes, i)
			delete(wanted, col.Name)
		}
	}
	if len(wanted) != 0 {
		for column := range wanted {
			return nil, fmt.Errorf("missing segment column %q", column)
		}
	}
	return indexes, nil
}

func buildSegmentPageInfos(meta SegmentMeta, indexes []int) ([]SegmentPageInfo, error) {
	pages, err := segmentPageCount(meta)
	if err != nil {
		return nil, err
	}
	infos := make([]SegmentPageInfo, pages)
	if pages == 0 {
		return infos, nil
	}
	firstColumnIndex := 0
	if indexes != nil {
		if len(indexes) == 0 {
			return nil, fmt.Errorf("segment read requires at least one column")
		}
		firstColumnIndex = indexes[0]
		if firstColumnIndex < 0 || firstColumnIndex >= len(meta.Columns) {
			return nil, fmt.Errorf("column index %d out of range", firstColumnIndex)
		}
	}
	for pageIndex, page := range meta.Columns[firstColumnIndex].Pages {
		infos[pageIndex].RowStart = page.RowStart
		infos[pageIndex].Rows = page.Rows
	}
	walk := func(colIndex int) error {
		if colIndex < 0 || colIndex >= len(meta.Columns) {
			return fmt.Errorf("column index %d out of range", colIndex)
		}
		col := meta.Columns[colIndex]
		if len(col.Pages) != len(infos) {
			return fmt.Errorf("column %q page count %d does not match %d", col.Name, len(col.Pages), len(infos))
		}
		for pageIndex, page := range col.Pages {
			info := &infos[pageIndex]
			if page.RowStart != info.RowStart || page.Rows != info.Rows {
				return fmt.Errorf("page %d column %q row range mismatch", pageIndex, col.Name)
			}
			if page.Length > uint64(math.MaxInt-info.PayloadBytes) {
				return fmt.Errorf("page %d payload size exceeds int capacity", pageIndex)
			}
			info.PayloadBytes += int(page.Length)
		}
		return nil
	}
	if indexes == nil {
		for colIndex := range meta.Columns {
			if err := walk(colIndex); err != nil {
				return nil, err
			}
		}
		return infos, nil
	}
	for _, colIndex := range indexes {
		if err := walk(colIndex); err != nil {
			return nil, err
		}
	}
	return infos, nil
}

func segmentPageCount(meta SegmentMeta) (int, error) {
	if len(meta.Columns) == 0 {
		return 0, fmt.Errorf("segment has no columns")
	}
	pages := len(meta.Columns[0].Pages)
	for _, col := range meta.Columns[1:] {
		if len(col.Pages) != pages {
			return 0, fmt.Errorf("column %q page count %d does not match %d", col.Name, len(col.Pages), pages)
		}
	}
	return pages, nil
}
