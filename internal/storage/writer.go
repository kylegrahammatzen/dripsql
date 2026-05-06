package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"net"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type preparedColumn struct {
	meta          Column
	payload       []byte
	relativePages []Page
	pages         []byte
	filters       []byte
}

// WriteSegment writes an immutable segment containing int64 and string columns.
func WriteSegment(w io.Writer, batch vector.Batch) (SegmentStats, error) {
	rowCount := batch.Count
	columnCount := len(batch.Columns)

	if rowCount < 0 {
		return SegmentStats{}, fmt.Errorf("negative row count %d", rowCount)
	}
	if uint64(columnCount) > uint64(^uint32(0)) {
		return SegmentStats{}, fmt.Errorf("column count %d overflows uint32", columnCount)
	}

	// This first slice keeps all payloads resident so the footer can be written last.
	// If segment sizes grow substantially, split this into size-planning and streaming encode passes.
	prepared := make([]preparedColumn, columnCount)
	seen := make(map[string]struct{}, columnCount)
	nextOffset := headerLen64
	for i := range batch.Columns {
		col := batch.Columns[i]
		if err := validateWriteColumn(col, rowCount, seen); err != nil {
			return SegmentStats{}, err
		}

		preparedCol, err := prepareColumn(col)
		if err != nil {
			return SegmentStats{}, err
		}
		preparedCol.meta.Count = rowCount
		payloadRange, err := reserveSegmentRange(&nextOffset, len(preparedCol.payload), "segment payload offset")
		if err != nil {
			return SegmentStats{}, err
		}
		preparedCol.meta.Payload = payloadRange
		if preparedCol.meta.Dictionary.Bytes > 0 {
			preparedCol.meta.Dictionary.Offset = payloadRange.Offset
		}
		prepared[i] = preparedCol
	}

	columns := make([]Column, columnCount)
	for i := range prepared {
		col := &prepared[i]
		if len(col.relativePages) > 0 {
			makePagesAbsolute(col.relativePages, col.meta.Payload.Offset)
			pagePayload, err := encodePageDirectory(col.relativePages)
			if err != nil {
				return SegmentStats{}, fmt.Errorf("encode page directory for %q: %w", col.meta.Name, err)
			}
			pageRange, err := reserveSegmentRange(&nextOffset, len(pagePayload), "segment page offset")
			if err != nil {
				return SegmentStats{}, err
			}
			col.pages = pagePayload
			col.meta.Pages = pageRange
		}
		if len(col.filters) > 0 {
			filterRange, err := reserveSegmentRange(&nextOffset, len(col.filters), "segment filter offset")
			if err != nil {
				return SegmentStats{}, err
			}
			col.meta.Filters = filterRange
		}
		columns[i] = col.meta
	}

	footer, err := encodeFooter(columns)
	if err != nil {
		return SegmentStats{}, err
	}
	footerAndTrailer, err := checkedAddInt64("footer and trailer length", int64(len(footer)), footerTailLen64)
	if err != nil {
		return SegmentStats{}, err
	}
	segmentLen, err := checkedAddInt64("segment length", nextOffset, footerAndTrailer)
	if err != nil {
		return SegmentStats{}, err
	}

	header := segmentHeader(rowCount, columnCount, segmentLen)
	var trailer [footerTailLen]byte
	binary.LittleEndian.PutUint64(trailer[:], uint64(len(footer)))
	binary.LittleEndian.PutUint32(trailer[8:], crc32.Checksum(footer, crc32cTable))
	copy(trailer[12:], footerMagic)

	bufs := segmentBuffers(header[:], prepared, footer, trailer[:])
	if _, err := bufs.WriteTo(w); err != nil {
		return SegmentStats{}, fmt.Errorf("write segment: %w", err)
	}

	return SegmentStats{Rows: rowCount, Columns: columns}, nil
}

func validateWriteColumn(col vector.Column, rowCount int, seen map[string]struct{}) error {
	if col.Name == "" {
		return fmt.Errorf("column name is required")
	}
	if len(col.Name) > maxColumnName {
		return fmt.Errorf("column name %q length %d exceeds max %d", col.Name, len(col.Name), maxColumnName)
	}
	if _, ok := seen[col.Name]; ok {
		return fmt.Errorf("duplicate column %q", col.Name)
	}
	seen[col.Name] = struct{}{}
	if col.Vector == nil {
		return fmt.Errorf("column %q has no vector", col.Name)
	}
	if got := col.Vector.Len(); got != rowCount {
		return fmt.Errorf("column %q length %d does not match batch length %d", col.Name, got, rowCount)
	}
	return nil
}

func reserveSegmentRange(nextOffset *int64, byteLen int, label string) (Range, error) {
	r := Range{Offset: *nextOffset, Bytes: int64(byteLen)}
	offset, err := checkedAddInt64(label, *nextOffset, r.Bytes)
	if err != nil {
		return Range{}, err
	}
	*nextOffset = offset
	return r, nil
}

func prepareColumn(col vector.Column) (preparedColumn, error) {
	meta := Column{Name: col.Name, Kind: col.Vector.Kind(), Count: col.Vector.Len()}
	switch values := col.Vector.(type) {
	case vector.Int64:
		minValue, maxValue, ok := values.MinMax()
		meta.HasMinMax = ok
		meta.MinInt64 = minValue
		meta.MaxInt64 = maxValue
		payload, codec, dictionaryBytes, err := encodeInt64(values)
		if err != nil {
			return preparedColumn{}, fmt.Errorf("encode column %q: %w", col.Name, err)
		}
		meta.Codec = codec
		meta.Dictionary.Bytes = int64(dictionaryBytes)
		return preparedColumn{meta: meta, payload: payload, relativePages: int64PlainPages(values, codec)}, nil
	case vector.String:
		payload, codec, dictionaryBytes, err := encodeString(values)
		if err != nil {
			return preparedColumn{}, fmt.Errorf("encode column %q: %w", col.Name, err)
		}
		meta.Codec = codec
		meta.Dictionary.Bytes = int64(dictionaryBytes)
		pages, err := stringPages(values, payload, codec)
		if err != nil {
			return preparedColumn{}, fmt.Errorf("build pages for column %q: %w", col.Name, err)
		}
		var filters []byte
		if len(pages) > 0 {
			filters, err = encodeStringPageBloom(values, pages)
			if err != nil {
				return preparedColumn{}, fmt.Errorf("encode page bloom for column %q: %w", col.Name, err)
			}
		}
		return preparedColumn{meta: meta, payload: payload, relativePages: pages, filters: filters}, nil
	default:
		return preparedColumn{}, fmt.Errorf("storage encoding is not implemented for %s vectors", col.Vector.Kind())
	}
}

func makePagesAbsolute(pages []Page, payloadOffset int64) {
	for i := range pages {
		pages[i].Payload.Offset += payloadOffset
		if pages[i].Values.Offset != 0 || pages[i].Values.Bytes != 0 {
			pages[i].Values.Offset += payloadOffset
		}
	}
}

func int64PlainPages(values vector.Int64, codec Codec) []Page {
	n := values.Len()
	if codec != CodecPlain || n == 0 {
		return nil
	}
	vals := values.Values[:n]
	pageCount := (n + defaultPageRows - 1) / defaultPageRows
	pages := make([]Page, 0, pageCount)
	for start := 0; start < n; start += defaultPageRows {
		end := min(start+defaultPageRows, n)
		pageValues := vals[start:end]
		minValue, maxValue := minMaxInt64(pageValues)
		pages = append(pages, Page{
			StartRow:  start,
			Count:     end - start,
			Payload:   Range{Offset: int64(start * int64Size), Bytes: int64((end - start) * int64Size)},
			HasMinMax: true,
			MinInt64:  minValue,
			MaxInt64:  maxValue,
		})
	}
	return pages
}

func minMaxInt64(values []int64) (int64, int64) {
	minValue := values[0]
	maxValue := values[0]
	for _, value := range values[1:] {
		if value < minValue {
			minValue = value
		}
		if value > maxValue {
			maxValue = value
		}
	}
	return minValue, maxValue
}

func stringPlainPages(values vector.String, payload []byte, codec Codec) []Page {
	n := values.Len()
	if codec != CodecPlain || n == 0 {
		return nil
	}
	offsetBytes := int64((n + 1) * stringOffsetWidth)
	pageCount := (n + defaultPageRows - 1) / defaultPageRows
	pages := make([]Page, 0, pageCount)
	for start := 0; start < n; start += defaultPageRows {
		end := min(start+defaultPageRows, n)
		startOffset := start * stringOffsetWidth
		endOffset := end * stringOffsetWidth
		dataStart := int64(binary.LittleEndian.Uint32(payload[startOffset : startOffset+stringOffsetWidth]))
		dataEnd := int64(binary.LittleEndian.Uint32(payload[endOffset : endOffset+stringOffsetWidth]))
		pages = append(pages, Page{
			StartRow: start,
			Count:    end - start,
			Payload:  Range{Offset: int64(startOffset), Bytes: int64(endOffset - startOffset + stringOffsetWidth)},
			Values:   Range{Offset: offsetBytes + dataStart, Bytes: dataEnd - dataStart},
		})
	}
	return pages
}

func stringPages(values vector.String, payload []byte, codec Codec) ([]Page, error) {
	switch codec {
	case CodecPlain:
		return stringPlainPages(values, payload, codec), nil
	case CodecStringPrefix:
		return stringPrefixPages(values, payload)
	default:
		return nil, nil
	}
}

func stringPrefixPages(values vector.String, payload []byte) ([]Page, error) {
	n := values.Len()
	if n == 0 {
		return nil, nil
	}
	_, _, middlePayload, err := parseStringPrefixPayload(payload, n)
	if err != nil {
		return nil, err
	}
	middleOffset := len(payload) - len(middlePayload)
	offsetBytes := int64((n + 1) * stringOffsetWidth)
	pageCount := (n + defaultPageRows - 1) / defaultPageRows
	pages := make([]Page, 0, pageCount)
	for start := 0; start < n; start += defaultPageRows {
		end := min(start+defaultPageRows, n)
		startOffset := start * stringOffsetWidth
		endOffset := end * stringOffsetWidth
		dataStart := int64(binary.LittleEndian.Uint32(middlePayload[startOffset : startOffset+stringOffsetWidth]))
		dataEnd := int64(binary.LittleEndian.Uint32(middlePayload[endOffset : endOffset+stringOffsetWidth]))
		pages = append(pages, Page{
			StartRow: start,
			Count:    end - start,
			Payload:  Range{Offset: int64(middleOffset + startOffset), Bytes: int64(endOffset - startOffset + stringOffsetWidth)},
			Values:   Range{Offset: int64(middleOffset) + offsetBytes + dataStart, Bytes: dataEnd - dataStart},
		})
	}
	return pages, nil
}

func segmentBuffers(header []byte, prepared []preparedColumn, footer []byte, trailer []byte) net.Buffers {
	bufs := make(net.Buffers, 0, 3+len(prepared)*3)
	bufs = append(bufs, header)
	for i := range prepared {
		if len(prepared[i].payload) > 0 {
			bufs = append(bufs, prepared[i].payload)
		}
	}
	for i := range prepared {
		if len(prepared[i].pages) > 0 {
			bufs = append(bufs, prepared[i].pages)
		}
		if len(prepared[i].filters) > 0 {
			bufs = append(bufs, prepared[i].filters)
		}
	}
	bufs = append(bufs, footer, trailer)
	return bufs
}

func segmentHeader(rows int, columns int, segmentLen int64) [headerLen]byte {
	var buf [headerLen]byte
	off := copy(buf[:], segmentMagic)
	binary.LittleEndian.PutUint16(buf[off:], formatVersion)
	off += 2
	binary.LittleEndian.PutUint64(buf[off:], uint64(rows))
	off += 8
	binary.LittleEndian.PutUint32(buf[off:], uint32(columns))
	off += 4
	binary.LittleEndian.PutUint64(buf[off:], uint64(segmentLen))
	return buf
}
