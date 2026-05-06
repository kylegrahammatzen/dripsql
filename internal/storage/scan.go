package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/util"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// ScanStats describes the physical work done by one scan.
type ScanStats struct {
	BytesRead       int64
	BytesScanned    int64
	RowsScanned     int
	RowsSkipped     int
	SegmentsScanned int
	PagesScanned    int
}

// CountInt64Equal counts rows where column equals value.
func CountInt64Equal(reader Reader, column string, value int64, scratch []byte) (int, []byte, ScanStats, error) {
	col, ok := reader.dir.Column(column)
	if !ok {
		return 0, scratch, ScanStats{}, fmt.Errorf("missing column %q", column)
	}
	if col.Kind != vector.KindInt64 {
		return 0, scratch, ScanStats{}, fmt.Errorf("column %q is %s, want int64", column, col.Kind)
	}
	stats := ScanStats{SegmentsScanned: 1}
	if col.HasMinMax && (value < col.MinInt64 || value > col.MaxInt64) {
		stats.RowsSkipped = col.Count
		return 0, scratch, stats, nil
	}
	switch col.Codec {
	case CodecDictionary:
		return countInt64EqualDictionary(reader, col, value, scratch, stats)
	case CodecInt64Sequence:
		return countInt64EqualSequence(reader, col, value, scratch, stats)
	case CodecPlain:
		payload, nextScratch, stats, err := readPlainPayload(reader, col, scratch)
		if err != nil {
			return 0, scratch, stats, err
		}
		count, err := countInt64EqualPlain(payload, col.Count, value)
		if err != nil {
			return 0, nextScratch, stats, fmt.Errorf("count column %q: %w", column, err)
		}
		return count, nextScratch, stats, nil
	default:
		return 0, scratch, ScanStats{}, unsupportedColumnCodec(column, col.Codec)
	}
}

// CountStringEqual counts rows where column equals value.
func CountStringEqual(reader Reader, column string, value string, scratch []byte) (int, []byte, ScanStats, error) {
	col, ok := reader.dir.Column(column)
	if !ok {
		return 0, scratch, ScanStats{}, fmt.Errorf("missing column %q", column)
	}
	if col.Kind != vector.KindString {
		return 0, scratch, ScanStats{}, fmt.Errorf("column %q is %s, want string", column, col.Kind)
	}
	stats := ScanStats{SegmentsScanned: 1}
	switch col.Codec {
	case CodecDictionary:
		return countStringEqualDictionary(reader, col, value, scratch, stats)
	case CodecStringPrefix:
		if col.Pages.Bytes > 0 && col.Filters.Bytes > 0 {
			return countStringEqualPrefixPageBloom(reader, col, value, scratch, stats)
		}
		return countStringEqualPrefix(reader, col, value, scratch, stats)
	case CodecStringTemplate:
		return countStringEqualTemplate(reader, col, value, scratch, stats)
	case CodecPlain:
		if col.Pages.Bytes > 0 && col.Filters.Bytes > 0 {
			return countStringEqualPageBloom(reader, col, value, scratch, stats)
		}
		payload, nextScratch, stats, err := readPlainPayload(reader, col, scratch)
		if err != nil {
			return 0, scratch, stats, err
		}
		count, err := countStringEqualPlain(payload, col.Count, value)
		if err != nil {
			return 0, nextScratch, stats, fmt.Errorf("count column %q: %w", column, err)
		}
		return count, nextScratch, stats, nil
	default:
		return 0, scratch, ScanStats{}, unsupportedColumnCodec(column, col.Codec)
	}
}

// GroupStringCounts adds per-value row counts for a string column into counts.
func GroupStringCounts(reader Reader, column string, counts map[string]int, scratch []byte, keyScratch []string) (map[string]int, []byte, []string, ScanStats, error) {
	if counts == nil {
		counts = make(map[string]int)
	}
	col, ok := reader.dir.Column(column)
	if !ok {
		return counts, scratch, keyScratch, ScanStats{}, fmt.Errorf("missing column %q", column)
	}
	if col.Kind != vector.KindString {
		return counts, scratch, keyScratch, ScanStats{}, fmt.Errorf("column %q is %s, want string", column, col.Kind)
	}
	stats := ScanStats{SegmentsScanned: 1}
	switch col.Codec {
	case CodecDictionary:
		return groupStringCountsDictionary(reader, col, counts, scratch, keyScratch, stats)
	case CodecStringPrefix:
		payload, nextScratch, stats, err := readPlainPayload(reader, col, scratch)
		if err != nil {
			return counts, scratch, keyScratch, stats, err
		}
		if err := groupStringCountsPrefix(payload, col.Count, counts); err != nil {
			return counts, nextScratch, keyScratch, stats, fmt.Errorf("group column %q: %w", column, err)
		}
		return counts, nextScratch, keyScratch, stats, nil
	case CodecStringTemplate:
		payload, nextScratch, stats, err := readPlainPayload(reader, col, scratch)
		if err != nil {
			return counts, scratch, keyScratch, stats, err
		}
		if err := groupStringCountsTemplate(payload, col.Count, counts); err != nil {
			return counts, nextScratch, keyScratch, stats, fmt.Errorf("group column %q: %w", column, err)
		}
		return counts, nextScratch, keyScratch, stats, nil
	case CodecPlain:
		payload, nextScratch, stats, err := readPlainPayload(reader, col, scratch)
		if err != nil {
			return counts, scratch, keyScratch, stats, err
		}
		if err := groupStringCountsPlain(payload, col.Count, counts); err != nil {
			return counts, nextScratch, keyScratch, stats, fmt.Errorf("group column %q: %w", column, err)
		}
		return counts, nextScratch, keyScratch, stats, nil
	default:
		return counts, scratch, keyScratch, ScanStats{}, unsupportedColumnCodec(column, col.Codec)
	}
}

func readPlainPayload(reader Reader, col Column, scratch []byte) ([]byte, []byte, ScanStats, error) {
	payload, nextScratch, stable, err := reader.readPayload(col, scratch)
	stats := ScanStats{SegmentsScanned: 1}
	if err != nil {
		return nil, scratch, stats, err
	}
	stats.BytesScanned = col.Payload.Bytes
	stats.RowsScanned = col.Count
	if !stable {
		stats.BytesRead = col.Payload.Bytes
	}
	return payload, nextScratch, stats, nil
}

func unsupportedColumnCodec(column string, codec Codec) error {
	return fmt.Errorf("column %q has unsupported codec %s", column, codec)
}

func countInt64EqualDictionary(reader Reader, col Column, value int64, scratch []byte, stats ScanStats) (int, []byte, ScanStats, error) {
	metadata, nextScratch, bytesRead, err := readInt64DictionaryMetadata(reader, col, scratch)
	if err != nil {
		return 0, scratch, stats, err
	}
	scratch = nextScratch
	stats.BytesRead = bytesRead
	stats.BytesScanned = int64(len(metadata))
	stats.RowsSkipped = col.Count
	count, err := countInt64DictionaryEqualMetadata(metadata, col.Count, value)
	if err != nil {
		return 0, scratch, stats, fmt.Errorf("count column %q: %w", col.Name, err)
	}
	return count, scratch, stats, nil
}

func countInt64EqualSequence(reader Reader, col Column, value int64, scratch []byte, stats ScanStats) (int, []byte, ScanStats, error) {
	payload, nextScratch, stable, err := reader.readPayload(col, scratch)
	if err != nil {
		return 0, scratch, stats, err
	}
	stats.BytesScanned = col.Payload.Bytes
	stats.RowsSkipped = col.Count
	if !stable {
		stats.BytesRead = col.Payload.Bytes
	}
	count, err := countInt64SequenceEqualPayload(payload, col.Count, value)
	if err != nil {
		return 0, nextScratch, stats, fmt.Errorf("count column %q: %w", col.Name, err)
	}
	return count, nextScratch, stats, nil
}

func countStringEqualDictionary(reader Reader, col Column, value string, scratch []byte, stats ScanStats) (int, []byte, ScanStats, error) {
	metadata, nextScratch, bytesRead, err := readStringDictionaryMetadata(reader, col, scratch)
	if err != nil {
		return 0, scratch, stats, err
	}
	scratch = nextScratch
	stats.BytesRead = bytesRead
	stats.BytesScanned = int64(len(metadata))
	stats.RowsSkipped = col.Count
	count, err := countStringDictionaryEqualMetadata(metadata, col.Count, value)
	if err != nil {
		return 0, scratch, stats, fmt.Errorf("count column %q: %w", col.Name, err)
	}
	return count, scratch, stats, nil
}

func countStringEqualPrefix(reader Reader, col Column, value string, scratch []byte, stats ScanStats) (int, []byte, ScanStats, error) {
	payload, nextScratch, stats, err := readPlainPayload(reader, col, scratch)
	if err != nil {
		return 0, scratch, stats, err
	}
	count, skipped, err := countStringPrefixEqualPayload(payload, col.Count, value)
	if err != nil {
		return 0, nextScratch, stats, fmt.Errorf("count column %q: %w", col.Name, err)
	}
	if skipped {
		stats.RowsScanned = 0
		stats.RowsSkipped = col.Count
	}
	return count, nextScratch, stats, nil
}

func countStringEqualTemplate(reader Reader, col Column, value string, scratch []byte, stats ScanStats) (int, []byte, ScanStats, error) {
	payload, nextScratch, stats, err := readPlainPayload(reader, col, scratch)
	if err != nil {
		return 0, scratch, stats, err
	}
	count, skipped, err := countStringTemplateEqualPayload(payload, col.Count, value)
	if err != nil {
		return 0, nextScratch, stats, fmt.Errorf("count column %q: %w", col.Name, err)
	}
	if skipped {
		stats.RowsScanned = 0
		stats.RowsSkipped = col.Count
	}
	return count, nextScratch, stats, nil
}

func countStringEqualPrefixPageBloom(reader Reader, col Column, value string, scratch []byte, stats ScanStats) (int, []byte, ScanStats, error) {
	prefix, suffix, scratch, bytesRead, err := readStringPrefixMetadata(reader, col, scratch)
	if err != nil {
		return 0, scratch, stats, err
	}
	stats.BytesRead = bytesRead
	stats.BytesScanned = int64(stringPrefixHeaderLen + len(prefix) + len(suffix))
	sharedLen := len(prefix) + len(suffix)
	if len(value) < sharedLen || !util.BytesEqualString(prefix, value[:len(prefix)]) || !util.BytesEqualString(suffix, value[len(value)-len(suffix):]) {
		stats.RowsSkipped = col.Count
		return 0, scratch, stats, nil
	}
	middleValue := value[len(prefix) : len(value)-len(suffix)]
	count, nextScratch, pageStats, err := countStringEqualPageBloomValues(reader, col, value, middleValue, scratch, stats)
	return count, nextScratch, pageStats, err
}

func readStringPrefixMetadata(reader Reader, col Column, scratch []byte) (prefix []byte, suffix []byte, outScratch []byte, bytesRead int64, err error) {
	if col.Payload.Bytes < stringPrefixHeaderLen {
		return nil, nil, scratch, 0, fmt.Errorf("short string prefix payload")
	}
	header, scratch, stable, bytesRead, err := readPayloadPrefix(reader, col, stringPrefixHeaderLen, scratch)
	if err != nil {
		return nil, nil, scratch, bytesRead, err
	}
	prefixLen, err := checkedInt("string prefix length", uint64(binary.LittleEndian.Uint32(header)))
	if err != nil {
		return nil, nil, scratch, bytesRead, err
	}
	suffixLen, err := checkedInt("string suffix length", uint64(binary.LittleEndian.Uint32(header[4:])))
	if err != nil {
		return nil, nil, scratch, bytesRead, err
	}
	sharedLen, err := checkedAddInt("string prefix metadata length", prefixLen, suffixLen)
	if err != nil {
		return nil, nil, scratch, bytesRead, err
	}
	metadataLen, err := checkedAddInt("string prefix metadata length", stringPrefixHeaderLen, sharedLen)
	if err != nil {
		return nil, nil, scratch, bytesRead, err
	}
	if int64(metadataLen) > col.Payload.Bytes {
		return nil, nil, scratch, bytesRead, fmt.Errorf("string prefix metadata length %d exceeds payload length %d", metadataLen, col.Payload.Bytes)
	}
	if metadataLen == len(header) {
		return header[stringPrefixHeaderLen:stringPrefixHeaderLen], header[stringPrefixHeaderLen:stringPrefixHeaderLen], scratch, bytesRead, nil
	}
	var metadata []byte
	if stable {
		var metadataBytesRead int64
		metadata, scratch, _, metadataBytesRead, err = readPayloadPrefix(reader, col, metadataLen, scratch)
		if err != nil {
			return nil, nil, scratch, bytesRead + metadataBytesRead, err
		}
		bytesRead += metadataBytesRead
	} else {
		if cap(scratch) < metadataLen {
			nextScratch := make([]byte, metadataLen)
			copy(nextScratch, header)
			scratch = nextScratch
		} else {
			scratch = scratch[:metadataLen]
			copy(scratch, header)
		}
		if err := readAtFull(reader.r, scratch[len(header):metadataLen], reader.offset+col.Payload.Offset+int64(len(header))); err != nil {
			return nil, nil, scratch, bytesRead, err
		}
		bytesRead += int64(metadataLen - len(header))
		metadata = scratch[:metadataLen]
	}
	prefixStart := stringPrefixHeaderLen
	suffixStart := prefixStart + prefixLen
	return metadata[prefixStart:suffixStart], metadata[suffixStart:metadataLen], scratch, bytesRead, nil
}

func groupStringCountsDictionary(reader Reader, col Column, counts map[string]int, scratch []byte, keyScratch []string, stats ScanStats) (map[string]int, []byte, []string, ScanStats, error) {
	metadata, nextScratch, bytesRead, err := readStringDictionaryMetadata(reader, col, scratch)
	if err != nil {
		return counts, scratch, keyScratch, stats, err
	}
	scratch = nextScratch
	stats.BytesRead = bytesRead
	stats.BytesScanned = int64(len(metadata))
	stats.RowsSkipped = col.Count
	keyScratch, err = groupStringDictionaryCountsMetadata(metadata, col.Count, counts, keyScratch)
	if err != nil {
		return counts, scratch, keyScratch, stats, fmt.Errorf("group column %q: %w", col.Name, err)
	}
	return counts, scratch, keyScratch, stats, nil
}

func readInt64DictionaryMetadata(reader Reader, col Column, scratch []byte) ([]byte, []byte, int64, error) {
	if col.Dictionary.Bytes > 0 {
		return readDictionaryMetadataRange(reader, col, scratch)
	}
	if col.Payload.Bytes < int64DictionaryHeaderLen {
		return nil, scratch, 0, fmt.Errorf("short int64 dictionary payload")
	}
	header, scratch, stable, bytesRead, err := readPayloadPrefix(reader, col, int64DictionaryHeaderLen, scratch)
	if err != nil {
		return nil, scratch, bytesRead, err
	}
	dictCount, err := checkedInt("int64 dictionary count", uint64(binary.LittleEndian.Uint32(header)))
	if err != nil {
		return nil, scratch, bytesRead, err
	}
	metadataLen, err := int64DictionaryMetadataLen(dictCount)
	if err != nil {
		return nil, scratch, bytesRead, err
	}
	return readDictionaryMetadataAfterHeader(reader, col, header, stable, metadataLen, bytesRead, scratch)
}

func readStringDictionaryMetadata(reader Reader, col Column, scratch []byte) ([]byte, []byte, int64, error) {
	if col.Dictionary.Bytes > 0 {
		return readDictionaryMetadataRange(reader, col, scratch)
	}
	if col.Payload.Bytes < stringDictionaryHeaderLen {
		return nil, scratch, 0, fmt.Errorf("short string dictionary payload")
	}
	header, scratch, stable, bytesRead, err := readPayloadPrefix(reader, col, stringDictionaryHeaderLen, scratch)
	if err != nil {
		return nil, scratch, bytesRead, err
	}
	dictCount, err := checkedInt("string dictionary count", uint64(binary.LittleEndian.Uint32(header)))
	if err != nil {
		return nil, scratch, bytesRead, err
	}
	dictSectionLen, err := checkedInt("string dictionary section length", binary.LittleEndian.Uint64(header[5:]))
	if err != nil {
		return nil, scratch, bytesRead, err
	}
	metadataLen, err := stringDictionaryMetadataLen(dictSectionLen, dictCount)
	if err != nil {
		return nil, scratch, bytesRead, err
	}
	return readDictionaryMetadataAfterHeader(reader, col, header, stable, metadataLen, bytesRead, scratch)
}

func readDictionaryMetadataRange(reader Reader, col Column, scratch []byte) ([]byte, []byte, int64, error) {
	metadataBytes, err := checkedInt("dictionary metadata length", uint64(col.Dictionary.Bytes))
	if err != nil {
		return nil, scratch, 0, err
	}
	metadata, nextScratch, stable, err := readRange(reader.r, reader.view, reader.offset+col.Dictionary.Offset, metadataBytes, scratch)
	if err != nil {
		return nil, scratch, 0, err
	}
	if stable {
		return metadata, nextScratch, 0, nil
	}
	return metadata, nextScratch, col.Dictionary.Bytes, nil
}

func readDictionaryMetadataAfterHeader(reader Reader, col Column, header []byte, stable bool, metadataLen int, bytesRead int64, scratch []byte) ([]byte, []byte, int64, error) {
	if int64(metadataLen) > col.Payload.Bytes {
		return nil, scratch, bytesRead, fmt.Errorf("dictionary metadata length %d exceeds payload length %d", metadataLen, col.Payload.Bytes)
	}
	if metadataLen == len(header) {
		return header, scratch, bytesRead, nil
	}
	if stable {
		metadata, nextScratch, _, metadataBytesRead, err := readPayloadPrefix(reader, col, metadataLen, scratch)
		return metadata, nextScratch, bytesRead + metadataBytesRead, err
	}
	if cap(scratch) < metadataLen {
		nextScratch := make([]byte, metadataLen)
		copy(nextScratch, header)
		scratch = nextScratch
	} else {
		scratch = scratch[:metadataLen]
		copy(scratch, header)
	}
	if err := readAtFull(reader.r, scratch[len(header):metadataLen], reader.offset+col.Payload.Offset+int64(len(header))); err != nil {
		return nil, scratch, bytesRead, err
	}
	bytesRead += int64(metadataLen - len(header))
	return scratch[:metadataLen], scratch, bytesRead, nil
}

func readPayloadPrefix(reader Reader, col Column, n int, scratch []byte) ([]byte, []byte, bool, int64, error) {
	if n < 0 {
		return nil, scratch, false, 0, fmt.Errorf("negative payload prefix length %d", n)
	}
	if int64(n) > col.Payload.Bytes {
		return nil, scratch, false, 0, fmt.Errorf("payload prefix length %d exceeds payload length %d", n, col.Payload.Bytes)
	}
	data, nextScratch, stable, err := readRange(reader.r, reader.view, reader.offset+col.Payload.Offset, n, scratch)
	if err != nil {
		return nil, scratch, stable, 0, err
	}
	if stable {
		return data, nextScratch, stable, 0, nil
	}
	return data, nextScratch, stable, int64(n), nil
}

func countStringEqualPageBloom(reader Reader, col Column, value string, scratch []byte, stats ScanStats) (int, []byte, ScanStats, error) {
	return countStringEqualPageBloomValues(reader, col, value, value, scratch, stats)
}

func countStringEqualPageBloomValues(reader Reader, col Column, bloomValue string, exactValue string, scratch []byte, stats ScanStats) (int, []byte, ScanStats, error) {
	pageDir, filters, scratch, metadataBytesRead, scratchPrefix, err := readPageBloomMetadata(reader, col, scratch)
	if err != nil {
		return 0, scratch, stats, err
	}
	stats.BytesRead += metadataBytesRead
	stats.BytesScanned += int64(len(pageDir) + len(filters))

	pageCount, err := pageDirectoryPageCount(pageDir)
	if err != nil {
		return 0, scratch, stats, err
	}
	filterPageCount, hashes, bitsPerRow, err := pageBloomHeader(filters)
	if err != nil {
		return 0, scratch, stats, err
	}
	if filterPageCount != pageCount {
		return 0, scratch, stats, fmt.Errorf("page bloom count %d does not match page directory count %d", filterPageCount, pageCount)
	}

	maxPageScratch := 0
	filterOff := pageBloomHeaderLen
	nextRow := 0
	for pageID := 0; pageID < pageCount; pageID++ {
		page, err := pageFromDirectoryAt(pageDir, pageID)
		if err != nil {
			return 0, scratch, stats, err
		}
		if err := validatePage(page, pageID, nextRow, col); err != nil {
			return 0, scratch, stats, err
		}
		nextRow += page.Count
		pageScratch, err := pageScratchLen(page)
		if err != nil {
			return 0, scratch, stats, err
		}
		maxPageScratch = max(maxPageScratch, pageScratch)
		byteLen, err := pageBloomByteLen(page.Count, bitsPerRow)
		if err != nil {
			return 0, scratch, stats, err
		}
		filterOff, err = checkedAddInt("page bloom offset", filterOff, byteLen)
		if err != nil {
			return 0, scratch, stats, err
		}
		if filterOff > len(filters) {
			return 0, scratch, stats, fmt.Errorf("short page bloom filter at page %d", pageID)
		}
	}
	if nextRow != col.Count {
		return 0, scratch, stats, fmt.Errorf("page rows sum %d does not match column rows %d", nextRow, col.Count)
	}
	if filterOff != len(filters) {
		return 0, scratch, stats, fmt.Errorf("trailing page bloom bytes: %d", len(filters)-filterOff)
	}

	if scratchPrefix > 0 {
		if cap(scratch) < scratchPrefix+maxPageScratch {
			nextScratch := make([]byte, scratchPrefix+maxPageScratch)
			copy(nextScratch, pageDir)
			copy(nextScratch[len(pageDir):], filters)
			scratch = nextScratch
			pageDir = scratch[:len(pageDir)]
			filters = scratch[len(pageDir):scratchPrefix]
		}
		scratch = scratch[:scratchPrefix+maxPageScratch]
	}

	count := 0
	filterOff = pageBloomHeaderLen
	for pageID := 0; pageID < pageCount; pageID++ {
		page, err := pageFromDirectoryAt(pageDir, pageID)
		if err != nil {
			return 0, scratch, stats, err
		}
		byteLen, err := pageBloomByteLen(page.Count, bitsPerRow)
		if err != nil {
			return 0, scratch, stats, err
		}
		filter := pageBloomFilter{bits: byteLen * 8, hashes: hashes, data: filters[filterOff : filterOff+byteLen]}
		filterOff += byteLen
		if !filter.mayContain(bloomValue) {
			stats.RowsSkipped += page.Count
			continue
		}

		pageMatches, nextScratch, bytesRead, err := countStringEqualPage(reader, page, exactValue, scratch, scratchPrefix)
		if err != nil {
			return 0, scratch, stats, err
		}
		scratch = nextScratch
		count += pageMatches
		stats.BytesRead += bytesRead
		stats.BytesScanned += page.Payload.Bytes + page.Values.Bytes
		stats.RowsScanned += page.Count
		stats.PagesScanned++
	}
	return count, scratch, stats, nil
}

func readPageBloomMetadata(reader Reader, col Column, scratch []byte) (pageDir []byte, filters []byte, outScratch []byte, bytesRead int64, scratchPrefix int, err error) {
	pageBytes, err := checkedInt("page directory length", uint64(col.Pages.Bytes))
	if err != nil {
		return nil, nil, scratch, 0, 0, err
	}
	filterBytes, err := checkedInt("filter length", uint64(col.Filters.Bytes))
	if err != nil {
		return nil, nil, scratch, 0, 0, err
	}
	if pageBytes == 0 || filterBytes == 0 {
		return nil, nil, scratch, 0, 0, fmt.Errorf("page bloom requires page and filter metadata")
	}
	pageOffset := reader.offset + col.Pages.Offset
	filterOffset := reader.offset + col.Filters.Offset
	if reader.view != nil {
		pageView, pageOK := reader.view.View(pageOffset, pageBytes)
		filterView, filterOK := reader.view.View(filterOffset, filterBytes)
		if pageOK && filterOK {
			return pageView, filterView, scratch, 0, 0, nil
		}
	}
	totalBytes, err := checkedAddInt("page bloom metadata length", pageBytes, filterBytes)
	if err != nil {
		return nil, nil, scratch, 0, 0, err
	}
	if cap(scratch) < totalBytes {
		scratch = make([]byte, totalBytes)
	}
	scratch = scratch[:totalBytes]
	if col.Pages.Offset+col.Pages.Bytes == col.Filters.Offset {
		if err := readAtFull(reader.r, scratch, pageOffset); err != nil {
			return nil, nil, scratch, 0, 0, err
		}
	} else {
		if err := readAtFull(reader.r, scratch[:pageBytes], pageOffset); err != nil {
			return nil, nil, scratch, 0, 0, err
		}
		if err := readAtFull(reader.r, scratch[pageBytes:], filterOffset); err != nil {
			return nil, nil, scratch, 0, 0, err
		}
	}
	return scratch[:pageBytes], scratch[pageBytes:], scratch, int64(totalBytes), totalBytes, nil
}

func pageScratchLen(page Page) (int, error) {
	offsetBytes, err := checkedInt("page string offset length", uint64(page.Payload.Bytes))
	if err != nil {
		return 0, err
	}
	valueBytes, err := checkedInt("page string value length", uint64(page.Values.Bytes))
	if err != nil {
		return 0, err
	}
	return checkedAddInt("page string scratch length", offsetBytes, valueBytes)
}

func countStringEqualPage(reader Reader, page Page, value string, scratch []byte, scratchOffset int) (int, []byte, int64, error) {
	offsetBytes, err := checkedInt("page string offset length", uint64(page.Payload.Bytes))
	if err != nil {
		return 0, scratch, 0, err
	}
	wantOffsetBytes, err := checkedMulInt("page string offset length", page.Count+1, stringOffsetWidth)
	if err != nil {
		return 0, scratch, 0, err
	}
	if offsetBytes != wantOffsetBytes {
		return 0, scratch, 0, fmt.Errorf("page string offset payload has %d bytes, want %d", offsetBytes, wantOffsetBytes)
	}
	valueBytes, err := checkedInt("page string value length", uint64(page.Values.Bytes))
	if err != nil {
		return 0, scratch, 0, err
	}
	if reader.view != nil {
		offsets, offsetsOK := reader.view.View(reader.offset+page.Payload.Offset, offsetBytes)
		values, valuesOK := reader.view.View(reader.offset+page.Values.Offset, valueBytes)
		if offsetsOK && valuesOK {
			count, err := countStringEqualPageData(offsets, values, page.Count, value)
			return count, scratch, 0, err
		}
	}
	readBytes, err := checkedAddInt("page string read length", offsetBytes, valueBytes)
	if err != nil {
		return 0, scratch, 0, err
	}
	needBytes, err := checkedAddInt("page string scratch length", scratchOffset, readBytes)
	if err != nil {
		return 0, scratch, 0, err
	}
	if cap(scratch) < needBytes {
		if scratchOffset > 0 {
			return 0, scratch, 0, fmt.Errorf("page string scratch length %d exceeds capacity %d", needBytes, cap(scratch))
		}
		scratch = make([]byte, needBytes)
	}
	scratch = scratch[:needBytes]
	offsets := scratch[scratchOffset : scratchOffset+offsetBytes]
	values := scratch[scratchOffset+offsetBytes : needBytes]
	if err := readAtFull(reader.r, offsets, reader.offset+page.Payload.Offset); err != nil {
		return 0, scratch, 0, err
	}
	if err := readAtFull(reader.r, values, reader.offset+page.Values.Offset); err != nil {
		return 0, scratch, 0, err
	}
	count, err := countStringEqualPageData(offsets, values, page.Count, value)
	return count, scratch, int64(readBytes), err
}

func countStringEqualPageData(offsets []byte, values []byte, rows int, value string) (int, error) {
	expected, err := checkedMulInt("page string offsets length", rows+1, stringOffsetWidth)
	if err != nil {
		return 0, err
	}
	if len(offsets) != expected {
		return 0, fmt.Errorf("page string offsets have %d bytes, want %d", len(offsets), expected)
	}
	base := uint64(binary.LittleEndian.Uint32(offsets))
	valueLen := uint64(len(values))
	last := uint64(binary.LittleEndian.Uint32(offsets[rows*stringOffsetWidth:]))
	if last < base || last-base != valueLen {
		return 0, fmt.Errorf("last page string offset is %d, want %d", last, base+valueLen)
	}
	targetLen := uint64(len(value))
	count := 0
	prev := base
	for row := 0; row < rows; row++ {
		next := uint64(binary.LittleEndian.Uint32(offsets[(row+1)*stringOffsetWidth:]))
		if next < prev || next-base > valueLen {
			return 0, fmt.Errorf("page string offsets at row %d are out of bounds", row)
		}
		start := prev - base
		end := next - base
		if next-prev == targetLen && util.BytesEqualString(values[start:end], value) {
			count++
		}
		prev = next
	}
	return count, nil
}

func countInt64EqualPlain(payload []byte, rows int, value int64) (int, error) {
	expected, err := checkedMulInt("int64 payload length", rows, int64Size)
	if err != nil {
		return 0, err
	}
	if len(payload) != expected {
		return 0, fmt.Errorf("plain int64 payload has %d bytes, want %d", len(payload), expected)
	}
	target := uint64(value)
	count := 0
	for off := 0; off < len(payload); off += int64Size {
		if binary.LittleEndian.Uint64(payload[off:off+int64Size]) == target {
			count++
		}
	}
	return count, nil
}

func countStringEqualPlain(payload []byte, rows int, value string) (int, error) {
	offsetBytes, err := checkedMulInt("string offset payload length", rows+1, stringOffsetWidth)
	if err != nil {
		return 0, err
	}
	if len(payload) < offsetBytes {
		return 0, fmt.Errorf("plain string payload has %d bytes, want at least %d", len(payload), offsetBytes)
	}
	if first := binary.LittleEndian.Uint32(payload); first != 0 {
		return 0, fmt.Errorf("first string offset is %d, want 0", first)
	}
	data := payload[offsetBytes:]
	dataLen := uint64(len(data))
	if dataLen > uint64(^uint32(0)) {
		return 0, fmt.Errorf("plain string data length %d overflows uint32 offsets", len(data))
	}
	last := uint64(binary.LittleEndian.Uint32(payload[rows*stringOffsetWidth:]))
	if last != dataLen {
		return 0, fmt.Errorf("last string offset is %d, want %d", last, dataLen)
	}

	targetLen := uint64(len(value))
	count := 0
	prev := uint64(0)
	for row := 0; row < rows; row++ {
		next := uint64(binary.LittleEndian.Uint32(payload[(row+1)*stringOffsetWidth:]))
		if next < prev || next > dataLen {
			return 0, fmt.Errorf("string offsets at row %d are out of bounds", row)
		}
		if next-prev == targetLen && util.BytesEqualString(data[prev:next], value) {
			count++
		}
		prev = next
	}
	return count, nil
}
