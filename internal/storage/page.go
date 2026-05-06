package storage

import (
	"encoding/binary"
	"fmt"
)

const (
	pageDirectoryHeaderLen = 4
	pageDirectoryEntryLen  = 65

	pageStartRowOff = 0
	pageRowCountOff = 8
	pagePayloadOff  = 16
	pageValuesOff   = 32
	pageMinMaxOff   = 48
	pageMinInt64Off = 49
	pageMaxInt64Off = 57
)

func encodePageDirectory(pages []Page) ([]byte, error) {
	if len(pages) == 0 {
		return nil, nil
	}

	size, err := pageDirectoryLen(len(pages))
	if err != nil {
		return nil, err
	}

	out := make([]byte, size)
	binary.LittleEndian.PutUint32(out[:4], uint32(len(pages)))

	off := pageDirectoryHeaderLen
	for i := range pages {
		page := pages[i]

		if page.StartRow < 0 || page.Count <= 0 {
			return nil, fmt.Errorf("invalid page row range start=%d count=%d", page.StartRow, page.Count)
		}

		writePageDirectoryEntry(out[off:off+pageDirectoryEntryLen], page)
		off += pageDirectoryEntryLen
	}

	return out, nil
}

func decodePageDirectory(buf []byte, col Column) ([]Page, error) {
	if len(buf) == 0 {
		return nil, nil
	}

	pageCount, err := pageDirectoryPageCount(buf)
	if err != nil {
		return nil, err
	}

	pages := make([]Page, pageCount)

	nextRow := 0
	for i := range pages {
		page, err := pageFromDirectoryAtUnchecked(buf, i)
		if err != nil {
			return nil, err
		}

		if err := validatePage(page, i, nextRow, col); err != nil {
			return nil, err
		}

		nextRow += page.Count
		pages[i] = page
	}

	if nextRow != col.Count {
		return nil, fmt.Errorf("page rows sum %d does not match column rows %d", nextRow, col.Count)
	}

	return pages, nil
}

func pageDirectoryPageCount(buf []byte) (int, error) {
	if len(buf) < pageDirectoryHeaderLen {
		return 0, fmt.Errorf("short page directory")
	}

	pageCount, err := checkedInt("page count", uint64(binary.LittleEndian.Uint32(buf[:4])))
	if err != nil {
		return 0, err
	}

	wantLen, err := pageDirectoryLen(pageCount)
	if err != nil {
		return 0, err
	}

	if len(buf) != wantLen {
		return 0, fmt.Errorf("page directory has %d bytes, want %d", len(buf), wantLen)
	}

	return pageCount, nil
}

func pageFromDirectoryAt(buf []byte, pageID int) (Page, error) {
	if pageID < 0 {
		return Page{}, fmt.Errorf("negative page id %d", pageID)
	}

	pageCount, err := pageDirectoryPageCount(buf)
	if err != nil {
		return Page{}, err
	}

	if pageID >= pageCount {
		return Page{}, fmt.Errorf("page id %d out of range, page count %d", pageID, pageCount)
	}

	return pageFromDirectoryAtUnchecked(buf, pageID)
}

func pageFromDirectoryAtUnchecked(buf []byte, pageID int) (Page, error) {
	off, err := pageDirectoryEntryOffset(pageID)
	if err != nil {
		return Page{}, err
	}

	if off > len(buf) || len(buf)-off < pageDirectoryEntryLen {
		return Page{}, fmt.Errorf("short page directory entry %d", pageID)
	}

	return decodePageDirectoryEntry(buf[off:off+pageDirectoryEntryLen], pageID)
}

func pageDirectoryLen(pageCount int) (int, error) {
	if pageCount < 0 {
		return 0, fmt.Errorf("negative page count %d", pageCount)
	}

	if uint64(pageCount) > uint64(^uint32(0)) {
		return 0, fmt.Errorf("page count %d overflows uint32", pageCount)
	}

	entriesLen, err := checkedMulInt("page directory entries length", pageCount, pageDirectoryEntryLen)
	if err != nil {
		return 0, err
	}

	return checkedAddInt("page directory length", pageDirectoryHeaderLen, entriesLen)
}

func pageDirectoryEntryOffset(pageID int) (int, error) {
	entryOffset, err := checkedMulInt("page directory entry offset", pageID, pageDirectoryEntryLen)
	if err != nil {
		return 0, err
	}

	return checkedAddInt("page directory entry offset", pageDirectoryHeaderLen, entryOffset)
}

func writePageDirectoryEntry(buf []byte, page Page) {
	binary.LittleEndian.PutUint64(buf[pageStartRowOff:], uint64(page.StartRow))
	binary.LittleEndian.PutUint64(buf[pageRowCountOff:], uint64(page.Count))

	writeRangeToPage(buf[pagePayloadOff:], page.Payload)
	writeRangeToPage(buf[pageValuesOff:], page.Values)

	if page.HasMinMax {
		buf[pageMinMaxOff] = 1
	} else {
		buf[pageMinMaxOff] = 0
	}

	binary.LittleEndian.PutUint64(buf[pageMinInt64Off:], uint64(page.MinInt64))
	binary.LittleEndian.PutUint64(buf[pageMaxInt64Off:], uint64(page.MaxInt64))
}

func decodePageDirectoryEntry(buf []byte, pageID int) (Page, error) {
	if len(buf) < pageDirectoryEntryLen {
		return Page{}, fmt.Errorf("short page directory entry %d", pageID)
	}

	startRow, err := checkedInt("page start row", binary.LittleEndian.Uint64(buf[pageStartRowOff:]))
	if err != nil {
		return Page{}, err
	}

	count, err := checkedInt("page row count", binary.LittleEndian.Uint64(buf[pageRowCountOff:]))
	if err != nil {
		return Page{}, err
	}

	payload, err := decodeRangeFromPage(buf[pagePayloadOff:])
	if err != nil {
		return Page{}, fmt.Errorf("page %d payload: %w", pageID, err)
	}

	values, err := decodeRangeFromPage(buf[pageValuesOff:])
	if err != nil {
		return Page{}, fmt.Errorf("page %d values: %w", pageID, err)
	}

	hasMinMax := buf[pageMinMaxOff]
	if hasMinMax != 0 && hasMinMax != 1 {
		return Page{}, fmt.Errorf("page %d has invalid minmax flag %d", pageID, hasMinMax)
	}

	minInt64 := int64(binary.LittleEndian.Uint64(buf[pageMinInt64Off:]))
	maxInt64 := int64(binary.LittleEndian.Uint64(buf[pageMaxInt64Off:]))

	page := Page{
		StartRow:  startRow,
		Count:     count,
		Payload:   payload,
		Values:    values,
		HasMinMax: hasMinMax == 1,
		MinInt64:  minInt64,
		MaxInt64:  maxInt64,
	}

	if page.Count <= 0 {
		return Page{}, fmt.Errorf("page %d has invalid row count %d", pageID, page.Count)
	}

	if page.HasMinMax && page.MinInt64 > page.MaxInt64 {
		return Page{}, fmt.Errorf("page %d min %d is greater than max %d", pageID, page.MinInt64, page.MaxInt64)
	}

	return page, nil
}

func writeRangeToPage(buf []byte, r Range) {
	binary.LittleEndian.PutUint64(buf[:8], uint64(r.Offset))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(r.Bytes))
}

func decodeRangeFromPage(buf []byte) (Range, error) {
	if len(buf) < 16 {
		return Range{}, fmt.Errorf("short range")
	}

	offset, err := checkedInt64("range offset", binary.LittleEndian.Uint64(buf[:8]))
	if err != nil {
		return Range{}, err
	}

	bytes, err := checkedInt64("range length", binary.LittleEndian.Uint64(buf[8:16]))
	if err != nil {
		return Range{}, err
	}

	return Range{Offset: offset, Bytes: bytes}, nil
}

func validatePage(page Page, pageID int, nextRow int, col Column) error {
	if page.Count <= 0 {
		return fmt.Errorf("page %d has invalid row count %d", pageID, page.Count)
	}

	if page.StartRow != nextRow {
		return fmt.Errorf("page %d starts at row %d, want %d", pageID, page.StartRow, nextRow)
	}

	if page.HasMinMax && page.MinInt64 > page.MaxInt64 {
		return fmt.Errorf("page %d min %d is greater than max %d", pageID, page.MinInt64, page.MaxInt64)
	}

	if err := validateColumnSubrange("page payload", page.Payload, col.Payload); err != nil {
		return fmt.Errorf("page %d: %w", pageID, err)
	}

	if err := validateOptionalColumnSubrange("page values", page.Values, col.Payload); err != nil {
		return fmt.Errorf("page %d: %w", pageID, err)
	}

	return nil
}

func validateColumnSubrange(label string, r Range, parent Range) error {
	if r.Bytes < 0 {
		return fmt.Errorf("%s length %d is negative", label, r.Bytes)
	}

	if _, err := checkedInt("range length", uint64(r.Bytes)); err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}

	parentEnd, err := checkedAddInt64("range parent end", parent.Offset, parent.Bytes)
	if err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}

	end, err := checkedAddInt64("range end", r.Offset, r.Bytes)
	if err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}

	if r.Offset < parent.Offset || end > parentEnd {
		return fmt.Errorf(
			"%s range [%d,%d) is outside column payload [%d,%d)",
			label,
			r.Offset,
			end,
			parent.Offset,
			parentEnd,
		)
	}

	return nil
}

func validateOptionalColumnSubrange(label string, r Range, parent Range) error {
	if r.Offset == 0 && r.Bytes == 0 {
		return nil
	}

	return validateColumnSubrange(label, r, parent)
}
