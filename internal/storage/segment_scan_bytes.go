package storage

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// SelectSegmentInt64EqualBytes appends matching row indexes from one encoded segment byte slice.
func SelectSegmentInt64EqualBytes(data []byte, column string, value int64, scratch []uint32) ([]uint32, bool, error) {
	stats, payload, found, skip, err := findInt64EqualPayloadBytes(data, column, value)
	if err != nil {
		return scratch, found, err
	}
	if !found {
		return scratch, false, nil
	}
	if skip {
		return scratch[:0], true, nil
	}

	selected, err := selectInt64EqualPayload(payload, stats, value, scratch)
	if err != nil {
		return scratch, true, err
	}
	return selected, found, nil
}

// CountSegmentInt64EqualBytes counts matching rows from one encoded segment byte slice.
func CountSegmentInt64EqualBytes(data []byte, column string, value int64) (int, bool, error) {
	stats, payload, found, skip, err := findInt64EqualPayloadBytes(data, column, value)
	if err != nil || !found || skip {
		return 0, found, err
	}

	count, err := countInt64EqualPayload(payload, stats, value)
	if err != nil {
		return 0, true, err
	}
	return count, true, nil
}

func findInt64EqualPayloadBytes(data []byte, column string, value int64) (ColumnStats, []byte, bool, bool, error) {
	offset, cols, err := readSegmentHeader(data)
	if err != nil {
		return ColumnStats{}, nil, false, false, err
	}

	found := false
	skip := false
	var foundStats ColumnStats
	var foundPayload []byte
	for range cols {
		name, stats, payload, err := readColumnBytes(data, &offset)
		if err != nil {
			return ColumnStats{}, nil, false, false, err
		}
		if !bytesEqualString(name, column) || found {
			continue
		}

		found = true
		stats.Name = column
		if stats.Kind != vector.KindInt64 {
			return ColumnStats{}, nil, true, false, fmt.Errorf("column %q is %s, want int64", column, stats.Kind)
		}
		if canSkipInt64Equal(stats, value) {
			skip = true
			continue
		}
		foundStats = stats
		foundPayload = payload
	}

	return foundStats, foundPayload, found, skip, nil
}

func readSegmentHeader(data []byte) (offset int, cols int, err error) {
	if len(data) < segmentHeaderLen {
		return 0, 0, io.ErrUnexpectedEOF
	}
	if !bytesEqualString(data[:len(segmentMagic)], segmentMagic) {
		return 0, 0, fmt.Errorf("invalid segment magic %q", string(data[:len(segmentMagic)]))
	}
	offset = len(segmentMagic)

	version := binary.LittleEndian.Uint16(data[offset:])
	offset += 2
	if version != segmentVersion {
		return 0, 0, fmt.Errorf("unsupported segment version %d", version)
	}
	rowCount := binary.LittleEndian.Uint64(data[offset:])
	offset += 8
	if _, err := checkedInt("segment row count", rowCount); err != nil {
		return 0, 0, err
	}
	columnCount := binary.LittleEndian.Uint32(data[offset:])
	offset += 4
	cols, err = checkedInt("segment column count", uint64(columnCount))
	if err != nil {
		return 0, 0, err
	}
	return offset, cols, nil
}

func readColumnBytes(data []byte, offset *int) ([]byte, ColumnStats, []byte, error) {
	name, stats, err := readColumnHeaderBytes(data, offset)
	if err != nil {
		return nil, ColumnStats{}, nil, err
	}
	payload, err := readBytes(data, offset, stats.EncodedLen)
	if err != nil {
		return nil, ColumnStats{}, nil, err
	}
	return name, stats, payload, nil
}

func readColumnHeaderBytes(data []byte, offset *int) ([]byte, ColumnStats, error) {
	name, err := readString16Bytes(data, offset)
	if err != nil {
		return nil, ColumnStats{}, err
	}
	fixed, err := readBytes(data, offset, columnFixedHeaderLen)
	if err != nil {
		return nil, ColumnStats{}, err
	}

	stats, err := decodeColumnFixedHeader("", fixed)
	if err != nil {
		return nil, ColumnStats{}, err
	}
	return name, stats, nil
}

func readString16Bytes(data []byte, offset *int) ([]byte, error) {
	lengthBytes, err := readBytes(data, offset, 2)
	if err != nil {
		return nil, err
	}
	length := binary.LittleEndian.Uint16(lengthBytes)
	return readBytes(data, offset, int(length))
}

func readBytes(data []byte, offset *int, n int) ([]byte, error) {
	if n < 0 {
		return nil, fmt.Errorf("negative read length %d", n)
	}
	if len(data)-*offset < n {
		return nil, io.ErrUnexpectedEOF
	}
	buf := data[*offset : *offset+n]
	*offset += n
	return buf, nil
}

func bytesEqualString(buf []byte, value string) bool {
	if len(buf) != len(value) {
		return false
	}
	for i, b := range buf {
		if b != value[i] {
			return false
		}
	}
	return true
}
