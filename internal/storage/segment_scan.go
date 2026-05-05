package storage

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// SelectSegmentInt64Equal appends matching row indexes for one int64 segment column into scratch.
func SelectSegmentInt64Equal(r io.Reader, column string, value int64, scratch []uint32) ([]uint32, bool, error) {
	sr := newSegmentReader(r)
	_, cols, err := sr.readHeaderChecked()
	if err != nil {
		return scratch, false, err
	}

	selected := scratch
	found := false
	for range cols {
		stats, err := sr.readColumnHeader()
		if err != nil {
			return scratch, false, err
		}
		if stats.Name != column || found {
			if err := sr.skip(stats.EncodedLen); err != nil {
				return scratch, false, err
			}
			continue
		}

		found = true
		if stats.Kind != vector.KindInt64 {
			if err := sr.skip(stats.EncodedLen); err != nil {
				return scratch, true, err
			}
			return scratch, true, fmt.Errorf("column %q is %s, want int64", column, stats.Kind)
		}
		if canSkipInt64Equal(stats, value) {
			selected = scratch[:0]
			if err := sr.skip(stats.EncodedLen); err != nil {
				return scratch, true, err
			}
			continue
		}

		selected, err = sr.selectInt64EqualPayload(stats, value, scratch)
		if err != nil {
			return scratch, true, err
		}
	}
	if err := sr.finishSegment(cols); err != nil {
		return scratch, found, err
	}

	return selected, found, nil
}

func canSkipInt64Equal(stats ColumnStats, value int64) bool {
	return stats.Kind == vector.KindInt64 &&
		stats.HasMinMax &&
		(value < stats.MinInt64 || value > stats.MaxInt64)
}

func (s *segmentReader) selectInt64EqualPayload(stats ColumnStats, value int64, scratch []uint32) ([]uint32, error) {
	if err := validateInt64SelectionPayload(stats, stats.EncodedLen); err != nil {
		return nil, err
	}

	scratch = scratch[:0]
	var buf [32 * 1024]byte
	row := 0
	remaining := stats.EncodedLen
	for remaining > 0 {
		chunk := min(remaining, len(buf))
		if err := readFull(s.r, buf[:chunk]); err != nil {
			return nil, err
		}
		for offset := 0; offset < chunk; offset += 8 {
			if int64(binary.LittleEndian.Uint64(buf[offset:])) == value {
				scratch = append(scratch, uint32(row))
			}
			row++
		}
		remaining -= chunk
	}
	return scratch, nil
}

func selectInt64EqualPayload(payload []byte, stats ColumnStats, value int64, scratch []uint32) ([]uint32, error) {
	if err := validateInt64SelectionPayload(stats, len(payload)); err != nil {
		return nil, err
	}

	scratch = scratch[:0]
	for row := range stats.Count {
		if int64(binary.LittleEndian.Uint64(payload[row*8:])) == value {
			scratch = append(scratch, uint32(row))
		}
	}
	return scratch, nil
}

func countInt64EqualPayload(payload []byte, stats ColumnStats, value int64) (int, error) {
	if err := validateInt64Payload(stats, len(payload)); err != nil {
		return 0, err
	}

	count := 0
	for row := range stats.Count {
		if int64(binary.LittleEndian.Uint64(payload[row*8:])) == value {
			count++
		}
	}
	return count, nil
}

func validateInt64SelectionPayload(stats ColumnStats, encodedLen int) error {
	if err := validateInt64Payload(stats, encodedLen); err != nil {
		return err
	}
	if uint64(stats.Count) > uint64(^uint32(0)) {
		return fmt.Errorf("column %q count %d exceeds selection vector capacity", stats.Name, stats.Count)
	}
	return nil
}
