package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

const footerColumnFixedLen = 93

func encodeFooter(columns []Column) ([]byte, error) {
	if uint64(len(columns)) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("column count %d overflows uint32", len(columns))
	}

	size := uint64(4)
	for _, col := range columns {
		if len(col.Name) > maxColumnName {
			return nil, fmt.Errorf("string %q is too long", col.Name)
		}
		size += footerColumnFixedLen + uint64(len(col.Name))
	}
	footerSize, err := checkedInt("footer length", size)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, footerSize)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(columns)))
	for _, col := range columns {
		out, err = appendString16(out, col.Name)
		if err != nil {
			return nil, err
		}
		out = append(out, byte(col.Kind), byte(col.Codec))
		out = binary.LittleEndian.AppendUint64(out, uint64(col.Count))
		out = appendRangeUnchecked(out, col.Payload)
		out = appendRangeUnchecked(out, col.Pages)
		out = appendRangeUnchecked(out, col.Filters)
		out = appendRangeUnchecked(out, col.Dictionary)
		if col.HasMinMax {
			out = append(out, 1)
		} else {
			out = append(out, 0)
		}
		out = binary.LittleEndian.AppendUint64(out, uint64(col.MinInt64))
		out = binary.LittleEndian.AppendUint64(out, uint64(col.MaxInt64))
	}
	return out, nil
}

// decodeFooter assumes the caller has already validated footerStart against segment bounds.
// Column payload/index ranges are still checked against footerStart, the start of the footer body.
func decodeFooter(buf []byte, expectedColumns int, rows int, footerStart int64) (Directory, error) {
	d := footerDecoder{buf: buf}
	columnCount, err := d.uint32()
	if err != nil {
		return Directory{}, err
	}
	cols, err := checkedInt("footer column count", uint64(columnCount))
	if err != nil {
		return Directory{}, err
	}
	if cols != expectedColumns {
		return Directory{}, fmt.Errorf("footer column count %d does not match header count %d", cols, expectedColumns)
	}

	seen := make(map[string]struct{}, cols)
	dir := Directory{Rows: rows, Columns: make([]Column, 0, cols)}
	for i := 0; i < cols; i++ {
		name, err := d.string16()
		if err != nil {
			return Directory{}, fmt.Errorf("column %d name: %w", i, err)
		}
		if name == "" {
			return Directory{}, fmt.Errorf("column %d has empty name", i)
		}
		if _, ok := seen[name]; ok {
			return Directory{}, fmt.Errorf("duplicate column %q", name)
		}
		seen[name] = struct{}{}

		kindByte, err := d.byte()
		if err != nil {
			return Directory{}, fmt.Errorf("column %q kind: %w", name, err)
		}
		codecByte, err := d.byte()
		if err != nil {
			return Directory{}, fmt.Errorf("column %q codec: %w", name, err)
		}
		count64, err := d.uint64()
		if err != nil {
			return Directory{}, fmt.Errorf("column %q count: %w", name, err)
		}
		count, err := checkedInt("column row count", count64)
		if err != nil {
			return Directory{}, fmt.Errorf("column %q: %w", name, err)
		}
		if count != rows {
			return Directory{}, fmt.Errorf("column %q row count %d does not match segment rows %d", name, count, rows)
		}

		payload, err := d.rangeValue()
		if err != nil {
			return Directory{}, fmt.Errorf("column %q payload: %w", name, err)
		}
		pages, err := d.rangeValue()
		if err != nil {
			return Directory{}, fmt.Errorf("column %q pages: %w", name, err)
		}
		filters, err := d.rangeValue()
		if err != nil {
			return Directory{}, fmt.Errorf("column %q filters: %w", name, err)
		}
		dictionary, err := d.rangeValue()
		if err != nil {
			return Directory{}, fmt.Errorf("column %q dictionary: %w", name, err)
		}
		hasMinMaxByte, err := d.byte()
		if err != nil {
			return Directory{}, fmt.Errorf("column %q minmax flag: %w", name, err)
		}
		minInt64, err := d.int64()
		if err != nil {
			return Directory{}, fmt.Errorf("column %q min: %w", name, err)
		}
		maxInt64, err := d.int64()
		if err != nil {
			return Directory{}, fmt.Errorf("column %q max: %w", name, err)
		}

		kind := vector.Kind(kindByte)
		codec := Codec(codecByte)
		if kind != vector.KindInt64 && kind != vector.KindString {
			return Directory{}, fmt.Errorf("column %q has unsupported kind %s", name, kind)
		}
		if codec != CodecPlain && codec != CodecDictionary && codec != CodecInt64Sequence && codec != CodecStringPrefix && codec != CodecStringTemplate {
			return Directory{}, fmt.Errorf("column %q has unsupported codec %s", name, codec)
		}
		if codec == CodecInt64Sequence && kind != vector.KindInt64 {
			return Directory{}, fmt.Errorf("column %q has %s codec for %s values", name, codec, kind)
		}
		if codec == CodecStringPrefix && kind != vector.KindString {
			return Directory{}, fmt.Errorf("column %q has %s codec for %s values", name, codec, kind)
		}
		if codec == CodecStringTemplate && kind != vector.KindString {
			return Directory{}, fmt.Errorf("column %q has %s codec for %s values", name, codec, kind)
		}
		if hasMinMaxByte != 0 && hasMinMaxByte != 1 {
			return Directory{}, fmt.Errorf("column %q has invalid minmax flag %d", name, hasMinMaxByte)
		}
		if err := validateRange("payload", payload, footerStart); err != nil {
			return Directory{}, fmt.Errorf("column %q: %w", name, err)
		}
		if err := validateOptionalRange("pages", pages, footerStart); err != nil {
			return Directory{}, fmt.Errorf("column %q: %w", name, err)
		}
		if err := validateOptionalRange("filters", filters, footerStart); err != nil {
			return Directory{}, fmt.Errorf("column %q: %w", name, err)
		}
		if err := validateOptionalRange("dictionary", dictionary, footerStart); err != nil {
			return Directory{}, fmt.Errorf("column %q: %w", name, err)
		}
		if codec == CodecDictionary {
			if dictionary.Offset != payload.Offset {
				return Directory{}, fmt.Errorf("column %q dictionary offset %d does not match payload offset %d", name, dictionary.Offset, payload.Offset)
			}
			if dictionary.Bytes <= 0 || dictionary.Bytes > payload.Bytes {
				return Directory{}, fmt.Errorf("column %q dictionary metadata length %d is outside payload length %d", name, dictionary.Bytes, payload.Bytes)
			}
		} else if dictionary.Offset != 0 || dictionary.Bytes != 0 {
			return Directory{}, fmt.Errorf("column %q has dictionary metadata for %s codec", name, codec)
		}
		dir.Columns = append(dir.Columns, Column{
			Name:       name,
			Kind:       kind,
			Codec:      codec,
			Count:      count,
			Payload:    payload,
			Pages:      pages,
			Filters:    filters,
			Dictionary: dictionary,
			HasMinMax:  hasMinMaxByte == 1,
			MinInt64:   minInt64,
			MaxInt64:   maxInt64,
		})
	}
	if len(d.buf) != 0 {
		return Directory{}, fmt.Errorf("footer has %d trailing bytes", len(d.buf))
	}
	return dir, nil
}

func appendRangeUnchecked(out []byte, r Range) []byte {
	out = binary.LittleEndian.AppendUint64(out, uint64(r.Offset))
	out = binary.LittleEndian.AppendUint64(out, uint64(r.Bytes))
	return out
}

func appendString16(out []byte, value string) ([]byte, error) {
	if len(value) > maxColumnName {
		return nil, fmt.Errorf("string %q is too long", value)
	}
	out = binary.LittleEndian.AppendUint16(out, uint16(len(value)))
	out = append(out, value...)
	return out, nil
}

func validateRange(label string, r Range, limit int64) error {
	if r.Offset < headerLen64 {
		return fmt.Errorf("%s offset %d is before payload area", label, r.Offset)
	}
	end, err := checkedAddInt64(label, r.Offset, r.Bytes)
	if err != nil {
		return err
	}
	if _, err := checkedInt(label+" length", uint64(r.Bytes)); err != nil {
		return err
	}
	if end > limit {
		return fmt.Errorf("%s range [%d,%d) exceeds data area %d", label, r.Offset, end, limit)
	}
	return nil
}

func validateOptionalRange(label string, r Range, limit int64) error {
	if r.Offset == 0 && r.Bytes == 0 {
		return nil
	}
	return validateRange(label, r, limit)
}

type footerDecoder struct {
	buf []byte
}

func (d *footerDecoder) consume(n int) ([]byte, error) {
	if len(d.buf) < n {
		return nil, fmt.Errorf("unexpected footer EOF")
	}
	out := d.buf[:n]
	d.buf = d.buf[n:]
	return out, nil
}

func (d *footerDecoder) byte() (byte, error) {
	buf, err := d.consume(1)
	if err != nil {
		return 0, err
	}
	value := buf[0]
	return value, nil
}

func (d *footerDecoder) uint16() (uint16, error) {
	buf, err := d.consume(2)
	if err != nil {
		return 0, err
	}
	value := binary.LittleEndian.Uint16(buf)
	return value, nil
}

func (d *footerDecoder) uint32() (uint32, error) {
	buf, err := d.consume(4)
	if err != nil {
		return 0, err
	}
	value := binary.LittleEndian.Uint32(buf)
	return value, nil
}

func (d *footerDecoder) uint64() (uint64, error) {
	buf, err := d.consume(8)
	if err != nil {
		return 0, err
	}
	value := binary.LittleEndian.Uint64(buf)
	return value, nil
}

func (d *footerDecoder) int64() (int64, error) {
	value, err := d.uint64()
	return int64(value), err
}

func (d *footerDecoder) string16() (string, error) {
	length, err := d.uint16()
	if err != nil {
		return "", err
	}
	buf, err := d.consume(int(length))
	if err != nil {
		return "", err
	}
	value := string(buf)
	return value, nil
}

func (d *footerDecoder) rangeValue() (Range, error) {
	off, err := d.uint64()
	if err != nil {
		return Range{}, err
	}
	n, err := d.uint64()
	if err != nil {
		return Range{}, err
	}
	offset, err := checkedInt64("range offset", off)
	if err != nil {
		return Range{}, err
	}
	bytes, err := checkedInt64("range length", n)
	if err != nil {
		return Range{}, err
	}
	return Range{Offset: offset, Bytes: bytes}, nil
}

func checkedInt64(label string, n uint64) (int64, error) {
	if n > uint64(maxSegmentSize) {
		return 0, fmt.Errorf("%s %d overflows int64", label, n)
	}
	return int64(n), nil
}
