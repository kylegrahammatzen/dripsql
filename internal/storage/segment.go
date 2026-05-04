// Package storage contains DripSQL's durable columnar storage primitives.
package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

var segmentMagic = []byte("DRIPSEG1")

const segmentVersion uint16 = 1

// ColumnMeta describes one encoded column in a segment.
type ColumnMeta struct {
	Name       string
	Kind       vector.Kind
	Count      int
	HasMinMax  bool
	MinInt64   int64
	MaxInt64   int64
	EncodedLen int
}

// SegmentMeta describes a written segment.
type SegmentMeta struct {
	Rows    int
	Columns []ColumnMeta
}

// WriteSegment writes a small immutable columnar segment.
func WriteSegment(w io.Writer, batch vector.Batch) (SegmentMeta, error) {
	if _, err := w.Write(segmentMagic); err != nil {
		return SegmentMeta{}, err
	}
	if err := binary.Write(w, binary.LittleEndian, segmentVersion); err != nil {
		return SegmentMeta{}, err
	}
	if err := binary.Write(w, binary.LittleEndian, uint64(batch.Count)); err != nil {
		return SegmentMeta{}, err
	}
	if err := binary.Write(w, binary.LittleEndian, uint32(len(batch.Columns))); err != nil {
		return SegmentMeta{}, err
	}

	meta := SegmentMeta{Rows: batch.Count, Columns: make([]ColumnMeta, 0, len(batch.Columns))}
	for _, col := range batch.Columns {
		encoded, colMeta, err := encodeColumn(col)
		if err != nil {
			return SegmentMeta{}, err
		}
		colMeta.EncodedLen = len(encoded)

		if len(col.Name) > 1<<16-1 {
			return SegmentMeta{}, fmt.Errorf("column name %q is too long", col.Name)
		}
		if err := binary.Write(w, binary.LittleEndian, uint16(len(col.Name))); err != nil {
			return SegmentMeta{}, err
		}
		if _, err := io.WriteString(w, col.Name); err != nil {
			return SegmentMeta{}, err
		}
		if err := binary.Write(w, binary.LittleEndian, uint8(col.Values.Kind())); err != nil {
			return SegmentMeta{}, err
		}
		if err := binary.Write(w, binary.LittleEndian, uint64(col.Values.Len())); err != nil {
			return SegmentMeta{}, err
		}
		if err := binary.Write(w, binary.LittleEndian, uint64(len(encoded))); err != nil {
			return SegmentMeta{}, err
		}
		if _, err := w.Write(encoded); err != nil {
			return SegmentMeta{}, err
		}

		meta.Columns = append(meta.Columns, colMeta)
	}

	return meta, nil
}

// ReadSegment reads a segment written by WriteSegment.
func ReadSegment(r io.Reader) (vector.Batch, SegmentMeta, error) {
	magic := make([]byte, len(segmentMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return vector.Batch{}, SegmentMeta{}, err
	}
	if !bytes.Equal(magic, segmentMagic) {
		return vector.Batch{}, SegmentMeta{}, fmt.Errorf("invalid segment magic %q", string(magic))
	}

	var version uint16
	if err := binary.Read(r, binary.LittleEndian, &version); err != nil {
		return vector.Batch{}, SegmentMeta{}, err
	}
	if version != segmentVersion {
		return vector.Batch{}, SegmentMeta{}, fmt.Errorf("unsupported segment version %d", version)
	}

	var rowCount uint64
	if err := binary.Read(r, binary.LittleEndian, &rowCount); err != nil {
		return vector.Batch{}, SegmentMeta{}, err
	}
	var columnCount uint32
	if err := binary.Read(r, binary.LittleEndian, &columnCount); err != nil {
		return vector.Batch{}, SegmentMeta{}, err
	}

	columns := make([]vector.Column, 0, columnCount)
	meta := SegmentMeta{Rows: int(rowCount), Columns: make([]ColumnMeta, 0, columnCount)}
	for range columnCount {
		col, colMeta, err := readColumn(r)
		if err != nil {
			return vector.Batch{}, SegmentMeta{}, err
		}
		columns = append(columns, col)
		meta.Columns = append(meta.Columns, colMeta)
	}

	batch, err := vector.NewBatch(columns...)
	if err != nil {
		return vector.Batch{}, SegmentMeta{}, err
	}
	if batch.Count != int(rowCount) {
		return vector.Batch{}, SegmentMeta{}, fmt.Errorf("segment row count %d does not match decoded batch count %d", rowCount, batch.Count)
	}

	return batch, meta, nil
}

func encodeColumn(col vector.Column) ([]byte, ColumnMeta, error) {
	meta := ColumnMeta{Name: col.Name, Kind: col.Values.Kind(), Count: col.Values.Len()}
	var buf bytes.Buffer

	switch values := col.Values.(type) {
	case vector.Int64:
		min, max, ok := values.MinMax()
		meta.HasMinMax = ok
		meta.MinInt64 = min
		meta.MaxInt64 = max
		for _, value := range values.Values {
			if err := binary.Write(&buf, binary.LittleEndian, value); err != nil {
				return nil, ColumnMeta{}, err
			}
		}
	case vector.String:
		for _, value := range values.Values {
			if len(value) > 1<<32-1 {
				return nil, ColumnMeta{}, fmt.Errorf("string value in column %q is too long", col.Name)
			}
			if err := binary.Write(&buf, binary.LittleEndian, uint32(len(value))); err != nil {
				return nil, ColumnMeta{}, err
			}
			if _, err := io.WriteString(&buf, value); err != nil {
				return nil, ColumnMeta{}, err
			}
		}
	default:
		return nil, ColumnMeta{}, fmt.Errorf("unsupported vector type %T", col.Values)
	}

	return buf.Bytes(), meta, nil
}

func readColumn(r io.Reader) (vector.Column, ColumnMeta, error) {
	var nameLen uint16
	if err := binary.Read(r, binary.LittleEndian, &nameLen); err != nil {
		return vector.Column{}, ColumnMeta{}, err
	}
	nameBytes := make([]byte, nameLen)
	if _, err := io.ReadFull(r, nameBytes); err != nil {
		return vector.Column{}, ColumnMeta{}, err
	}
	name := string(nameBytes)

	var kindByte uint8
	if err := binary.Read(r, binary.LittleEndian, &kindByte); err != nil {
		return vector.Column{}, ColumnMeta{}, err
	}
	kind := vector.Kind(kindByte)

	var count uint64
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		return vector.Column{}, ColumnMeta{}, err
	}
	var encodedLen uint64
	if err := binary.Read(r, binary.LittleEndian, &encodedLen); err != nil {
		return vector.Column{}, ColumnMeta{}, err
	}
	encoded := make([]byte, encodedLen)
	if _, err := io.ReadFull(r, encoded); err != nil {
		return vector.Column{}, ColumnMeta{}, err
	}

	meta := ColumnMeta{Name: name, Kind: kind, Count: int(count), EncodedLen: int(encodedLen)}
	values, err := decodeValues(kind, encoded, int(count), &meta)
	if err != nil {
		return vector.Column{}, ColumnMeta{}, err
	}

	return vector.Column{Name: name, Values: values}, meta, nil
}

func decodeValues(kind vector.Kind, encoded []byte, count int, meta *ColumnMeta) (vector.Values, error) {
	reader := bytes.NewReader(encoded)

	switch kind {
	case vector.KindInt64:
		values := make([]int64, count)
		for i := range values {
			if err := binary.Read(reader, binary.LittleEndian, &values[i]); err != nil {
				return nil, err
			}
		}
		v := vector.Int64{Values: values}
		min, max, ok := v.MinMax()
		meta.HasMinMax = ok
		meta.MinInt64 = min
		meta.MaxInt64 = max
		return v, nil
	case vector.KindString:
		values := make([]string, count)
		for i := range values {
			var length uint32
			if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
				return nil, err
			}
			data := make([]byte, length)
			if _, err := io.ReadFull(reader, data); err != nil {
				return nil, err
			}
			values[i] = string(data)
		}
		return vector.String{Values: values}, nil
	default:
		return nil, fmt.Errorf("unsupported vector kind %s", kind)
	}
}
