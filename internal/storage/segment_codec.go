package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

const (
	stringCodecPlain                byte = 1
	stringCodecDictionary           byte = 2
	maxLinearStringDictionaryValues      = 16
)

type preparedColumn struct {
	col     vector.Column
	stats   ColumnStats
	encoded []byte
	offset  uint64
}

func prepareSegmentColumns(batch vector.Batch) ([]preparedColumn, uint64, error) {
	prepared := make([]preparedColumn, 0, len(batch.Columns))
	segmentLen := uint64(segmentHeaderLen)
	footerLen := uint64(4)

	for _, col := range batch.Columns {
		preparedCol, err := prepareColumn(col)
		if err != nil {
			return nil, 0, err
		}
		preparedCol.offset = segmentLen
		columnLen, err := encodedColumnLen(preparedCol.stats)
		if err != nil {
			return nil, 0, err
		}
		segmentLen += columnLen

		entryLen, err := footerEntryLen(preparedCol.stats.Name)
		if err != nil {
			return nil, 0, err
		}
		footerLen += entryLen
		prepared = append(prepared, preparedCol)
	}
	segmentLen += footerLen + uint64(footerTrailerLen)
	return prepared, segmentLen, nil
}

func prepareColumn(col vector.Column) (preparedColumn, error) {
	stats := ColumnStats{Name: col.Name, Kind: col.Vector.Kind(), Count: col.Vector.Len()}

	switch values := col.Vector.(type) {
	case vector.Int64:
		min, max, ok := values.MinMax()
		stats.HasMinMax = ok
		stats.MinInt64 = min
		stats.MaxInt64 = max

		encodedLen, err := checkedMulInt("int64 encoded length", len(values.Values), 8)
		if err != nil {
			return preparedColumn{}, err
		}
		stats.EncodedLen = encodedLen
		return preparedColumn{col: col, stats: stats}, nil

	case vector.String:
		encoded, err := encodeStringVector(values)
		if err != nil {
			return preparedColumn{}, fmt.Errorf("encode column %q: %w", col.Name, err)
		}
		stats.EncodedLen = len(encoded)
		return preparedColumn{col: col, stats: stats, encoded: encoded}, nil

	default:
		return preparedColumn{}, fmt.Errorf("segment encoding is not implemented for %s vectors", col.Vector.Kind())
	}
}

func (s *segmentWriter) writePreparedColumn(col preparedColumn) error {
	if err := s.writeColumnHeader(col.stats, uint64(col.stats.EncodedLen)); err != nil {
		return err
	}
	switch values := col.col.Vector.(type) {
	case vector.Int64:
		return s.writeInt64Payload(values.Values)
	case vector.String:
		return writeFull(s.w, col.encoded)
	default:
		return fmt.Errorf("segment encoding is not implemented for %s vectors", col.col.Vector.Kind())
	}
}

func encodedColumnLen(stats ColumnStats) (uint64, error) {
	nameLen, err := checkedAddInt("column name encoded length", 2, len(stats.Name))
	if err != nil {
		return 0, err
	}
	return uint64(nameLen) + uint64(columnFixedHeaderLen) + uint64(stats.EncodedLen), nil
}

func encodeInt64(values []int64) []byte {
	out := make([]byte, 0, len(values)*8)
	for _, value := range values {
		out = binary.LittleEndian.AppendUint64(out, uint64(value))
	}
	return out
}

func encodeString(values []string) ([]byte, error) {
	return encodeStringVector(vector.String{Values: values})
}

func encodeStringVector(values vector.String) ([]byte, error) {
	plainLen, dictValues, dictIDs, dictDataLen, dictRowIDs, err := analyzeStringEncoding(values)
	if err != nil {
		return nil, err
	}
	idWidth := stringDictionaryIDWidth(len(dictValues))
	dictLen, err := stringDictionaryEncodedLen(dictDataLen, values.Len(), idWidth)
	if err != nil {
		return nil, err
	}
	if len(dictValues) > 0 && dictLen < plainLen {
		return encodeStringDictionary(values, dictValues, dictIDs, dictRowIDs, idWidth, dictLen)
	}
	return encodeStringPlain(values, plainLen), nil
}

func analyzeStringEncoding(values vector.String) (plainLen int, dictValues []string, dictIDs map[string]uint32, dictDataLen int, dictRowIDs []byte, err error) {
	plainLen = 1
	if values.Len() > 0 {
		dictRowIDs = make([]byte, 0, values.Len())
	}

	for i := 0; i < values.Len(); i++ {
		value := values.Value(i)
		if len(value) > 1<<32-1 {
			return 0, nil, nil, 0, nil, fmt.Errorf("string value is too long")
		}

		plainLen, err = checkedAddInt("string encoded length", plainLen, 4+len(value))
		if err != nil {
			return 0, nil, nil, 0, nil, err
		}

		id, ok := stringDictionaryID(value, dictValues, dictIDs)
		if ok {
			if dictRowIDs != nil {
				dictRowIDs = append(dictRowIDs, byte(id))
			}
			continue
		}
		if dictIDs == nil && len(dictValues) >= maxLinearStringDictionaryValues {
			dictIDs = make(map[string]uint32, min(values.Len(), 1024))
			for id, dictValue := range dictValues {
				dictIDs[dictValue] = uint32(id)
			}
		}
		if uint64(len(dictValues)) >= uint64(^uint32(0)) {
			return 0, nil, nil, 0, nil, fmt.Errorf("string dictionary value count overflows uint32")
		}

		id = uint32(len(dictValues))
		if dictIDs != nil {
			dictIDs[value] = id
		}
		dictValues = append(dictValues, value)
		if dictRowIDs != nil {
			if id <= uint32(^uint8(0)) {
				dictRowIDs = append(dictRowIDs, byte(id))
			} else {
				dictRowIDs = nil
			}
		}
		dictDataLen, err = checkedAddInt("string dictionary encoded length", dictDataLen, 4+len(value))
		if err != nil {
			return 0, nil, nil, 0, nil, err
		}
	}
	return plainLen, dictValues, dictIDs, dictDataLen, dictRowIDs, nil
}

func encodeStringPlain(values vector.String, size int) []byte {
	out := make([]byte, 0, size)
	out = append(out, stringCodecPlain)
	for i := 0; i < values.Len(); i++ {
		value := values.Value(i)
		out = binary.LittleEndian.AppendUint32(out, uint32(len(value)))
		out = append(out, value...)
	}
	return out
}

func encodeStringDictionary(values vector.String, dictValues []string, dictIDs map[string]uint32, dictRowIDs []byte, idWidth int, size int) ([]byte, error) {
	out := make([]byte, 0, size)
	out = append(out, stringCodecDictionary)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(dictValues)))
	out = append(out, byte(idWidth))

	for _, value := range dictValues {
		out = binary.LittleEndian.AppendUint32(out, uint32(len(value)))
		out = append(out, value...)
	}
	if idWidth == 1 && len(dictRowIDs) == values.Len() {
		out = append(out, dictRowIDs...)
		return out, nil
	}
	for i := 0; i < values.Len(); i++ {
		id, ok := stringDictionaryID(values.Value(i), dictValues, dictIDs)
		if !ok {
			return nil, fmt.Errorf("missing string dictionary id at row %d", i)
		}
		switch idWidth {
		case 1:
			out = append(out, byte(id))
		case 2:
			out = binary.LittleEndian.AppendUint16(out, uint16(id))
		case 4:
			out = binary.LittleEndian.AppendUint32(out, id)
		default:
			return nil, fmt.Errorf("unsupported string dictionary id width %d", idWidth)
		}
	}
	return out, nil
}

func stringDictionaryID(value string, dictValues []string, dictIDs map[string]uint32) (uint32, bool) {
	if dictIDs != nil {
		id, ok := dictIDs[value]
		return id, ok
	}
	for id, dictValue := range dictValues {
		if dictValue == value {
			return uint32(id), true
		}
	}
	return 0, false
}

func stringDictionaryIDWidth(dictCount int) int {
	if dictCount <= 1<<8 {
		return 1
	}
	if dictCount <= 1<<16 {
		return 2
	}
	return 4
}

func (s *segmentWriter) writeInt64Payload(values []int64) error {
	var buf [32 * 1024]byte
	for len(values) > 0 {
		count := min(len(values), len(buf)/8)
		chunk := buf[:count*8]
		for i, value := range values[:count] {
			binary.LittleEndian.PutUint64(chunk[i*8:], uint64(value))
		}
		if err := writeFull(s.w, chunk); err != nil {
			return err
		}
		values = values[count:]
	}
	return nil
}

func stringDictionaryEncodedLen(dictDataLen int, count int, idWidth int) (int, error) {
	size := 1 + 4 + 1
	size, err := checkedAddInt("string dictionary encoded length", size, dictDataLen)
	if err != nil {
		return 0, err
	}
	idsLen, err := checkedMulInt("string dictionary ids length", count, idWidth)
	if err != nil {
		return 0, err
	}
	return checkedAddInt("string dictionary encoded length", size, idsLen)
}

func (s *segmentReader) ReadColumn() (vector.Column, ColumnStats, error) {
	stats, err := s.readColumnHeader()
	if err != nil {
		return vector.Column{}, ColumnStats{}, err
	}
	col, err := s.readColumnPayload(stats)
	if err != nil {
		return vector.Column{}, ColumnStats{}, err
	}
	return col, stats, nil
}

func (s *segmentReader) readColumnPayload(stats ColumnStats) (vector.Column, error) {
	if stats.Kind == vector.KindInt64 {
		values, err := s.readInt64Payload(stats)
		if err != nil {
			return vector.Column{}, err
		}
		return vector.Column{Name: stats.Name, Vector: vector.FromInt64(values)}, nil
	}

	encoded := make([]byte, stats.EncodedLen)
	if err := readFull(s.r, encoded); err != nil {
		return vector.Column{}, err
	}

	values, err := decodeValues(stats.Kind, encoded, stats.Count)
	if err != nil {
		return vector.Column{}, err
	}
	return vector.Column{Name: stats.Name, Vector: values}, nil
}

func (s *segmentReader) readInt64Payload(stats ColumnStats) ([]int64, error) {
	if err := validateInt64Payload(stats, stats.EncodedLen); err != nil {
		return nil, err
	}

	// Decode directly from the stream to avoid allocating a full encoded payload for int64 columns.
	values := make([]int64, stats.Count)
	var buf [32 * 1024]byte
	row := 0
	remaining := stats.EncodedLen
	for remaining > 0 {
		chunk := min(remaining, len(buf))
		if err := readFull(s.r, buf[:chunk]); err != nil {
			return nil, err
		}
		rows := chunk / 8
		decodeInt64Into(values[row:row+rows], buf[:chunk])
		row += rows
		remaining -= chunk
	}
	return values, nil
}

func decodeValues(kind vector.Kind, encoded []byte, count int) (vector.Vector, error) {
	switch kind {
	case vector.KindInt64:
		values, err := decodeInt64(encoded, count)
		if err != nil {
			return nil, err
		}
		return vector.FromInt64(values), nil
	case vector.KindString:
		values, err := decodeString(encoded, count)
		if err != nil {
			return nil, err
		}
		return values, nil
	default:
		return nil, fmt.Errorf("unsupported vector kind %s", kind)
	}
}

func decodeInt64(encoded []byte, count int) ([]int64, error) {
	if err := validateInt64Payload(ColumnStats{Count: count}, len(encoded)); err != nil {
		return nil, err
	}

	values := make([]int64, count)
	decodeInt64Into(values, encoded)
	return values, nil
}

func decodeInt64Into(values []int64, encoded []byte) {
	for i := range values {
		values[i] = int64(binary.LittleEndian.Uint64(encoded[i*8:]))
	}
}

func decodeString(encoded []byte, count int) (vector.String, error) {
	if count < 0 {
		return vector.String{}, fmt.Errorf("negative string count %d", count)
	}
	if len(encoded) == 0 {
		return vector.String{}, fmt.Errorf("missing string codec")
	}

	switch encoded[0] {
	case stringCodecPlain:
		return decodePlainString(encoded, count)
	case stringCodecDictionary:
		return decodeDictionaryString(encoded, count)
	default:
		return vector.String{}, fmt.Errorf("unsupported string codec %d", encoded[0])
	}
}

func decodePlainString(encoded []byte, count int) (vector.String, error) {
	minLen, err := checkedMulInt("plain string lengths", count, 4)
	if err != nil {
		return vector.String{}, err
	}
	minLen, err = checkedAddInt("plain string encoded length", 1, minLen)
	if err != nil {
		return vector.String{}, err
	}
	if len(encoded) < minLen {
		return vector.String{}, fmt.Errorf("short string length at row 0")
	}

	ranges := make([]uint64, count)
	offset := 1
	for i := range ranges {
		if len(encoded)-offset < 4 {
			return vector.String{}, fmt.Errorf("short string length at row %d", i)
		}
		length := binary.LittleEndian.Uint32(encoded[offset:])
		offset += 4
		if uint64(length) > uint64(len(encoded)-offset) {
			return vector.String{}, fmt.Errorf("short string data at row %d", i)
		}
		if uint64(offset) > uint64(^uint32(0)) {
			return vector.String{}, fmt.Errorf("string offset at row %d overflows uint32", i)
		}
		ranges[i] = vector.StringRange(uint32(offset), length)
		offset += int(length)
	}
	if offset != len(encoded) {
		return vector.String{}, fmt.Errorf("trailing string bytes: %d", len(encoded)-offset)
	}
	return vector.FromStringDataUnsafe(encoded, ranges), nil
}

func decodeDictionaryString(encoded []byte, count int) (vector.String, error) {
	if len(encoded) < 1+4+1 {
		return vector.String{}, fmt.Errorf("short string dictionary header")
	}

	offset := 1
	dictCount, err := checkedInt("string dictionary count", uint64(binary.LittleEndian.Uint32(encoded[offset:])))
	if err != nil {
		return vector.String{}, err
	}
	offset += 4
	idWidth := int(encoded[offset])
	offset++
	if idWidth != 1 && idWidth != 2 && idWidth != 4 {
		return vector.String{}, fmt.Errorf("unsupported string dictionary id width %d", idWidth)
	}
	if dictCount > count {
		return vector.String{}, fmt.Errorf("string dictionary value count %d exceeds row count %d", dictCount, count)
	}
	minDictLen, err := checkedMulInt("string dictionary lengths", dictCount, 4)
	if err != nil {
		return vector.String{}, err
	}
	if len(encoded)-offset < minDictLen {
		return vector.String{}, fmt.Errorf("short string dictionary length at value 0")
	}

	dictRanges := make([]uint64, dictCount)
	for i := range dictRanges {
		if len(encoded)-offset < 4 {
			return vector.String{}, fmt.Errorf("short string dictionary length at value %d", i)
		}
		length := binary.LittleEndian.Uint32(encoded[offset:])
		offset += 4
		if uint64(length) > uint64(len(encoded)-offset) {
			return vector.String{}, fmt.Errorf("short string dictionary data at value %d", i)
		}
		if uint64(offset) > uint64(^uint32(0)) {
			return vector.String{}, fmt.Errorf("string dictionary offset at value %d overflows uint32", i)
		}
		dictRanges[i] = vector.StringRange(uint32(offset), length)
		offset += int(length)
	}

	idsLen, err := checkedMulInt("string dictionary ids length", count, idWidth)
	if err != nil {
		return vector.String{}, err
	}
	if len(encoded)-offset < idsLen {
		return vector.String{}, fmt.Errorf("short string dictionary ids")
	}
	if len(encoded)-offset > idsLen {
		return vector.String{}, fmt.Errorf("trailing string bytes: %d", len(encoded)-offset-idsLen)
	}

	ranges := make([]uint64, count)
	ids := encoded[offset : offset+idsLen]
	for row := range ranges {
		var id uint32
		switch idWidth {
		case 1:
			id = uint32(ids[row])
		case 2:
			id = uint32(binary.LittleEndian.Uint16(ids[row*2:]))
		case 4:
			id = binary.LittleEndian.Uint32(ids[row*4:])
		}
		if uint64(id) >= uint64(dictCount) {
			return vector.String{}, fmt.Errorf("string dictionary id %d out of range at row %d", id, row)
		}
		ranges[row] = dictRanges[id]
	}
	return vector.FromStringDataUnsafe(encoded, ranges), nil
}

func checkedInt(name string, value uint64) (int, error) {
	maxInt := uint64(^uint(0) >> 1)
	if value > maxInt {
		return 0, fmt.Errorf("%s overflows int: %d", name, value)
	}
	return int(value), nil
}

func checkedMulInt(name string, a, b int) (int, error) {
	if a < 0 || b < 0 {
		return 0, fmt.Errorf("%s is negative", name)
	}
	maxInt := int(^uint(0) >> 1)
	if a != 0 && b > maxInt/a {
		return 0, fmt.Errorf("%s overflows int", name)
	}
	return a * b, nil
}

func checkedAddInt(name string, a, b int) (int, error) {
	if a < 0 || b < 0 {
		return 0, fmt.Errorf("%s is negative", name)
	}
	maxInt := int(^uint(0) >> 1)
	if b > maxInt-a {
		return 0, fmt.Errorf("%s overflows int", name)
	}
	return a + b, nil
}

func validateInt64Payload(stats ColumnStats, encodedLen int) error {
	wantLen, err := checkedMulInt("int64 encoded length", stats.Count, 8)
	if err != nil {
		return err
	}
	if encodedLen != wantLen {
		return fmt.Errorf("invalid int64 encoded length %d for count %d", encodedLen, stats.Count)
	}
	return nil
}
