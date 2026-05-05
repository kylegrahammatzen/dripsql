package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

const (
	int64CodecPlain                 byte = 1
	int64CodecDictionary            byte = 2
	int64DictionaryPackedIDFlag     byte = 1 << 7
	maxLinearInt64DictionaryValues       = 16
	stringCodecPlain                byte = 1
	stringCodecDictionary           byte = 2
	stringDictionaryPackedIDFlag    byte = 1 << 7
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

		encoded, err := encodeInt64Vector(values)
		if err != nil {
			return preparedColumn{}, fmt.Errorf("encode column %q: %w", col.Name, err)
		}
		stats.EncodedLen = len(encoded)
		return preparedColumn{col: col, stats: stats, encoded: encoded}, nil

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
	return writeFull(s.w, col.encoded)
}

func encodedColumnLen(stats ColumnStats) (uint64, error) {
	nameLen, err := checkedAddInt("column name encoded length", 2, len(stats.Name))
	if err != nil {
		return 0, err
	}
	return uint64(nameLen) + uint64(columnFixedHeaderLen) + uint64(stats.EncodedLen), nil
}

func encodeInt64(values []int64) []byte {
	encoded, err := encodeInt64Vector(vector.Int64{Values: values})
	if err != nil {
		panic(err)
	}
	return encoded
}

func encodeInt64Vector(values vector.Int64) ([]byte, error) {
	plainLen, dictValues, dictIDs, dictRowIDs, err := analyzeInt64Encoding(values)
	if err != nil {
		return nil, err
	}
	idEncoding, dictLen, err := chooseInt64DictionaryIDEncoding(len(dictValues), values.Len())
	if err != nil {
		return nil, err
	}
	if len(dictValues) > 0 && dictLen < plainLen {
		return encodeInt64Dictionary(values, dictValues, dictIDs, dictRowIDs, idEncoding, dictLen)
	}
	return encodeInt64Plain(values, plainLen), nil
}

func analyzeInt64Encoding(values vector.Int64) (plainLen int, dictValues []int64, dictIDs map[int64]uint32, dictRowIDs []byte, err error) {
	dataLen, err := checkedMulInt("int64 encoded length", values.Len(), 8)
	if err != nil {
		return 0, nil, nil, nil, err
	}
	plainLen, err = checkedAddInt("int64 encoded length", 1, dataLen)
	if err != nil {
		return 0, nil, nil, nil, err
	}
	if values.Len() > 0 {
		dictRowIDs = make([]byte, 0, values.Len())
	}

	for _, value := range values.Values {
		id, ok := int64DictionaryID(value, dictValues, dictIDs)
		if ok {
			if dictRowIDs != nil {
				dictRowIDs = append(dictRowIDs, byte(id))
			}
			continue
		}
		if dictIDs == nil && len(dictValues) >= maxLinearInt64DictionaryValues {
			dictIDs = make(map[int64]uint32, min(values.Len(), 1024))
			for id, dictValue := range dictValues {
				dictIDs[dictValue] = uint32(id)
			}
		}
		if uint64(len(dictValues)) >= uint64(^uint32(0)) {
			return 0, nil, nil, nil, fmt.Errorf("int64 dictionary value count overflows uint32")
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
	}
	return plainLen, dictValues, dictIDs, dictRowIDs, nil
}

func encodeInt64Plain(values vector.Int64, size int) []byte {
	out := make([]byte, 0, size)
	out = append(out, int64CodecPlain)
	for _, value := range values.Values {
		out = binary.LittleEndian.AppendUint64(out, uint64(value))
	}
	return out
}

func encodeInt64Dictionary(values vector.Int64, dictValues []int64, dictIDs map[int64]uint32, dictRowIDs []byte, idEncoding int, size int) ([]byte, error) {
	out := make([]byte, 0, size)
	out = append(out, int64CodecDictionary)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(dictValues)))
	out = append(out, byte(idEncoding))

	for _, value := range dictValues {
		out = binary.LittleEndian.AppendUint64(out, uint64(value))
	}
	if int64DictionaryIDEncodingIsPacked(idEncoding) {
		return appendPackedInt64DictionaryIDs(out, values, dictValues, dictIDs, int64DictionaryPackedBitWidth(idEncoding))
	}
	if idEncoding == 1 && len(dictRowIDs) == values.Len() {
		out = append(out, dictRowIDs...)
		return out, nil
	}
	for _, value := range values.Values {
		id, ok := int64DictionaryID(value, dictValues, dictIDs)
		if !ok {
			return nil, fmt.Errorf("missing int64 dictionary id")
		}
		switch idEncoding {
		case 1:
			out = append(out, byte(id))
		case 2:
			out = binary.LittleEndian.AppendUint16(out, uint16(id))
		case 4:
			out = binary.LittleEndian.AppendUint32(out, id)
		default:
			return nil, fmt.Errorf("unsupported int64 dictionary id width %d", idEncoding)
		}
	}
	return out, nil
}

func appendPackedInt64DictionaryIDs(out []byte, values vector.Int64, dictValues []int64, dictIDs map[int64]uint32, bitWidth int) ([]byte, error) {
	idsLen, err := packedInt64DictionaryIDsLen(values.Len(), bitWidth)
	if err != nil {
		return nil, err
	}
	idsOffset := len(out)
	out = append(out, make([]byte, idsLen)...)
	if bitWidth == 0 {
		return out, nil
	}
	ids := out[idsOffset:]
	for row, value := range values.Values {
		id, ok := int64DictionaryID(value, dictValues, dictIDs)
		if !ok {
			return nil, fmt.Errorf("missing int64 dictionary id")
		}
		setPackedDictionaryID(ids, row, bitWidth, id)
	}
	return out, nil
}

func int64DictionaryID(value int64, dictValues []int64, dictIDs map[int64]uint32) (uint32, bool) {
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

func int64DictionaryIDWidth(dictCount int) int {
	if dictCount <= 1<<8 {
		return 1
	}
	if dictCount <= 1<<16 {
		return 2
	}
	return 4
}

func chooseInt64DictionaryIDEncoding(dictCount int, count int) (int, int, error) {
	fixedEncoding := int64DictionaryIDWidth(dictCount)
	fixedLen, err := int64DictionaryEncodedLen(dictCount, count, fixedEncoding)
	if err != nil {
		return 0, 0, err
	}
	packedBitWidth := int64DictionaryBitWidth(dictCount)
	packedEncoding := int(int64DictionaryPackedIDFlag) | packedBitWidth
	packedLen, err := int64DictionaryEncodedLen(dictCount, count, packedEncoding)
	if err != nil {
		return 0, 0, err
	}
	if packedLen < fixedLen {
		return packedEncoding, packedLen, nil
	}
	return fixedEncoding, fixedLen, nil
}

func int64DictionaryBitWidth(dictCount int) int {
	if dictCount <= 1 {
		return 0
	}
	bits := 0
	value := dictCount - 1
	for value > 0 {
		bits++
		value >>= 1
	}
	return bits
}

func int64DictionaryEncodedLen(dictCount int, count int, idEncoding int) (int, error) {
	size := 1 + 4 + 1
	dictLen, err := checkedMulInt("int64 dictionary values length", dictCount, 8)
	if err != nil {
		return 0, err
	}
	size, err = checkedAddInt("int64 dictionary encoded length", size, dictLen)
	if err != nil {
		return 0, err
	}
	idsLen, err := int64DictionaryIDsLen(count, idEncoding)
	if err != nil {
		return 0, err
	}
	return checkedAddInt("int64 dictionary encoded length", size, idsLen)
}

func int64DictionaryIDsLen(count int, idEncoding int) (int, error) {
	if int64DictionaryIDEncodingIsPacked(idEncoding) {
		return packedInt64DictionaryIDsLen(count, int64DictionaryPackedBitWidth(idEncoding))
	}
	return checkedMulInt("int64 dictionary ids length", count, idEncoding)
}

func packedInt64DictionaryIDsLen(count int, bitWidth int) (int, error) {
	bitsLen, err := checkedMulInt("int64 dictionary packed id bits", count, bitWidth)
	if err != nil {
		return 0, err
	}
	bitsLen, err = checkedAddInt("int64 dictionary packed id bits", bitsLen, 7)
	if err != nil {
		return 0, err
	}
	return bitsLen / 8, nil
}

func int64DictionaryIDEncodingIsPacked(idEncoding int) bool {
	return idEncoding&int(int64DictionaryPackedIDFlag) != 0
}

func int64DictionaryPackedBitWidth(idEncoding int) int {
	return idEncoding &^ int(int64DictionaryPackedIDFlag)
}

func setPackedDictionaryID(ids []byte, row int, bitWidth int, id uint32) {
	bitOffset := row * bitWidth
	byteOffset := bitOffset / 8
	shift := uint(bitOffset % 8)
	value := uint64(id) << shift
	bytes := (int(shift) + bitWidth + 7) / 8
	for i := 0; i < bytes; i++ {
		ids[byteOffset+i] |= byte(value >> (i * 8))
	}
}

func encodeString(values []string) ([]byte, error) {
	return encodeStringVector(vector.String{Values: values})
}

func encodeStringVector(values vector.String) ([]byte, error) {
	plainLen, dictValues, dictIDs, dictDataLen, dictRowIDs, err := analyzeStringEncoding(values)
	if err != nil {
		return nil, err
	}
	idEncoding, dictLen, err := chooseStringDictionaryIDEncoding(dictDataLen, len(dictValues), values.Len())
	if err != nil {
		return nil, err
	}
	if len(dictValues) > 0 && dictLen < plainLen {
		return encodeStringDictionary(values, dictValues, dictIDs, dictRowIDs, idEncoding, dictLen)
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

func encodeStringDictionary(values vector.String, dictValues []string, dictIDs map[string]uint32, dictRowIDs []byte, idEncoding int, size int) ([]byte, error) {
	out := make([]byte, 0, size)
	out = append(out, stringCodecDictionary)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(dictValues)))
	out = append(out, byte(idEncoding))

	for _, value := range dictValues {
		out = binary.LittleEndian.AppendUint32(out, uint32(len(value)))
		out = append(out, value...)
	}
	if stringDictionaryIDEncodingIsPacked(idEncoding) {
		return appendPackedStringDictionaryIDs(out, values, dictValues, dictIDs, stringDictionaryPackedBitWidth(idEncoding))
	}
	if idEncoding == 1 && len(dictRowIDs) == values.Len() {
		out = append(out, dictRowIDs...)
		return out, nil
	}
	for i := 0; i < values.Len(); i++ {
		id, ok := stringDictionaryID(values.Value(i), dictValues, dictIDs)
		if !ok {
			return nil, fmt.Errorf("missing string dictionary id at row %d", i)
		}
		switch idEncoding {
		case 1:
			out = append(out, byte(id))
		case 2:
			out = binary.LittleEndian.AppendUint16(out, uint16(id))
		case 4:
			out = binary.LittleEndian.AppendUint32(out, id)
		default:
			return nil, fmt.Errorf("unsupported string dictionary id width %d", idEncoding)
		}
	}
	return out, nil
}

func appendPackedStringDictionaryIDs(out []byte, values vector.String, dictValues []string, dictIDs map[string]uint32, bitWidth int) ([]byte, error) {
	idsLen, err := packedStringDictionaryIDsLen(values.Len(), bitWidth)
	if err != nil {
		return nil, err
	}
	idsOffset := len(out)
	out = append(out, make([]byte, idsLen)...)
	if bitWidth == 0 {
		return out, nil
	}
	ids := out[idsOffset:]
	for row := 0; row < values.Len(); row++ {
		id, ok := stringDictionaryID(values.Value(row), dictValues, dictIDs)
		if !ok {
			return nil, fmt.Errorf("missing string dictionary id at row %d", row)
		}
		setPackedDictionaryID(ids, row, bitWidth, id)
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

func chooseStringDictionaryIDEncoding(dictDataLen int, dictCount int, count int) (int, int, error) {
	fixedEncoding := stringDictionaryIDWidth(dictCount)
	fixedLen, err := stringDictionaryEncodedLen(dictDataLen, count, fixedEncoding)
	if err != nil {
		return 0, 0, err
	}
	packedBitWidth := stringDictionaryBitWidth(dictCount)
	packedEncoding := int(stringDictionaryPackedIDFlag) | packedBitWidth
	packedLen, err := stringDictionaryEncodedLen(dictDataLen, count, packedEncoding)
	if err != nil {
		return 0, 0, err
	}
	if packedLen < fixedLen {
		return packedEncoding, packedLen, nil
	}
	return fixedEncoding, fixedLen, nil
}

func stringDictionaryBitWidth(dictCount int) int {
	return int64DictionaryBitWidth(dictCount)
}

func stringDictionaryEncodedLen(dictDataLen int, count int, idEncoding int) (int, error) {
	size := 1 + 4 + 1
	size, err := checkedAddInt("string dictionary encoded length", size, dictDataLen)
	if err != nil {
		return 0, err
	}
	idsLen, err := stringDictionaryIDsLen(count, idEncoding)
	if err != nil {
		return 0, err
	}
	return checkedAddInt("string dictionary encoded length", size, idsLen)
}

func stringDictionaryIDsLen(count int, idEncoding int) (int, error) {
	if stringDictionaryIDEncodingIsPacked(idEncoding) {
		return packedStringDictionaryIDsLen(count, stringDictionaryPackedBitWidth(idEncoding))
	}
	return checkedMulInt("string dictionary ids length", count, idEncoding)
}

func packedStringDictionaryIDsLen(count int, bitWidth int) (int, error) {
	bitsLen, err := checkedMulInt("string dictionary packed id bits", count, bitWidth)
	if err != nil {
		return 0, err
	}
	bitsLen, err = checkedAddInt("string dictionary packed id bits", bitsLen, 7)
	if err != nil {
		return 0, err
	}
	return bitsLen / 8, nil
}

func stringDictionaryIDEncodingIsPacked(idEncoding int) bool {
	return idEncoding&int(stringDictionaryPackedIDFlag) != 0
}

func stringDictionaryPackedBitWidth(idEncoding int) int {
	return idEncoding &^ int(stringDictionaryPackedIDFlag)
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
	if stats.Count < 0 {
		return nil, fmt.Errorf("negative int64 count %d", stats.Count)
	}
	encoded := make([]byte, stats.EncodedLen)
	if err := readFull(s.r, encoded); err != nil {
		return nil, err
	}
	return decodeInt64(encoded, stats.Count)
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
	if count < 0 {
		return nil, fmt.Errorf("negative int64 count %d", count)
	}
	if len(encoded) == 0 {
		return nil, fmt.Errorf("missing int64 codec")
	}

	switch encoded[0] {
	case int64CodecPlain:
		return decodePlainInt64(encoded, count)
	case int64CodecDictionary:
		return decodeDictionaryInt64(encoded, count)
	default:
		return nil, fmt.Errorf("unsupported int64 codec %d", encoded[0])
	}
}

func decodePlainInt64(encoded []byte, count int) ([]int64, error) {
	if err := validatePlainInt64Payload(ColumnStats{Count: count}, len(encoded)); err != nil {
		return nil, err
	}

	values := make([]int64, count)
	decodeInt64Into(values, encoded[1:])
	return values, nil
}

func decodeDictionaryInt64(encoded []byte, count int) ([]int64, error) {
	dict, ids, idEncoding, dictCount, err := parseInt64DictionaryPayload(encoded, ColumnStats{Count: count})
	if err != nil {
		return nil, err
	}
	values := make([]int64, count)
	if int64DictionaryIDEncodingIsPacked(idEncoding) {
		return decodePackedDictionaryInt64(values, dict, ids, int64DictionaryPackedBitWidth(idEncoding), dictCount)
	}
	switch idEncoding {
	case 1:
		for row := 0; row < count; row++ {
			id := ids[row]
			if int(id) >= dictCount {
				return nil, fmt.Errorf("int64 dictionary id %d out of range at row %d", id, row)
			}
			values[row] = int64(binary.LittleEndian.Uint64(dict[int(id)*8:]))
		}
	case 2:
		for row := 0; row < count; row++ {
			id := binary.LittleEndian.Uint16(ids[row*2:])
			if int(id) >= dictCount {
				return nil, fmt.Errorf("int64 dictionary id %d out of range at row %d", id, row)
			}
			values[row] = int64(binary.LittleEndian.Uint64(dict[int(id)*8:]))
		}
	case 4:
		for row := 0; row < count; row++ {
			id := binary.LittleEndian.Uint32(ids[row*4:])
			if uint64(id) >= uint64(dictCount) {
				return nil, fmt.Errorf("int64 dictionary id %d out of range at row %d", id, row)
			}
			values[row] = int64(binary.LittleEndian.Uint64(dict[int(id)*8:]))
		}
	}
	return values, nil
}

func decodePackedDictionaryInt64(values []int64, dict []byte, ids []byte, bitWidth int, dictCount int) ([]int64, error) {
	for row := range values {
		id := packedDictionaryIDAt(ids, row, bitWidth)
		if uint64(id) >= uint64(dictCount) {
			return nil, fmt.Errorf("int64 dictionary id %d out of range at row %d", id, row)
		}
		values[row] = int64(binary.LittleEndian.Uint64(dict[int(id)*8:]))
	}
	return values, nil
}

func decodeInt64Into(values []int64, encoded []byte) {
	for i := range values {
		values[i] = int64(binary.LittleEndian.Uint64(encoded[i*8:]))
	}
}

func parseInt64DictionaryPayload(payload []byte, stats ColumnStats) (dict []byte, ids []byte, idEncoding int, dictCount int, err error) {
	if len(payload) < 1+4+1 {
		return nil, nil, 0, 0, fmt.Errorf("short int64 dictionary header")
	}
	offset := 1
	dictCount = int(binary.LittleEndian.Uint32(payload[offset:]))
	offset += 4
	idEncoding = int(payload[offset])
	offset++
	if err := validateInt64DictionaryIDEncoding(stats, dictCount, idEncoding); err != nil {
		return nil, nil, 0, 0, err
	}

	dictLen, err := checkedMulInt("int64 dictionary values length", dictCount, 8)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	if len(payload)-offset < dictLen {
		return nil, nil, 0, 0, fmt.Errorf("short int64 dictionary values")
	}
	dict = payload[offset : offset+dictLen]
	offset += dictLen

	idsLen, err := int64DictionaryIDsLen(stats.Count, idEncoding)
	if err != nil {
		return nil, nil, 0, 0, err
	}
	if len(payload)-offset < idsLen {
		return nil, nil, 0, 0, fmt.Errorf("short int64 dictionary ids")
	}
	if len(payload)-offset > idsLen {
		return nil, nil, 0, 0, fmt.Errorf("trailing int64 bytes: %d", len(payload)-offset-idsLen)
	}
	return dict, payload[offset : offset+idsLen], idEncoding, dictCount, nil
}

func validateInt64DictionaryIDEncoding(stats ColumnStats, dictCount int, idEncoding int) error {
	if int64DictionaryIDEncodingIsPacked(idEncoding) {
		bitWidth := int64DictionaryPackedBitWidth(idEncoding)
		if bitWidth > 32 {
			return fmt.Errorf("unsupported int64 dictionary packed bit width %d", bitWidth)
		}
		if bitWidth == 0 {
			if dictCount > 1 {
				return fmt.Errorf("int64 dictionary value count %d exceeds packed bit width %d", dictCount, bitWidth)
			}
		} else if uint64(dictCount) > uint64(1)<<uint(bitWidth) {
			return fmt.Errorf("int64 dictionary value count %d exceeds packed bit width %d", dictCount, bitWidth)
		}
		if dictCount > stats.Count {
			return fmt.Errorf("int64 dictionary value count %d exceeds row count %d", dictCount, stats.Count)
		}
		return nil
	}

	if idEncoding != 1 && idEncoding != 2 && idEncoding != 4 {
		return fmt.Errorf("unsupported int64 dictionary id width %d", idEncoding)
	}
	if idEncoding == 1 && dictCount > 1<<8 {
		return fmt.Errorf("int64 dictionary value count %d exceeds id width %d", dictCount, idEncoding)
	}
	if idEncoding == 2 && dictCount > 1<<16 {
		return fmt.Errorf("int64 dictionary value count %d exceeds id width %d", dictCount, idEncoding)
	}
	if dictCount > stats.Count {
		return fmt.Errorf("int64 dictionary value count %d exceeds row count %d", dictCount, stats.Count)
	}
	return nil
}

func packedDictionaryIDAt(ids []byte, row int, bitWidth int) uint32 {
	if bitWidth == 0 {
		return 0
	}
	bitOffset := row * bitWidth
	byteOffset := bitOffset / 8
	shift := uint(bitOffset % 8)
	bytes := (int(shift) + bitWidth + 7) / 8
	var value uint64
	for i := 0; i < bytes; i++ {
		value |= uint64(ids[byteOffset+i]) << (i * 8)
	}
	if bitWidth == 32 {
		return uint32(value >> shift)
	}
	mask := (uint64(1) << uint(bitWidth)) - 1
	return uint32((value >> shift) & mask)
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
	idEncoding := int(encoded[offset])
	offset++
	if err := validateStringDictionaryIDEncoding(ColumnStats{Count: count}, dictCount, idEncoding); err != nil {
		return vector.String{}, err
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

	idsLen, err := stringDictionaryIDsLen(count, idEncoding)
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
	if stringDictionaryIDEncodingIsPacked(idEncoding) {
		return decodePackedDictionaryString(encoded, ranges, dictRanges, ids, stringDictionaryPackedBitWidth(idEncoding), dictCount)
	}
	for row := range ranges {
		id, err := fixedDictionaryIDAt(ids, row, idEncoding, dictCount, "string")
		if err != nil {
			return vector.String{}, err
		}
		ranges[row] = dictRanges[id]
	}
	return vector.FromStringDataUnsafe(encoded, ranges), nil
}

func decodePackedDictionaryString(encoded []byte, ranges []uint64, dictRanges []uint64, ids []byte, bitWidth int, dictCount int) (vector.String, error) {
	for row := range ranges {
		id := packedDictionaryIDAt(ids, row, bitWidth)
		if uint64(id) >= uint64(dictCount) {
			return vector.String{}, fmt.Errorf("string dictionary id %d out of range at row %d", id, row)
		}
		ranges[row] = dictRanges[id]
	}
	return vector.FromStringDataUnsafe(encoded, ranges), nil
}

func fixedDictionaryIDAt(ids []byte, row int, idEncoding int, dictCount int, label string) (uint32, error) {
	var id uint32
	switch idEncoding {
	case 1:
		id = uint32(ids[row])
	case 2:
		id = uint32(binary.LittleEndian.Uint16(ids[row*2:]))
	case 4:
		id = binary.LittleEndian.Uint32(ids[row*4:])
	}
	if uint64(id) >= uint64(dictCount) {
		return 0, fmt.Errorf("%s dictionary id %d out of range at row %d", label, id, row)
	}
	return id, nil
}

func validateStringDictionaryIDEncoding(stats ColumnStats, dictCount int, idEncoding int) error {
	if stringDictionaryIDEncodingIsPacked(idEncoding) {
		bitWidth := stringDictionaryPackedBitWidth(idEncoding)
		if bitWidth > 32 {
			return fmt.Errorf("unsupported string dictionary packed bit width %d", bitWidth)
		}
		if bitWidth == 0 {
			if dictCount > 1 {
				return fmt.Errorf("string dictionary value count %d exceeds packed bit width %d", dictCount, bitWidth)
			}
		} else if uint64(dictCount) > uint64(1)<<uint(bitWidth) {
			return fmt.Errorf("string dictionary value count %d exceeds packed bit width %d", dictCount, bitWidth)
		}
		if dictCount > stats.Count {
			return fmt.Errorf("string dictionary value count %d exceeds row count %d", dictCount, stats.Count)
		}
		return nil
	}

	if idEncoding != 1 && idEncoding != 2 && idEncoding != 4 {
		return fmt.Errorf("unsupported string dictionary id width %d", idEncoding)
	}
	if idEncoding == 1 && dictCount > 1<<8 {
		return fmt.Errorf("string dictionary value count %d exceeds id width %d", dictCount, idEncoding)
	}
	if idEncoding == 2 && dictCount > 1<<16 {
		return fmt.Errorf("string dictionary value count %d exceeds id width %d", dictCount, idEncoding)
	}
	if dictCount > stats.Count {
		return fmt.Errorf("string dictionary value count %d exceeds row count %d", dictCount, stats.Count)
	}
	return nil
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

func validatePlainInt64Payload(stats ColumnStats, encodedLen int) error {
	if stats.Count < 0 {
		return fmt.Errorf("negative int64 count %d", stats.Count)
	}
	wantLen, err := checkedMulInt("int64 encoded length", stats.Count, 8)
	if err != nil {
		return err
	}
	wantLen, err = checkedAddInt("int64 encoded length", 1, wantLen)
	if err != nil {
		return err
	}
	if encodedLen != wantLen {
		return fmt.Errorf("invalid int64 encoded length %d for count %d", encodedLen, stats.Count)
	}
	return nil
}
