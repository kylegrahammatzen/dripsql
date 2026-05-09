package storage

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"

	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// le32 covers every value type the plain encoder writes as a 4-byte LE word.
type le32 interface {
	~int32 | ~uint32
}

// le64 covers every value type the plain encoder writes as an 8-byte LE word.
type le64 interface {
	~int64 | ~uint64
}

// encodeColumnPage serializes col[start:start+rows] into a self-contained byte payload (validity bitmap + values) and returns the matching PageMeta.
func encodeColumnPage(col vector.Column, start int, rows int) ([]byte, PageMeta, error) {
	valid, nullCount := pageValidity(col.V.Valid, start, rows)
	meta := PageMeta{
		Rows:      uint32(rows),
		NullCount: uint32(nullCount),
		AllValid:  nullCount == 0,
		AllNull:   nullCount == rows,
		Codec:     CodecPlain,
	}
	switch col.Type.Kind {
	case sqltype.KindBool:
		return encodeBoolPlain(col, valid, start, rows), meta, nil
	case sqltype.KindInt16:
		payload, stats := encodeInt16Plain(col, valid, start, rows)
		meta.Int32 = stats
		return payload, meta, nil
	case sqltype.KindInt32, sqltype.KindDate:
		payload, stats := encodeInt32Plain(col, valid, start, rows)
		meta.Int32 = stats
		return payload, meta, nil
	case sqltype.KindInt64, sqltype.KindTimestamp:
		payload, stats := encodeInt64Plain(col, valid, start, rows)
		meta.Int64 = stats
		return payload, meta, nil
	case sqltype.KindFloat32:
		return encodeFloat32Plain(col, valid, start, rows), meta, nil
	case sqltype.KindFloat64:
		return encodeFloat64Plain(col, valid, start, rows), meta, nil
	case sqltype.KindNamed:
		return encodeNamedPlain(col, valid, start, rows), meta, nil
	case sqltype.KindUUID:
		return encodeUUIDPlain(col, valid, start, rows), meta, nil
	case sqltype.KindText:
		return encodeTextPage(col, valid, start, rows, &meta)
	case sqltype.KindBytes:
		payload, _, err := encodeVarBytesPlain(col, valid, start, rows, false)
		if err != nil {
			return nil, PageMeta{}, err
		}
		return payload, meta, nil
	default:
		return nil, PageMeta{}, fmt.Errorf("unsupported plain encoding type %s", col.Type)
	}
}

// encodeFixed16 writes valid + len(values)*2 bytes (each int16 LE).
func encodeFixed16(valid vector.Validity, values []int16) []byte {
	payload := make([]byte, validityPayloadLen(valid)+len(values)*2)
	pos := writeValidityPayload(payload, valid)
	for _, v := range values {
		binary.LittleEndian.PutUint16(payload[pos:pos+2], uint16(v))
		pos += 2
	}
	return payload
}

// encodeFixed32 writes valid + len(values)*4 bytes (each value LE).
func encodeFixed32[T le32](valid vector.Validity, values []T) []byte {
	payload := make([]byte, validityPayloadLen(valid)+len(values)*4)
	pos := writeValidityPayload(payload, valid)
	for _, v := range values {
		binary.LittleEndian.PutUint32(payload[pos:pos+4], uint32(v))
		pos += 4
	}
	return payload
}

// encodeFixed64 writes valid + len(values)*8 bytes (each value LE).
func encodeFixed64[T le64](valid vector.Validity, values []T) []byte {
	payload := make([]byte, validityPayloadLen(valid)+len(values)*8)
	pos := writeValidityPayload(payload, valid)
	for _, v := range values {
		binary.LittleEndian.PutUint64(payload[pos:pos+8], uint64(v))
		pos += 8
	}
	return payload
}

// decodeFixed16 reads len*2 bytes as int16 LE values.
func decodeFixed16(payload []byte, pos, rows int, colName, typeName string) ([]int16, error) {
	if len(payload)-pos < rows*2 {
		return nil, fmt.Errorf("column %q page payload is too short for %s values", colName, typeName)
	}
	values := make([]int16, rows)
	for i := range values {
		values[i] = int16(binary.LittleEndian.Uint16(payload[pos : pos+2]))
		pos += 2
	}
	return values, nil
}

// decodeFixed32 reads rows*4 bytes as T LE values.
func decodeFixed32[T le32](payload []byte, pos, rows int, colName, typeName string) ([]T, error) {
	if len(payload)-pos < rows*4 {
		return nil, fmt.Errorf("column %q page payload is too short for %s values", colName, typeName)
	}
	values := make([]T, rows)
	for i := range values {
		values[i] = T(binary.LittleEndian.Uint32(payload[pos : pos+4]))
		pos += 4
	}
	return values, nil
}

// decodeFixed64 reads rows*8 bytes as T LE values.
func decodeFixed64[T le64](payload []byte, pos, rows int, colName, typeName string) ([]T, error) {
	if len(payload)-pos < rows*8 {
		return nil, fmt.Errorf("column %q page payload is too short for %s values", colName, typeName)
	}
	values := make([]T, rows)
	for i := range values {
		values[i] = T(binary.LittleEndian.Uint64(payload[pos : pos+8]))
		pos += 8
	}
	return values, nil
}

// observeInt32Slice folds every valid row of values into a single Int32Stats.
func observeInt32Slice(valid vector.Validity, values []int32) *Int32Stats {
	var stats *Int32Stats
	for i, v := range values {
		if vector.IsValid(valid, i) {
			stats = observeInt32(stats, v)
		}
	}
	return stats
}

// observeInt16Slice folds every valid row into Int32Stats (int16 widens losslessly to int32).
func observeInt16Slice(valid vector.Validity, values []int16) *Int32Stats {
	var stats *Int32Stats
	for i, v := range values {
		if vector.IsValid(valid, i) {
			stats = observeInt32(stats, int32(v))
		}
	}
	return stats
}

// observeInt64Slice folds every valid row of values into a single Int64Stats.
func observeInt64Slice(valid vector.Validity, values []int64) *Int64Stats {
	var stats *Int64Stats
	for i, v := range values {
		if vector.IsValid(valid, i) {
			stats = observeInt64(stats, v)
		}
	}
	return stats
}

func encodeBoolPlain(col vector.Column, valid vector.Validity, start, rows int) []byte {
	payload := make([]byte, validityPayloadLen(valid)+vector.ValidityWords(rows)*8)
	pos := writeValidityPayload(payload, valid)
	for row := 0; row < rows; row++ {
		if boolAt(col.V.BoolBits, start+row) {
			setPayloadBit(payload[pos:], row)
		}
	}
	return payload
}

func encodeInt16Plain(col vector.Column, valid vector.Validity, start, rows int) ([]byte, *Int32Stats) {
	values := col.V.I16[start : start+rows]
	return encodeFixed16(valid, values), observeInt16Slice(valid, values)
}

func encodeInt32Plain(col vector.Column, valid vector.Validity, start, rows int) ([]byte, *Int32Stats) {
	values := col.V.I32[start : start+rows]
	return encodeFixed32(valid, values), observeInt32Slice(valid, values)
}

func encodeInt64Plain(col vector.Column, valid vector.Validity, start, rows int) ([]byte, *Int64Stats) {
	values := col.V.I64[start : start+rows]
	return encodeFixed64(valid, values), observeInt64Slice(valid, values)
}

// encodeFloat32Plain inlines the LE write so floats don't need an intermediate []uint32 buffer.
func encodeFloat32Plain(col vector.Column, valid vector.Validity, start, rows int) []byte {
	payload := make([]byte, validityPayloadLen(valid)+rows*4)
	pos := writeValidityPayload(payload, valid)
	for _, v := range col.V.F32[start : start+rows] {
		binary.LittleEndian.PutUint32(payload[pos:pos+4], math.Float32bits(v))
		pos += 4
	}
	return payload
}

// encodeFloat64Plain is encodeFloat32Plain at 8-byte width.
func encodeFloat64Plain(col vector.Column, valid vector.Validity, start, rows int) []byte {
	payload := make([]byte, validityPayloadLen(valid)+rows*8)
	pos := writeValidityPayload(payload, valid)
	for _, v := range col.V.F64[start : start+rows] {
		binary.LittleEndian.PutUint64(payload[pos:pos+8], math.Float64bits(v))
		pos += 8
	}
	return payload
}

func encodeNamedPlain(col vector.Column, valid vector.Validity, start, rows int) []byte {
	return encodeFixed32(valid, col.V.U32[start:start+rows])
}

func encodeUUIDPlain(col vector.Column, valid vector.Validity, start, rows int) []byte {
	payload := make([]byte, validityPayloadLen(valid)+rows*16)
	pos := writeValidityPayload(payload, valid)
	for _, value := range col.V.UUID[start : start+rows] {
		copy(payload[pos:pos+16], value[:])
		pos += 16
	}
	return payload
}

// encodeVarBytesPlain handles Text and Bytes columns; withTextStats enables the histogram for KindText only (KindBytes never produces stats).
func encodeVarBytesPlain(col vector.Column, valid vector.Validity, start, rows int, withTextStats bool) ([]byte, *TextStats, error) {
	dataLen := 0
	for row := 0; row < rows; row++ {
		if vector.IsValid(valid, row) {
			dataLen += len(col.V.Var.Bytes(start + row))
		}
	}
	if dataLen > int(^uint32(0)) {
		return nil, nil, fmt.Errorf("text page payload too large")
	}
	var stats *TextStats
	if withTextStats {
		stats = buildTextStats(col.V.Var, valid, start, rows)
	}
	offsetLen := (rows + 1) * 4
	payload := make([]byte, validityPayloadLen(valid)+offsetLen+dataLen)
	pos := writeValidityPayload(payload, valid)
	offsetPos := pos
	dataPos := pos + offsetLen
	dataStart := dataPos
	for row := 0; row < rows; row++ {
		if vector.IsValid(valid, row) {
			value := col.V.Var.Bytes(start + row)
			dataPos += copy(payload[dataPos:], value)
		}
		binary.LittleEndian.PutUint32(payload[offsetPos+(row+1)*4:], uint32(dataPos-dataStart))
	}
	return payload, stats, nil
}

func readColumnPage(path string, col ColumnMeta, page PageMeta) (vector.Column, error) {
	file, err := os.Open(path)
	if err != nil {
		return vector.Column{}, err
	}
	defer file.Close()
	return readColumnPageFromFile(file, col, page)
}

// readColumnPageFromFile reads one page worth of values for col, mirroring encodeColumnPage's dispatch.
func readColumnPageFromFile(file *os.File, col ColumnMeta, page PageMeta) (vector.Column, error) {
	payload := make([]byte, page.Length)
	if _, err := file.ReadAt(payload, int64(page.Offset)); err != nil {
		return vector.Column{}, err
	}
	valid, pos, err := decodeValidity(payload, page, col.Name)
	if err != nil {
		return vector.Column{}, err
	}
	rows := int(page.Rows)
	switch col.Type.Kind {
	case sqltype.KindBool:
		vec, err := decodeBoolPlain(payload, valid, pos, rows, col.Name)
		if err != nil {
			return vector.Column{}, err
		}
		return vector.Column{Name: col.Name, Type: col.Type, V: vec}, nil
	case sqltype.KindInt16:
		vec, err := decodeInt16Plain(payload, valid, pos, rows, col.Name)
		if err != nil {
			return vector.Column{}, err
		}
		return vector.Column{Name: col.Name, Type: col.Type, V: vec}, nil
	case sqltype.KindInt32, sqltype.KindDate:
		kind, err := vectorKindForType(col.Type)
		if err != nil {
			return vector.Column{}, err
		}
		vec, err := decodeInt32Plain(payload, valid, pos, rows, kind, col.Name)
		if err != nil {
			return vector.Column{}, err
		}
		return vector.Column{Name: col.Name, Type: col.Type, V: vec}, nil
	case sqltype.KindInt64, sqltype.KindTimestamp:
		kind, err := vectorKindForType(col.Type)
		if err != nil {
			return vector.Column{}, err
		}
		vec, err := decodeInt64Plain(payload, valid, pos, rows, kind, col.Name)
		if err != nil {
			return vector.Column{}, err
		}
		return vector.Column{Name: col.Name, Type: col.Type, V: vec}, nil
	case sqltype.KindFloat32:
		vec, err := decodeFloat32Plain(payload, valid, pos, rows, col.Name)
		if err != nil {
			return vector.Column{}, err
		}
		return vector.Column{Name: col.Name, Type: col.Type, V: vec}, nil
	case sqltype.KindFloat64:
		vec, err := decodeFloat64Plain(payload, valid, pos, rows, col.Name)
		if err != nil {
			return vector.Column{}, err
		}
		return vector.Column{Name: col.Name, Type: col.Type, V: vec}, nil
	case sqltype.KindNamed:
		vec, err := decodeNamedPlain(payload, valid, pos, rows, col.Name)
		if err != nil {
			return vector.Column{}, err
		}
		return vector.Column{Name: col.Name, Type: col.Type, EnumLabels: append([]string(nil), col.EnumLabels...), V: vec}, nil
	case sqltype.KindUUID:
		vec, err := decodeUUIDPlain(payload, valid, pos, rows, col.Name)
		if err != nil {
			return vector.Column{}, err
		}
		return vector.Column{Name: col.Name, Type: col.Type, V: vec}, nil
	case sqltype.KindText, sqltype.KindBytes:
		kind, err := vectorKindForType(col.Type)
		if err != nil {
			return vector.Column{}, err
		}
		if col.Type.Kind == sqltype.KindText && page.Codec == CodecDictionary {
			vec, err := decodeTextDictionary(payload, valid, pos, rows, kind, col.Name)
			if err != nil {
				return vector.Column{}, err
			}
			return vector.Column{Name: col.Name, Type: col.Type, V: vec}, nil
		}
		vec, err := decodeVarBytesPlain(payload, valid, pos, rows, kind, col.Name)
		if err != nil {
			return vector.Column{}, err
		}
		return vector.Column{Name: col.Name, Type: col.Type, V: vec}, nil
	default:
		return vector.Column{}, fmt.Errorf("unsupported plain decoding type %s", col.Type)
	}
}

func decodeBoolPlain(payload []byte, valid vector.Validity, pos, rows int, colName string) (vector.Vec, error) {
	boolBytes := vector.ValidityWords(rows) * 8
	if len(payload)-pos < boolBytes {
		return vector.Vec{}, fmt.Errorf("column %q page payload is too short for bool values", colName)
	}
	bits := make([]uint64, vector.ValidityWords(rows))
	for i := range bits {
		bits[i] = binary.LittleEndian.Uint64(payload[pos : pos+8])
		pos += 8
	}
	return vector.Vec{Kind: vector.Bool, Len: rows, Valid: valid, BoolBits: bits}, nil
}

func decodeInt16Plain(payload []byte, valid vector.Validity, pos, rows int, colName string) (vector.Vec, error) {
	values, err := decodeFixed16(payload, pos, rows, colName, "int16")
	if err != nil {
		return vector.Vec{}, err
	}
	return vector.Vec{Kind: vector.Int16, Len: rows, Valid: valid, I16: values}, nil
}

func decodeInt32Plain(payload []byte, valid vector.Validity, pos, rows int, kind vector.Kind, colName string) (vector.Vec, error) {
	values, err := decodeFixed32[int32](payload, pos, rows, colName, "int32/date")
	if err != nil {
		return vector.Vec{}, err
	}
	return vector.Vec{Kind: kind, Len: rows, Valid: valid, I32: values}, nil
}

func decodeInt64Plain(payload []byte, valid vector.Validity, pos, rows int, kind vector.Kind, colName string) (vector.Vec, error) {
	values, err := decodeFixed64[int64](payload, pos, rows, colName, "int64/timestamp")
	if err != nil {
		return vector.Vec{}, err
	}
	return vector.Vec{Kind: kind, Len: rows, Valid: valid, I64: values}, nil
}

// decodeFloat32Plain inlines the LE read so floats don't need an intermediate []uint32 buffer.
func decodeFloat32Plain(payload []byte, valid vector.Validity, pos, rows int, colName string) (vector.Vec, error) {
	if len(payload)-pos < rows*4 {
		return vector.Vec{}, fmt.Errorf("column %q page payload is too short for float32 values", colName)
	}
	values := make([]float32, rows)
	for i := range values {
		values[i] = math.Float32frombits(binary.LittleEndian.Uint32(payload[pos : pos+4]))
		pos += 4
	}
	return vector.Vec{Kind: vector.Float32, Len: rows, Valid: valid, F32: values}, nil
}

func decodeFloat64Plain(payload []byte, valid vector.Validity, pos, rows int, colName string) (vector.Vec, error) {
	if len(payload)-pos < rows*8 {
		return vector.Vec{}, fmt.Errorf("column %q page payload is too short for float64 values", colName)
	}
	values := make([]float64, rows)
	for i := range values {
		values[i] = math.Float64frombits(binary.LittleEndian.Uint64(payload[pos : pos+8]))
		pos += 8
	}
	return vector.Vec{Kind: vector.Float64, Len: rows, Valid: valid, F64: values}, nil
}

func decodeNamedPlain(payload []byte, valid vector.Validity, pos, rows int, colName string) (vector.Vec, error) {
	values, err := decodeFixed32[uint32](payload, pos, rows, colName, "enum")
	if err != nil {
		return vector.Vec{}, err
	}
	return vector.Vec{Kind: vector.Enum32, Len: rows, Valid: valid, U32: values}, nil
}

func decodeUUIDPlain(payload []byte, valid vector.Validity, pos, rows int, colName string) (vector.Vec, error) {
	if len(payload)-pos < rows*16 {
		return vector.Vec{}, fmt.Errorf("column %q page payload is too short for uuid values", colName)
	}
	values := make([]vector.UUID16, rows)
	for i := range values {
		copy(values[i][:], payload[pos:pos+16])
		pos += 16
	}
	return vector.Vec{Kind: vector.UUID, Len: rows, Valid: valid, UUID: values}, nil
}

func decodeVarBytesPlain(payload []byte, valid vector.Validity, pos, rows int, kind vector.Kind, colName string) (vector.Vec, error) {
	offsetBytes := (rows + 1) * 4
	if len(payload)-pos < offsetBytes {
		return vector.Vec{}, fmt.Errorf("column %q page payload is too short for varbytes offsets", colName)
	}
	offsets := make([]uint32, rows+1)
	for i := range offsets {
		offsets[i] = binary.LittleEndian.Uint32(payload[pos : pos+4])
		pos += 4
	}
	data := make([]byte, len(payload)-pos)
	copy(data, payload[pos:])
	return vector.Vec{Kind: kind, Len: rows, Valid: valid, Var: vector.VarBytes{Offsets: offsets, Data: data}}, nil
}

// decodeValidityBytes reads the optional leading validity bitmap as raw bytes (used by byte-level fast paths), returning nil for AllValid pages and the bytes consumed.
func decodeValidityBytes(payload []byte, page PageMeta, colName string) ([]byte, int, error) {
	if page.AllValid {
		return nil, 0, nil
	}
	validBytes := vector.ValidityWords(int(page.Rows)) * 8
	if len(payload) < validBytes {
		return nil, 0, fmt.Errorf("column %q page payload is too short for validity", colName)
	}
	return payload[:validBytes], validBytes, nil
}

// decodeValidity reads the optional leading validity bitmap, returning nil for AllValid pages and the bytes consumed.
func decodeValidity(payload []byte, page PageMeta, colName string) (vector.Validity, int, error) {
	if page.AllValid {
		return nil, 0, nil
	}
	words := vector.ValidityWords(int(page.Rows))
	validBytes := words * 8
	if len(payload) < validBytes {
		return nil, 0, fmt.Errorf("column %q page payload is too short for validity", colName)
	}
	valid := make(vector.Validity, words)
	for i := range valid {
		valid[i] = binary.LittleEndian.Uint64(payload[i*8 : i*8+8])
	}
	return valid, validBytes, nil
}

func pageValidity(valid vector.Validity, start int, rows int) (vector.Validity, int) {
	if valid == nil {
		return nil, 0
	}
	var page vector.Validity
	nulls := 0
	for row := 0; row < rows; row++ {
		if !vector.IsValid(valid, start+row) {
			if page == nil {
				page = vector.NewValidity(rows)
			}
			vector.SetInvalid(page, row)
			nulls++
		}
	}
	if nulls == 0 {
		return nil, 0
	}
	return page, nulls
}

func validityPayloadLen(valid vector.Validity) int {
	if valid == nil {
		return 0
	}
	return len(valid) * 8
}

func writeValidityPayload(payload []byte, valid vector.Validity) int {
	pos := 0
	for _, word := range valid {
		binary.LittleEndian.PutUint64(payload[pos:pos+8], word)
		pos += 8
	}
	return pos
}

func boolAt(bits []uint64, row int) bool {
	return bits[row>>6]&(uint64(1)<<uint(row&63)) != 0
}

func setPayloadBit(payload []byte, row int) {
	wordOffset := (row >> 6) * 8
	word := binary.LittleEndian.Uint64(payload[wordOffset : wordOffset+8])
	word |= uint64(1) << uint(row&63)
	binary.LittleEndian.PutUint64(payload[wordOffset:wordOffset+8], word)
}
