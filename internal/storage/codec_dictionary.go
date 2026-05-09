package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// dictMaxValues is the cardinality threshold for picking dictionary
// encoding over plain varlen for a text page. 256 keeps every value ID in
// one byte, which is the densest representation we get without bitpacking.
// Wider tables (uint16 IDs) are deferred until there's evidence they pay
// off; the bench data so far only stresses ≤ 8 distinct values.
const dictMaxValues = 256

// shouldDictionaryEncodeText decides whether to dict-encode a text page. A
// nil/zero stats pointer or a distinct count above the threshold falls back
// to plain. This is a heuristic, not a guarantee: callers must still call
// the plain encoder if dictionary encoding fails (e.g. unexpectedly more
// distinct values than the histogram suggested).
func shouldDictionaryEncodeText(stats *TextStats) bool {
	if stats == nil {
		return false
	}
	distinct := len(stats.Values)
	return stats.Complete && distinct > 0 && distinct <= dictMaxValues
}

// encodeTextPage picks dictionary or plain for a single text page based on
// projected payload size, encodes it, and stamps meta.Codec / meta.Text. Plain
// remains the fallback whenever stats are incomplete or the dictionary
// estimate isn't strictly smaller.
func encodeTextPage(col vector.Column, valid vector.Validity, start, rows int, meta *PageMeta) ([]byte, PageMeta, error) {
	stats := buildTextStats(col.V.Var, valid, start, rows)
	validBytes := validityPayloadLen(valid)
	if shouldDictionaryEncodeText(stats) {
		dictSize := dictionaryPayloadEstimate(stats, validBytes, rows)
		plainSize := plainTextSizeFromStats(stats, validBytes, rows)
		if dictSize < plainSize {
			payload, _, err := encodeTextDictionary(col, valid, start, rows)
			if err == nil {
				meta.Text = stats
				meta.Codec = CodecDictionary
				return payload, *meta, nil
			}
		}
	}
	payload, _, err := encodeVarBytesPlain(col, valid, start, rows, true)
	if err != nil {
		return nil, PageMeta{}, err
	}
	meta.Text = stats
	meta.Codec = CodecPlain
	return payload, *meta, nil
}

// encodeTextDictionary serializes a text page as: validity bitmap, uint16
// dict count, (count+1) uint32 offsets, dict data, then `rows` uint8 IDs.
// Returns the payload, the page-level TextStats (so segment metadata still
// records exact value/count info), and any error.
func encodeTextDictionary(col vector.Column, valid vector.Validity, start, rows int) ([]byte, *TextStats, error) {
	stats := buildTextStats(col.V.Var, valid, start, rows)
	if !shouldDictionaryEncodeText(stats) {
		return nil, stats, fmt.Errorf("text column %q does not fit dictionary threshold", col.Name)
	}

	dict := stats.Values
	if len(dict) > dictMaxValues {
		return nil, stats, fmt.Errorf("text column %q dictionary overflow", col.Name)
	}
	idByValue := make(map[string]uint8, len(dict))
	for i, v := range dict {
		idByValue[v.Value] = uint8(i)
	}
	dataLen := 0
	for _, v := range dict {
		dataLen += len(v.Value)
	}

	dictHeader := 2 + (len(dict)+1)*4
	idLen := rows
	payload := make([]byte, validityPayloadLen(valid)+dictHeader+dataLen+idLen)
	pos := writeValidityPayload(payload, valid)

	binary.LittleEndian.PutUint16(payload[pos:pos+2], uint16(len(dict)))
	pos += 2
	offsetPos := pos
	dataStart := pos + (len(dict)+1)*4
	dataPos := dataStart
	binary.LittleEndian.PutUint32(payload[offsetPos:offsetPos+4], 0)
	for i, v := range dict {
		dataPos += copy(payload[dataPos:], v.Value)
		binary.LittleEndian.PutUint32(payload[offsetPos+(i+1)*4:offsetPos+(i+2)*4], uint32(dataPos-dataStart))
	}
	pos = dataStart + dataLen

	for row := 0; row < rows; row++ {
		if !vector.IsValid(valid, row) {
			payload[pos+row] = 0
			continue
		}
		v := col.V.Var.Bytes(start + row)
		id, ok := idByValue[string(v)]
		if !ok {
			return nil, stats, fmt.Errorf("text column %q row %d not in dictionary", col.Name, row)
		}
		payload[pos+row] = id
	}
	return payload, stats, nil
}

// decodeTextDictionary inverts encodeTextDictionary, materializing a
// regular varbytes vector so query paths see the same shape as plain text.
// Predicate / group / count fast-paths over dict IDs are deferred.
func decodeTextDictionary(payload []byte, valid vector.Validity, pos, rows int, kind vector.Kind, colName string) (vector.Vec, error) {
	if len(payload)-pos < 2 {
		return vector.Vec{}, fmt.Errorf("column %q dictionary payload missing dict count", colName)
	}
	dictCount := int(binary.LittleEndian.Uint16(payload[pos : pos+2]))
	pos += 2
	offsetBytes := (dictCount + 1) * 4
	if len(payload)-pos < offsetBytes {
		return vector.Vec{}, fmt.Errorf("column %q dictionary payload missing offsets", colName)
	}
	offsets := make([]uint32, dictCount+1)
	for i := range offsets {
		offsets[i] = binary.LittleEndian.Uint32(payload[pos : pos+4])
		pos += 4
	}
	dataLen := int(offsets[dictCount])
	if len(payload)-pos < dataLen+rows {
		return vector.Vec{}, fmt.Errorf("column %q dictionary payload truncated", colName)
	}
	dictData := payload[pos : pos+dataLen]
	pos += dataLen
	ids := payload[pos : pos+rows]

	outOffsets := make([]uint32, rows+1)
	totalLen := 0
	for row := 0; row < rows; row++ {
		if vector.IsValid(valid, row) {
			id := int(ids[row])
			if id < 0 || id >= dictCount {
				return vector.Vec{}, fmt.Errorf("column %q dictionary id %d out of range", colName, id)
			}
			totalLen += int(offsets[id+1] - offsets[id])
		}
		outOffsets[row+1] = uint32(totalLen)
	}
	outData := make([]byte, totalLen)
	cursor := 0
	for row := 0; row < rows; row++ {
		if !vector.IsValid(valid, row) {
			continue
		}
		id := int(ids[row])
		valueStart := offsets[id]
		valueEnd := offsets[id+1]
		copy(outData[cursor:], dictData[valueStart:valueEnd])
		cursor += int(valueEnd - valueStart)
	}
	return vector.Vec{Kind: kind, Len: rows, Valid: valid, Var: vector.VarBytes{Offsets: outOffsets, Data: outData}}, nil
}

// dictionaryPayloadEstimate returns the byte size encodeTextDictionary
// would produce for the given stats + row count. Used by codec selection
// to compare against the plain varlen size before committing.
func dictionaryPayloadEstimate(stats *TextStats, validBytes, rows int) int {
	if stats == nil {
		return 0
	}
	dataLen := 0
	for _, v := range stats.Values {
		dataLen += len(v.Value)
	}
	return validBytes + 2 + (len(stats.Values)+1)*4 + dataLen + rows
}

// plainTextSizeFromStats predicts encodeVarBytesPlain's payload size from
// complete stats (validBytes + per-row offsets + total bytes).
func plainTextSizeFromStats(stats *TextStats, validBytes, rows int) int {
	if stats == nil || !stats.Complete {
		return 0
	}
	dataLen := 0
	for _, v := range stats.Values {
		dataLen += len(v.Value) * int(v.Count)
	}
	return validBytes + (rows+1)*4 + dataLen
}
