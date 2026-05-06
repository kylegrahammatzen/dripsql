package storage

import (
	"encoding/binary"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

const (
	int64Size                = 8
	int64SequencePayloadLen  = 2 * int64Size
	int64DictionaryHeaderLen = 4 + 1
)

func encodeInt64(values vector.Int64) ([]byte, Codec, int, error) {
	rows := values.Len()
	plainLen, err := int64PlainPayloadLen(rows)
	if err != nil {
		return nil, 0, 0, err
	}
	if rows > 0 {
		payload, ok, err := encodeInt64SequenceIfSmaller(values, plainLen)
		if err != nil {
			return nil, 0, 0, err
		}
		if ok {
			return payload, CodecInt64Sequence, 0, nil
		}
		payload, metadataLen, ok, err := encodeInt64DictionaryIfSmaller(values, plainLen)
		if err != nil {
			return nil, 0, 0, err
		}
		if ok {
			return payload, CodecDictionary, metadataLen, nil
		}
	}
	payload, err := encodeInt64Plain(values)
	return payload, CodecPlain, 0, err
}

func encodeInt64SequenceIfSmaller(values vector.Int64, plainLen int) ([]byte, bool, error) {
	if int64SequencePayloadLen >= plainLen {
		return nil, false, nil
	}
	base, step, ok := analyzeInt64Sequence(values)
	if !ok {
		return nil, false, nil
	}
	out := make([]byte, int64SequencePayloadLen)
	binary.LittleEndian.PutUint64(out, uint64(base))
	binary.LittleEndian.PutUint64(out[int64Size:], uint64(step))
	return out, true, nil
}

func analyzeInt64Sequence(values vector.Int64) (base int64, step int64, ok bool) {
	rows := values.Len()
	if rows == 0 {
		return 0, 0, false
	}
	base = values.Values[0]
	if rows == 1 {
		return base, 0, true
	}
	step, ok = checkedSubInt64Signed(values.Values[1], base)
	if !ok {
		return 0, 0, false
	}
	prev := values.Values[1]
	for row := 2; row < rows; row++ {
		want, ok := checkedAddInt64Signed(prev, step)
		if !ok || values.Values[row] != want {
			return 0, 0, false
		}
		prev = want
	}
	return base, step, true
}

func encodeInt64Plain(values vector.Int64) ([]byte, error) {
	rows := values.Len()
	if rows == 0 {
		return []byte{}, nil
	}
	size, err := int64PlainPayloadLen(rows)
	if err != nil {
		return nil, err
	}
	out := make([]byte, size)
	copyInt64ToBytes(out, values.Values)
	return out, nil
}

func encodeInt64DictionaryIfSmaller(values vector.Int64, plainLen int) ([]byte, int, bool, error) {
	dictValues, rowIDs, dictCounts, ok := analyzeInt64Dictionary(values)
	if !ok {
		return nil, 0, false, nil
	}
	rows := values.Len()
	dictCount := len(dictValues)
	idEncoding := dictionaryIDEncoding(dictCount, rows)
	metadataLen, err := int64DictionaryMetadataLen(dictCount)
	if err != nil {
		return nil, 0, false, err
	}
	dictLen, err := int64DictionaryPayloadLen(dictCount, rows, idEncoding)
	if err != nil {
		return nil, 0, false, err
	}
	if dictLen >= plainLen {
		return nil, 0, false, nil
	}
	payload, err := encodeInt64Dictionary(rowIDs, dictValues, dictCounts, idEncoding, dictLen)
	if err != nil {
		return nil, 0, false, err
	}
	return payload, metadataLen, true, nil
}

func analyzeInt64Dictionary(values vector.Int64) (dictValues []int64, rowIDs []uint32, dictCounts []uint64, ok bool) {
	if !shouldAnalyzeInt64Dictionary(values) {
		return nil, nil, nil, false
	}
	rows := values.Len()
	dictCap := min(rows, maxLinearDictionaryValues)
	dictValues = make([]int64, 0, dictCap)
	dictCounts = make([]uint64, 0, dictCap)
	rowIDs = make([]uint32, rows)
	var dictIDs map[int64]uint32
	for row, value := range values.Values {
		id, found := int64DictionaryIDLinear(value, dictValues, dictIDs)
		if found {
			dictCounts[id]++
			rowIDs[row] = id
			continue
		}
		if len(dictValues) >= maxDictionaryValues {
			return nil, nil, nil, false
		}
		if dictIDs == nil && len(dictValues) >= maxLinearDictionaryValues {
			dictIDs = make(map[int64]uint32, min(rows, 1024))
			for dictID, dictValue := range dictValues {
				dictIDs[dictValue] = uint32(dictID)
			}
		}
		id = uint32(len(dictValues))
		if dictIDs != nil {
			dictIDs[value] = id
		}
		dictValues = append(dictValues, value)
		dictCounts = append(dictCounts, 1)
		rowIDs[row] = id
	}
	return dictValues, rowIDs, dictCounts, len(dictValues) > 0
}

func shouldAnalyzeInt64Dictionary(values vector.Int64) bool {
	count := min(values.Len(), dictionarySampleRows)
	if count < dictionarySampleMinRows {
		return true
	}
	seen := make(map[int64]struct{}, min(count, 1024))
	limit := count * dictionarySampleMaxDistinctPercent / 100
	for _, value := range values.Values[:count] {
		seen[value] = struct{}{}
		if len(seen) > limit {
			return false
		}
	}
	return true
}

func int64DictionaryIDLinear(value int64, dictValues []int64, dictIDs map[int64]uint32) (uint32, bool) {
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

func encodeInt64Dictionary(rowIDs []uint32, dictValues []int64, dictCounts []uint64, idEncoding int, size int) ([]byte, error) {
	if len(dictCounts) != len(dictValues) {
		return nil, fmt.Errorf("int64 dictionary count metadata has %d values, want %d", len(dictCounts), len(dictValues))
	}
	out := make([]byte, size)
	offset := 0
	binary.LittleEndian.PutUint32(out[offset:], uint32(len(dictValues)))
	offset += 4
	out[offset] = byte(idEncoding)
	offset++
	for _, value := range dictValues {
		binary.LittleEndian.PutUint64(out[offset:], uint64(value))
		offset += int64Size
	}
	for _, count := range dictCounts {
		binary.LittleEndian.PutUint64(out[offset:], count)
		offset += int64Size
	}
	if idEncoding&dictionaryPackedIDFlag != 0 {
		idsLen, err := dictionaryIDsLen(len(rowIDs), idEncoding)
		if err != nil {
			return nil, err
		}
		if offset+idsLen != size {
			return nil, fmt.Errorf("encoded int64 dictionary has %d metadata+id bytes, want %d", offset+idsLen, size)
		}
		ids := out[offset:]
		bitWidth := idEncoding &^ dictionaryPackedIDFlag
		for row, id := range rowIDs {
			setPackedDictionaryID(ids, row, bitWidth, id)
		}
		return out, nil
	}
	dst := out[:offset]
	for _, id := range rowIDs {
		dst = appendDictionaryID(dst, id, idEncoding)
	}
	if len(dst) != size {
		return nil, fmt.Errorf("encoded int64 dictionary has %d bytes, want %d", len(dst), size)
	}
	return dst, nil
}

func int64PlainPayloadLen(rows int) (int, error) {
	return checkedMulInt("int64 payload length", rows, int64Size)
}

func int64DictionaryPayloadLen(dictCount int, rows int, idEncoding int) (int, error) {
	metadataLen, err := int64DictionaryMetadataLen(dictCount)
	if err != nil {
		return 0, err
	}
	idsLen, err := dictionaryIDsLen(rows, idEncoding)
	if err != nil {
		return 0, err
	}
	return checkedAddInt("int64 dictionary payload length", metadataLen, idsLen)
}

func int64DictionaryMetadataLen(dictCount int) (int, error) {
	dictLen, err := checkedMulInt("int64 dictionary values length", dictCount, int64Size)
	if err != nil {
		return 0, err
	}
	countsLen, err := dictionaryCountsLen(dictCount)
	if err != nil {
		return 0, err
	}
	size, err := checkedAddInt("int64 dictionary metadata length", int64DictionaryHeaderLen, dictLen)
	if err != nil {
		return 0, err
	}
	return checkedAddInt("int64 dictionary metadata length", size, countsLen)
}

func decodeInt64Plain(payload []byte, rows int) (vector.Int64, error) {
	expected, err := int64PlainPayloadLen(rows)
	if err != nil {
		return vector.Int64{}, err
	}
	if len(payload) != expected {
		return vector.Int64{}, fmt.Errorf("plain int64 payload has %d bytes, want %d", len(payload), expected)
	}
	if rows == 0 {
		return vector.FromInt64(nil), nil
	}
	values := make([]int64, rows)
	copyBytesToInt64(values, payload)
	return vector.FromInt64(values), nil
}

func decodeInt64Payload(codec Codec, payload []byte, rows int) (vector.Int64, error) {
	switch codec {
	case CodecPlain:
		return decodeInt64Plain(payload, rows)
	case CodecDictionary:
		return decodeInt64Dictionary(payload, rows)
	case CodecInt64Sequence:
		return decodeInt64Sequence(payload, rows)
	default:
		return vector.Int64{}, fmt.Errorf("unsupported int64 codec %s", codec)
	}
}

func decodeInt64Sequence(payload []byte, rows int) (vector.Int64, error) {
	base, step, err := parseInt64SequencePayload(payload, rows)
	if err != nil {
		return vector.Int64{}, err
	}
	values := make([]int64, rows)
	value := base
	for row := range values {
		values[row] = value
		if row+1 < rows {
			var ok bool
			value, ok = checkedAddInt64Signed(value, step)
			if !ok {
				return vector.Int64{}, fmt.Errorf("int64 sequence overflows at row %d", row+1)
			}
		}
	}
	return vector.FromInt64(values), nil
}

func parseInt64SequencePayload(payload []byte, rows int) (base int64, step int64, err error) {
	if rows <= 0 {
		return 0, 0, fmt.Errorf("sequence int64 row count %d is not positive", rows)
	}
	if len(payload) != int64SequencePayloadLen {
		return 0, 0, fmt.Errorf("sequence int64 payload has %d bytes, want %d", len(payload), int64SequencePayloadLen)
	}
	base = int64(binary.LittleEndian.Uint64(payload))
	step = int64(binary.LittleEndian.Uint64(payload[int64Size:]))
	return base, step, nil
}

func countInt64SequenceEqualPayload(payload []byte, rows int, value int64) (int, error) {
	base, step, err := parseInt64SequencePayload(payload, rows)
	if err != nil {
		return 0, err
	}
	if step == 0 {
		if value == base {
			return rows, nil
		}
		return 0, nil
	}
	delta, ok := checkedSubInt64Signed(value, base)
	if !ok || delta%step != 0 {
		return 0, nil
	}
	row := delta / step
	if row < 0 || row >= int64(rows) {
		return 0, nil
	}
	return 1, nil
}

func checkedAddInt64Signed(a int64, b int64) (int64, bool) {
	sum := a + b
	if ((a ^ sum) & (b ^ sum)) < 0 {
		return 0, false
	}
	return sum, true
}

func checkedSubInt64Signed(a int64, b int64) (int64, bool) {
	diff := a - b
	if ((a ^ b) & (a ^ diff)) < 0 {
		return 0, false
	}
	return diff, true
}

func decodeInt64Dictionary(payload []byte, rows int) (vector.Int64, error) {
	dict, _, ids, idEncoding, dictCount, err := parseInt64DictionaryPayload(payload, rows)
	if err != nil {
		return vector.Int64{}, err
	}
	dictValues := make([]int64, dictCount)
	for id := range dictValues {
		dictValues[id] = int64(binary.LittleEndian.Uint64(dict[id*int64Size:]))
	}
	values := make([]int64, rows)
	for row := range values {
		id, err := dictionaryIDAt(ids, row, idEncoding, dictCount, "int64")
		if err != nil {
			return vector.Int64{}, err
		}
		values[row] = dictValues[int(id)]
	}
	return vector.FromInt64(values), nil
}

func parseInt64DictionaryPayload(payload []byte, rows int) (dict []byte, counts []byte, ids []byte, idEncoding int, dictCount int, err error) {
	if len(payload) < int64DictionaryHeaderLen {
		return nil, nil, nil, 0, 0, fmt.Errorf("short int64 dictionary header")
	}
	dictCount, err = checkedInt("int64 dictionary count", uint64(binary.LittleEndian.Uint32(payload)))
	if err != nil {
		return nil, nil, nil, 0, 0, err
	}
	idEncoding = int(payload[4])
	if err := validateDictionaryIDEncoding("int64", rows, dictCount, idEncoding); err != nil {
		return nil, nil, nil, 0, 0, err
	}
	metadataLen, err := int64DictionaryMetadataLen(dictCount)
	if err != nil {
		return nil, nil, nil, 0, 0, err
	}
	idsLen, err := dictionaryIDsLen(rows, idEncoding)
	if err != nil {
		return nil, nil, nil, 0, 0, err
	}
	payloadLen, err := checkedAddInt("int64 dictionary payload length", metadataLen, idsLen)
	if err != nil {
		return nil, nil, nil, 0, 0, err
	}
	if len(payload) != payloadLen {
		return nil, nil, nil, 0, 0, fmt.Errorf("int64 dictionary payload has %d bytes, want %d", len(payload), payloadLen)
	}
	dictLen, err := checkedMulInt("int64 dictionary values length", dictCount, int64Size)
	if err != nil {
		return nil, nil, nil, 0, 0, err
	}
	dictStart := int64DictionaryHeaderLen
	countsStart := dictStart + dictLen
	idsStart := metadataLen
	dict = payload[dictStart:countsStart]
	counts = payload[countsStart:idsStart]
	if err := validateDictionaryCounts("int64", counts, dictCount, rows); err != nil {
		return nil, nil, nil, 0, 0, err
	}
	return dict, counts, payload[idsStart:], idEncoding, dictCount, nil
}

func countInt64DictionaryEqualMetadata(metadata []byte, rows int, value int64) (int, error) {
	if len(metadata) < int64DictionaryHeaderLen {
		return 0, fmt.Errorf("short int64 dictionary header")
	}
	dictCount, err := checkedInt("int64 dictionary count", uint64(binary.LittleEndian.Uint32(metadata)))
	if err != nil {
		return 0, err
	}
	idEncoding := int(metadata[4])
	if err := validateDictionaryIDEncoding("int64", rows, dictCount, idEncoding); err != nil {
		return 0, err
	}
	metadataLen, err := int64DictionaryMetadataLen(dictCount)
	if err != nil {
		return 0, err
	}
	if len(metadata) != metadataLen {
		return 0, fmt.Errorf("int64 dictionary metadata has %d bytes, want %d", len(metadata), metadataLen)
	}
	dictLen, err := checkedMulInt("int64 dictionary values length", dictCount, int64Size)
	if err != nil {
		return 0, err
	}
	dict := metadata[int64DictionaryHeaderLen : int64DictionaryHeaderLen+dictLen]
	counts := metadata[int64DictionaryHeaderLen+dictLen:]
	if err := validateDictionaryCounts("int64", counts, dictCount, rows); err != nil {
		return 0, err
	}
	for id := 0; id < dictCount; id++ {
		if int64(binary.LittleEndian.Uint64(dict[id*int64Size:])) == value {
			count, err := checkedInt("int64 dictionary count", dictionaryCountAt(counts, id))
			if err != nil {
				return 0, err
			}
			return count, nil
		}
	}
	return 0, nil
}
