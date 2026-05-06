package storage

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/util"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

const (
	stringOffsetWidth         = 4
	stringDictionaryHeaderLen = 4 + 1 + 8
	stringPrefixHeaderLen     = 4 + 4 + 8
	stringTemplateHeaderLen   = 4 + 4 + 4 + 1 + 1 + 1 + 1 + 8 + 8
)

func encodeString(values vector.String) (columnEncoding, error) {
	strings, offsetBytes, dataLen, err := collectStringValues(values)
	if err != nil {
		return columnEncoding{}, err
	}
	plainLen, err := checkedAddInt("string total payload length", offsetBytes, dataLen)
	if err != nil {
		return columnEncoding{}, err
	}
	if values.Len() > 0 {
		payload, metadataLen, dictCount, idEncoding, ok, err := encodeStringDictionaryIfSmaller(values, plainLen)
		if err != nil {
			return columnEncoding{}, err
		}
		if ok {
			return columnEncoding{payload: payload, codec: CodecDictionary, plainBytes: plainLen, dictionaryBytes: metadataLen, dictionaryValues: dictCount, dictionaryIDEncoding: idEncoding}, nil
		}
	}
	if values.Len() > 0 {
		payload, ok, err := encodeStringTemplateIfSmaller(strings, plainLen)
		if err != nil {
			return columnEncoding{}, err
		}
		if ok {
			return columnEncoding{payload: payload, codec: CodecStringTemplate, plainBytes: plainLen}, nil
		}
	}
	if values.Len() > 0 {
		payload, ok, err := encodeStringPrefixIfSmaller(strings, plainLen)
		if err != nil {
			return columnEncoding{}, err
		}
		if ok {
			return columnEncoding{payload: payload, codec: CodecStringPrefix, plainBytes: plainLen}, nil
		}
	}
	payload, err := encodeStringPlainValues(strings, offsetBytes, dataLen)
	if err != nil {
		return columnEncoding{}, err
	}
	return columnEncoding{payload: payload, codec: CodecPlain, plainBytes: plainLen}, nil
}

func encodeStringPlain(values vector.String) ([]byte, error) {
	strings, offsetBytes, dataLen, err := collectStringValues(values)
	if err != nil {
		return nil, err
	}
	return encodeStringPlainValues(strings, offsetBytes, dataLen)
}

func encodeStringPlainValues(strings []string, offsetBytes int, dataLen int) ([]byte, error) {
	totalBytes, err := checkedAddInt("string total payload length", offsetBytes, dataLen)
	if err != nil {
		return nil, err
	}

	out := make([]byte, totalBytes)
	cursor := 0
	for row, value := range strings {
		binary.LittleEndian.PutUint32(out[row*stringOffsetWidth:], uint32(cursor))
		copy(out[offsetBytes+cursor:], value)
		cursor += len(value)
	}
	binary.LittleEndian.PutUint32(out[len(strings)*stringOffsetWidth:], uint32(cursor))
	return out, nil
}

func encodeStringDictionaryIfSmaller(values vector.String, plainLen int) ([]byte, int, int, int, bool, error) {
	dictValues, dictIDs, dictDataLen, dictCounts, ok, err := analyzeStringDictionary(values)
	if err != nil || !ok {
		return nil, 0, 0, 0, false, err
	}
	idEncoding := dictionaryIDEncoding(len(dictValues), values.Len())
	dictSectionLen, err := stringDictionarySectionLen(len(dictValues), dictDataLen)
	if err != nil {
		return nil, 0, 0, 0, false, err
	}
	metadataLen, err := stringDictionaryMetadataLen(dictSectionLen, len(dictValues))
	if err != nil {
		return nil, 0, 0, 0, false, err
	}
	dictLen, err := stringDictionaryPayloadLen(dictSectionLen, len(dictValues), values.Len(), idEncoding)
	if err != nil {
		return nil, 0, 0, 0, false, err
	}
	if dictLen >= plainLen {
		return nil, 0, 0, 0, false, nil
	}
	payload, err := encodeStringDictionary(values, dictValues, dictIDs, dictDataLen, dictCounts, idEncoding, dictLen)
	if err != nil {
		return nil, 0, 0, 0, false, err
	}
	return payload, metadataLen, len(dictValues), idEncoding, true, nil
}

func encodeStringPrefixIfSmaller(values []string, plainLen int) ([]byte, bool, error) {
	prefixLen := commonStringPrefixLen(values)
	suffixLen := commonStringSuffixLen(values, prefixLen)
	sharedLen := prefixLen + suffixLen
	if sharedLen == 0 {
		return nil, false, nil
	}
	middleDataLen := 0
	for _, value := range values {
		middleLen := len(value) - sharedLen
		if middleLen < 0 {
			return nil, false, nil
		}
		var err error
		middleDataLen, err = checkedAddInt("string prefix middle data length", middleDataLen, middleLen)
		if err != nil {
			return nil, false, err
		}
	}
	offsetBytes, err := checkedMulInt("string prefix offset payload length", len(values)+1, stringOffsetWidth)
	if err != nil {
		return nil, false, err
	}
	middleSectionLen, err := checkedAddInt("string prefix middle section length", offsetBytes, middleDataLen)
	if err != nil {
		return nil, false, err
	}
	metadataLen, err := checkedAddInt("string prefix metadata length", stringPrefixHeaderLen, sharedLen)
	if err != nil {
		return nil, false, err
	}
	totalBytes, err := checkedAddInt("string prefix payload length", metadataLen, middleSectionLen)
	if err != nil {
		return nil, false, err
	}
	if totalBytes >= plainLen || uint64(middleDataLen) > uint64(^uint32(0)) {
		return nil, false, nil
	}
	out := make([]byte, totalBytes)
	binary.LittleEndian.PutUint32(out, uint32(prefixLen))
	binary.LittleEndian.PutUint32(out[4:], uint32(suffixLen))
	binary.LittleEndian.PutUint64(out[8:], uint64(middleSectionLen))
	offset := stringPrefixHeaderLen
	prefix := values[0][:prefixLen]
	suffix := values[0][len(values[0])-suffixLen:]
	copy(out[offset:], prefix)
	offset += prefixLen
	copy(out[offset:], suffix)
	offset += suffixLen
	middle := out[offset:]
	cursor := 0
	for row, value := range values {
		binary.LittleEndian.PutUint32(middle[row*stringOffsetWidth:], uint32(cursor))
		middleValue := value[prefixLen : len(value)-suffixLen]
		copy(middle[offsetBytes+cursor:], middleValue)
		cursor += len(middleValue)
	}
	binary.LittleEndian.PutUint32(middle[len(values)*stringOffsetWidth:], uint32(cursor))
	return out, true, nil
}

type stringTemplateSpan struct {
	start int
	end   int
}

func encodeStringTemplateIfSmaller(values []string, plainLen int) ([]byte, bool, error) {
	if len(values) == 0 {
		return nil, false, nil
	}
	spans := stringTemplateCandidateSpans(values[0])
	if len(spans) < 2 {
		return nil, false, nil
	}
	sample := stringTemplateSample(values)
	best := []byte(nil)
	for firstID := 0; firstID < len(spans); firstID++ {
		for secondID := firstID + 1; secondID < len(spans); secondID++ {
			first := spans[firstID]
			second := spans[secondID]
			middle := values[0][first.end:second.start]
			if middle == "" {
				continue
			}
			prefix := values[0][:first.start]
			suffix := values[0][second.end:]
			for _, firstRadix := range stringTemplateRadices(values[0][first.start:first.end]) {
				for _, secondRadix := range stringTemplateRadices(values[0][second.start:second.end]) {
					if sample != nil {
						if _, _, ok := parseStringTemplateValues(sample, prefix, middle, suffix, firstRadix, secondRadix); !ok {
							continue
						}
					}
					firstValues, secondValues, ok := parseStringTemplateValues(values, prefix, middle, suffix, firstRadix, secondRadix)
					if !ok {
						continue
					}
					if !stringTemplateValuesVary(firstValues) || !stringTemplateValuesVary(secondValues) {
						continue
					}
					payload, err := encodeStringTemplate(prefix, middle, suffix, firstRadix, secondRadix, firstValues, secondValues)
					if err != nil {
						return nil, false, err
					}
					if len(payload) >= plainLen {
						continue
					}
					if best == nil || len(payload) < len(best) {
						best = payload
					}
				}
			}
		}
	}
	if best == nil {
		return nil, false, nil
	}
	return best, true, nil
}

func stringTemplateSample(values []string) []string {
	const sampleSize = 128
	if len(values) <= sampleSize {
		return nil
	}
	sample := make([]string, sampleSize)
	last := len(values) - 1
	for i := range sample {
		sample[i] = values[i*last/(sampleSize-1)]
	}
	return sample
}

func stringTemplateCandidateSpans(value string) []stringTemplateSpan {
	spans := make([]stringTemplateSpan, 0, 4)
	start := -1
	for i := 0; i < len(value); i++ {
		if isStringTemplateNumberByte(value[i]) {
			if start < 0 {
				start = i
			}
			continue
		}
		if start >= 0 {
			spans = append(spans, stringTemplateSpan{start: start, end: i})
			start = -1
		}
	}
	if start >= 0 {
		spans = append(spans, stringTemplateSpan{start: start, end: len(value)})
	}
	return spans
}

func isStringTemplateNumberByte(b byte) bool {
	return ('0' <= b && b <= '9') || ('a' <= b && b <= 'z')
}

func stringTemplateRadices(value string) []int {
	if value == "" {
		return nil
	}
	decimal := true
	base36 := true
	hasDigit := false
	for i := 0; i < len(value); i++ {
		b := value[i]
		if '0' <= b && b <= '9' {
			hasDigit = true
		}
		if b < '0' || b > '9' {
			decimal = false
		}
		if !isStringTemplateNumberByte(b) {
			base36 = false
		}
	}
	if !hasDigit {
		return nil
	}
	switch {
	case decimal && base36:
		return []int{10, 36}
	case decimal:
		return []int{10}
	case base36:
		return []int{36}
	default:
		return nil
	}
}

func stringTemplateValuesVary(values []int64) bool {
	if len(values) < 2 {
		return false
	}
	first := values[0]
	for _, value := range values[1:] {
		if value != first {
			return true
		}
	}
	return false
}

func parseStringTemplateValues(values []string, prefix string, middle string, suffix string, firstRadix int, secondRadix int) ([]int64, []int64, bool) {
	firstValues := make([]int64, len(values))
	secondValues := make([]int64, len(values))
	for row, value := range values {
		firstText, secondText, ok := splitStringTemplateValue(value, prefix, middle, suffix)
		if !ok {
			return nil, nil, false
		}
		firstValue, ok := parseStringTemplateNumber(firstText, firstRadix)
		if !ok {
			return nil, nil, false
		}
		secondValue, ok := parseStringTemplateNumber(secondText, secondRadix)
		if !ok {
			return nil, nil, false
		}
		if !stringTemplateNumberMatches(firstText, firstValue, firstRadix) || !stringTemplateNumberMatches(secondText, secondValue, secondRadix) {
			return nil, nil, false
		}
		firstValues[row] = firstValue
		secondValues[row] = secondValue
	}
	return firstValues, secondValues, true
}

func splitStringTemplateValue(value string, prefix string, middle string, suffix string) (string, string, bool) {
	if !strings.HasPrefix(value, prefix) || !strings.HasSuffix(value, suffix) {
		return "", "", false
	}
	body := value[len(prefix) : len(value)-len(suffix)]
	middleAt := strings.Index(body, middle)
	if middleAt < 0 {
		return "", "", false
	}
	return body[:middleAt], body[middleAt+len(middle):], true
}

func parseStringTemplateNumber(value string, radix int) (int64, bool) {
	if value == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(value, radix, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

func stringTemplateNumberMatches(value string, n int64, radix int) bool {
	var buf [binary.MaxVarintLen64 * 2]byte
	out := strconv.AppendInt(buf[:0], n, radix)
	return util.BytesEqualString(out, value)
}

func encodeStringTemplate(prefix string, middle string, suffix string, firstRadix int, secondRadix int, firstValues []int64, secondValues []int64) ([]byte, error) {
	if len(firstValues) != len(secondValues) {
		return nil, fmt.Errorf("string template has %d first values, want %d", len(firstValues), len(secondValues))
	}
	firstEncoding, err := encodeInt64(vector.FromInt64(firstValues))
	if err != nil {
		return nil, err
	}
	secondEncoding, err := encodeInt64(vector.FromInt64(secondValues))
	if err != nil {
		return nil, err
	}
	firstPayload := firstEncoding.payload
	secondPayload := secondEncoding.payload
	firstCodec := firstEncoding.codec
	secondCodec := secondEncoding.codec
	if firstCodec == CodecStringTemplate || secondCodec == CodecStringTemplate || firstCodec == CodecStringPrefix || secondCodec == CodecStringPrefix {
		return nil, fmt.Errorf("string template nested non-int64 codec")
	}
	literalsLen, err := checkedAddInt("string template literal length", len(prefix), len(middle))
	if err != nil {
		return nil, err
	}
	literalsLen, err = checkedAddInt("string template literal length", literalsLen, len(suffix))
	if err != nil {
		return nil, err
	}
	firstEnd, err := checkedAddInt("string template payload length", stringTemplateHeaderLen, literalsLen)
	if err != nil {
		return nil, err
	}
	secondStart, err := checkedAddInt("string template payload length", firstEnd, len(firstPayload))
	if err != nil {
		return nil, err
	}
	totalLen, err := checkedAddInt("string template payload length", secondStart, len(secondPayload))
	if err != nil {
		return nil, err
	}
	out := make([]byte, totalLen)
	binary.LittleEndian.PutUint32(out, uint32(len(prefix)))
	binary.LittleEndian.PutUint32(out[4:], uint32(len(middle)))
	binary.LittleEndian.PutUint32(out[8:], uint32(len(suffix)))
	out[12] = byte(firstRadix)
	out[13] = byte(secondRadix)
	out[14] = byte(firstCodec)
	out[15] = byte(secondCodec)
	binary.LittleEndian.PutUint64(out[16:], uint64(len(firstPayload)))
	binary.LittleEndian.PutUint64(out[24:], uint64(len(secondPayload)))
	offset := stringTemplateHeaderLen
	copy(out[offset:], prefix)
	offset += len(prefix)
	copy(out[offset:], middle)
	offset += len(middle)
	copy(out[offset:], suffix)
	offset += len(suffix)
	copy(out[offset:], firstPayload)
	copy(out[secondStart:], secondPayload)
	return out, nil
}

func commonStringPrefixLen(values []string) int {
	if len(values) == 0 {
		return 0
	}
	prefixLen := len(values[0])
	for _, value := range values[1:] {
		prefixLen = min(prefixLen, len(value))
		for i := 0; i < prefixLen; i++ {
			if values[0][i] != value[i] {
				prefixLen = i
				break
			}
		}
	}
	return prefixLen
}

func commonStringSuffixLen(values []string, prefixLen int) int {
	if len(values) == 0 {
		return 0
	}
	suffixLen := len(values[0]) - prefixLen
	for _, value := range values[1:] {
		suffixLen = min(suffixLen, len(value)-prefixLen)
		for i := 0; i < suffixLen; i++ {
			if values[0][len(values[0])-1-i] != value[len(value)-1-i] {
				suffixLen = i
				break
			}
		}
	}
	return suffixLen
}

func analyzeStringDictionary(values vector.String) (dictValues []string, dictIDs map[string]uint32, dictDataLen int, dictCounts []uint64, ok bool, err error) {
	if !shouldAnalyzeStringDictionary(values) {
		return nil, nil, 0, nil, false, nil
	}
	for row := 0; row < values.Len(); row++ {
		value := values.Value(row)
		id, found := stringDictionaryID(value, dictValues, dictIDs)
		if found {
			dictCounts[id]++
			continue
		}
		if dictIDs == nil && len(dictValues) >= maxLinearDictionaryValues {
			dictIDs = make(map[string]uint32, min(values.Len(), 1024))
			for dictID, dictValue := range dictValues {
				dictIDs[dictValue] = uint32(dictID)
			}
		}
		if len(dictValues) >= maxDictionaryValues {
			return nil, nil, 0, nil, false, nil
		}
		nextDictDataLen, err := checkedAddInt("string dictionary data length", dictDataLen, len(value))
		if err != nil {
			return nil, nil, 0, nil, false, err
		}
		if nextDictDataLen > maxStringDictionaryDataLen {
			return nil, nil, 0, nil, false, nil
		}
		id = uint32(len(dictValues))
		if dictIDs != nil {
			dictIDs[value] = id
		}
		dictValues = append(dictValues, value)
		dictCounts = append(dictCounts, 1)
		dictDataLen = nextDictDataLen
	}
	return dictValues, dictIDs, dictDataLen, dictCounts, len(dictValues) > 0, nil
}

func shouldAnalyzeStringDictionary(values vector.String) bool {
	count := min(values.Len(), dictionarySampleRows)
	if count < dictionarySampleMinRows {
		return true
	}
	seen := make(map[string]struct{}, min(count, 1024))
	limit := count * dictionarySampleMaxDistinctPercent / 100
	for row := 0; row < count; row++ {
		seen[values.Value(row)] = struct{}{}
		if len(seen) > limit {
			return false
		}
	}
	return true
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

func encodeStringDictionary(values vector.String, dictValues []string, dictIDs map[string]uint32, dictDataLen int, dictCounts []uint64, idEncoding int, size int) ([]byte, error) {
	if len(dictCounts) != len(dictValues) {
		return nil, fmt.Errorf("string dictionary count metadata has %d values, want %d", len(dictCounts), len(dictValues))
	}
	dictSectionLen, err := stringDictionarySectionLen(len(dictValues), dictDataLen)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, size)
	out = binary.LittleEndian.AppendUint32(out, uint32(len(dictValues)))
	out = append(out, byte(idEncoding))
	out = binary.LittleEndian.AppendUint64(out, uint64(dictSectionLen))
	dictOffsetBytes, err := checkedMulInt("string dictionary offset payload length", len(dictValues)+1, stringOffsetWidth)
	if err != nil {
		return nil, err
	}
	dictStart := len(out)
	out = append(out, make([]byte, dictSectionLen)...)
	cursor := 0
	for id, value := range dictValues {
		binary.LittleEndian.PutUint32(out[dictStart+id*stringOffsetWidth:], uint32(cursor))
		copy(out[dictStart+dictOffsetBytes+cursor:], value)
		cursor += len(value)
	}
	binary.LittleEndian.PutUint32(out[dictStart+len(dictValues)*stringOffsetWidth:], uint32(cursor))
	for _, count := range dictCounts {
		out = binary.LittleEndian.AppendUint64(out, count)
	}
	if idEncoding&dictionaryPackedIDFlag != 0 {
		idsLen, err := dictionaryIDsLen(values.Len(), idEncoding)
		if err != nil {
			return nil, err
		}
		idsOffset := len(out)
		out = append(out, make([]byte, idsLen)...)
		ids := out[idsOffset:]
		bitWidth := idEncoding &^ dictionaryPackedIDFlag
		for row := 0; row < values.Len(); row++ {
			id, ok := stringDictionaryID(values.Value(row), dictValues, dictIDs)
			if !ok {
				return nil, fmt.Errorf("missing string dictionary id at row %d", row)
			}
			setPackedDictionaryID(ids, row, bitWidth, id)
		}
		return out, nil
	}
	for row := 0; row < values.Len(); row++ {
		id, ok := stringDictionaryID(values.Value(row), dictValues, dictIDs)
		if !ok {
			return nil, fmt.Errorf("missing string dictionary id at row %d", row)
		}
		out = appendDictionaryID(out, id, idEncoding)
	}
	return out, nil
}

func collectStringValues(values vector.String) ([]string, int, int, error) {
	offsetBytes, err := checkedMulInt("string offset payload length", values.Len()+1, stringOffsetWidth)
	if err != nil {
		return nil, 0, 0, err
	}
	strings := make([]string, values.Len())
	dataLen := 0
	for row := 0; row < values.Len(); row++ {
		strings[row] = values.Value(row)
		dataLen, err = checkedAddInt("string data length", dataLen, len(strings[row]))
		if err != nil {
			return nil, 0, 0, err
		}
	}
	if uint64(dataLen) > uint64(^uint32(0)) {
		return nil, 0, 0, fmt.Errorf("string data length %d overflows uint32 offsets", dataLen)
	}
	return strings, offsetBytes, dataLen, nil
}

func stringPlainPayloadLen(values vector.String) (int, error) {
	_, offsetBytes, dataLen, err := collectStringValues(values)
	if err != nil {
		return 0, err
	}
	return checkedAddInt("string total payload length", offsetBytes, dataLen)
}

func stringDictionarySectionLen(dictCount int, dataLen int) (int, error) {
	if uint64(dataLen) > uint64(^uint32(0)) {
		return 0, fmt.Errorf("string dictionary data length %d overflows uint32 offsets", dataLen)
	}
	offsetBytes, err := checkedMulInt("string dictionary offset payload length", dictCount+1, stringOffsetWidth)
	if err != nil {
		return 0, err
	}
	return checkedAddInt("string dictionary section length", offsetBytes, dataLen)
}

func stringDictionaryPayloadLen(dictSectionLen int, dictCount int, rows int, idEncoding int) (int, error) {
	metadataLen, err := stringDictionaryMetadataLen(dictSectionLen, dictCount)
	if err != nil {
		return 0, err
	}
	idsLen, err := dictionaryIDsLen(rows, idEncoding)
	if err != nil {
		return 0, err
	}
	return checkedAddInt("string dictionary payload length", metadataLen, idsLen)
}

func stringDictionaryMetadataLen(dictSectionLen int, dictCount int) (int, error) {
	countsLen, err := dictionaryCountsLen(dictCount)
	if err != nil {
		return 0, err
	}
	size, err := checkedAddInt("string dictionary metadata length", stringDictionaryHeaderLen, dictSectionLen)
	if err != nil {
		return 0, err
	}
	return checkedAddInt("string dictionary metadata length", size, countsLen)
}

func decodeStringPlain(payload []byte, rows int, stable bool) (vector.String, error) {
	offsetBytes, err := checkedMulInt("string offset payload length", rows+1, stringOffsetWidth)
	if err != nil {
		return vector.String{}, err
	}
	if len(payload) < offsetBytes {
		return vector.String{}, fmt.Errorf("plain string payload has %d bytes, want at least %d", len(payload), offsetBytes)
	}
	if first := binary.LittleEndian.Uint32(payload); first != 0 {
		return vector.String{}, fmt.Errorf("first string offset is %d, want 0", first)
	}

	data := payload[offsetBytes:]
	dataLen := uint64(len(data))
	if dataLen > uint64(^uint32(0)) {
		return vector.String{}, fmt.Errorf("plain string data length %d overflows uint32 offsets", len(data))
	}
	if last := uint64(binary.LittleEndian.Uint32(payload[rows*stringOffsetWidth:])); last != dataLen {
		return vector.String{}, fmt.Errorf("last string offset is %d, want %d", last, dataLen)
	}

	for row := 0; row < rows; row++ {
		start := uint64(binary.LittleEndian.Uint32(payload[row*stringOffsetWidth:]))
		end := uint64(binary.LittleEndian.Uint32(payload[(row+1)*stringOffsetWidth:]))
		if start > end || end > dataLen {
			return vector.String{}, fmt.Errorf("string offsets at row %d are out of bounds", row)
		}
	}

	ranges := make([]uint64, rows)
	for row := 0; row < rows; row++ {
		start := binary.LittleEndian.Uint32(payload[row*stringOffsetWidth:])
		end := binary.LittleEndian.Uint32(payload[(row+1)*stringOffsetWidth:])
		ranges[row] = vector.StringRange(start, end-start)
	}
	if !stable {
		data = append([]byte(nil), data...)
	}
	return vector.FromStringDataUnsafe(data, ranges), nil
}

func decodeStringDictionary(payload []byte, rows int, stable bool) (vector.String, error) {
	data, dictRanges, counts, ids, idEncoding, dictCount, err := parseStringDictionaryPayload(payload, rows)
	if err != nil {
		return vector.String{}, err
	}
	if err := validateDictionaryCounts("string", counts, dictCount, rows); err != nil {
		return vector.String{}, err
	}
	ranges := make([]uint64, rows)
	for row := range ranges {
		id, err := dictionaryIDAt(ids, row, idEncoding, dictCount, "string")
		if err != nil {
			return vector.String{}, err
		}
		ranges[row] = dictRanges[id]
	}
	if !stable {
		data = append([]byte(nil), data...)
	}
	return vector.FromStringDataUnsafe(data, ranges), nil
}

func decodeStringPrefix(payload []byte, rows int) (vector.String, error) {
	prefix, suffix, middlePayload, err := parseStringPrefixPayload(payload, rows)
	if err != nil {
		return vector.String{}, err
	}
	middle, err := decodeStringPlain(middlePayload, rows, true)
	if err != nil {
		return vector.String{}, err
	}
	sharedLen := len(prefix) + len(suffix)
	dataLen := len(middle.Data)
	sharedBytes, err := checkedMulInt("string prefix shared bytes", rows, sharedLen)
	if err != nil {
		return vector.String{}, err
	}
	dataLen, err = checkedAddInt("string prefix decoded data length", dataLen, sharedBytes)
	if err != nil {
		return vector.String{}, err
	}
	if uint64(dataLen) > uint64(^uint32(0)) {
		return vector.String{}, fmt.Errorf("string prefix decoded data length %d overflows uint32 ranges", dataLen)
	}
	data := make([]byte, dataLen)
	ranges := make([]uint64, rows)
	cursor := 0
	for row := 0; row < rows; row++ {
		start := cursor
		copy(data[cursor:], prefix)
		cursor += len(prefix)
		middleRange := middle.Ranges[row]
		middleStart := int(middleRange >> 32)
		middleLen := int(uint32(middleRange))
		copy(data[cursor:], middle.Data[middleStart:middleStart+middleLen])
		cursor += middleLen
		copy(data[cursor:], suffix)
		cursor += len(suffix)
		ranges[row] = vector.StringRange(uint32(start), uint32(cursor-start))
	}
	return vector.FromStringDataUnsafe(data, ranges), nil
}

func decodeStringTemplate(payload []byte, rows int) (vector.String, error) {
	t, err := parseStringTemplatePayload(payload, rows)
	if err != nil {
		return vector.String{}, err
	}
	firstValues, err := decodeInt64Payload(t.firstCodec, t.firstPayload, rows)
	if err != nil {
		return vector.String{}, fmt.Errorf("decode first template field: %w", err)
	}
	secondValues, err := decodeInt64Payload(t.secondCodec, t.secondPayload, rows)
	if err != nil {
		return vector.String{}, fmt.Errorf("decode second template field: %w", err)
	}
	decodedBytes, err := stringTemplateDecodedDataLen(t, firstValues.Values, secondValues.Values)
	if err != nil {
		return vector.String{}, err
	}
	data := make([]byte, 0, decodedBytes)
	ranges := make([]uint64, rows)
	for row := 0; row < rows; row++ {
		start := len(data)
		data = append(data, t.prefix...)
		data = strconv.AppendInt(data, firstValues.Values[row], t.firstRadix)
		data = append(data, t.middle...)
		data = strconv.AppendInt(data, secondValues.Values[row], t.secondRadix)
		data = append(data, t.suffix...)
		if uint64(start) > uint64(^uint32(0)) || uint64(len(data)-start) > uint64(^uint32(0)) {
			return vector.String{}, fmt.Errorf("string template decoded data length overflows uint32 ranges")
		}
		ranges[row] = vector.StringRange(uint32(start), uint32(len(data)-start))
	}
	return vector.FromStringDataUnsafe(data, ranges), nil
}

func stringTemplateDecodedDataLen(t stringTemplatePayload, firstValues []int64, secondValues []int64) (int, error) {
	if len(firstValues) != len(secondValues) {
		return 0, fmt.Errorf("string template has %d first values, want %d", len(firstValues), len(secondValues))
	}
	literalLen, err := checkedAddInt("string template decoded literal length", len(t.prefix), len(t.middle))
	if err != nil {
		return 0, err
	}
	literalLen, err = checkedAddInt("string template decoded literal length", literalLen, len(t.suffix))
	if err != nil {
		return 0, err
	}
	total, err := checkedMulInt("string template decoded literal length", literalLen, len(firstValues))
	if err != nil {
		return 0, err
	}
	for row := range firstValues {
		total, err = checkedAddInt("string template decoded data length", total, stringTemplateIntTextLen(firstValues[row], t.firstRadix))
		if err != nil {
			return 0, err
		}
		total, err = checkedAddInt("string template decoded data length", total, stringTemplateIntTextLen(secondValues[row], t.secondRadix))
		if err != nil {
			return 0, err
		}
	}
	if uint64(total) > uint64(^uint32(0)) {
		return 0, fmt.Errorf("string template decoded data length overflows uint32 ranges")
	}
	return total, nil
}

func stringTemplateIntTextLen(value int64, radix int) int {
	if value == 0 {
		return 1
	}
	length := 0
	var n uint64
	if value < 0 {
		length = 1
		n = uint64(-(value + 1)) + 1
	} else {
		n = uint64(value)
	}
	base := uint64(radix)
	for n > 0 {
		length++
		n /= base
	}
	return length
}

type stringTemplatePayload struct {
	prefix        string
	middle        string
	suffix        string
	firstRadix    int
	secondRadix   int
	firstCodec    Codec
	secondCodec   Codec
	firstPayload  []byte
	secondPayload []byte
}

func parseStringTemplatePayload(payload []byte, rows int) (stringTemplatePayload, error) {
	if rows <= 0 {
		return stringTemplatePayload{}, fmt.Errorf("string template row count %d is not positive", rows)
	}
	if len(payload) < stringTemplateHeaderLen {
		return stringTemplatePayload{}, fmt.Errorf("short string template header")
	}
	prefixLen, err := checkedInt("string template prefix length", uint64(binary.LittleEndian.Uint32(payload)))
	if err != nil {
		return stringTemplatePayload{}, err
	}
	middleLen, err := checkedInt("string template middle length", uint64(binary.LittleEndian.Uint32(payload[4:])))
	if err != nil {
		return stringTemplatePayload{}, err
	}
	suffixLen, err := checkedInt("string template suffix length", uint64(binary.LittleEndian.Uint32(payload[8:])))
	if err != nil {
		return stringTemplatePayload{}, err
	}
	firstRadix := int(payload[12])
	secondRadix := int(payload[13])
	if !validStringTemplateRadix(firstRadix) || !validStringTemplateRadix(secondRadix) {
		return stringTemplatePayload{}, fmt.Errorf("invalid string template radices %d/%d", firstRadix, secondRadix)
	}
	firstCodec := Codec(payload[14])
	secondCodec := Codec(payload[15])
	if !validStringTemplateInt64Codec(firstCodec) || !validStringTemplateInt64Codec(secondCodec) {
		return stringTemplatePayload{}, fmt.Errorf("invalid string template codecs %s/%s", firstCodec, secondCodec)
	}
	firstPayloadLen, err := checkedInt("string template first payload length", binary.LittleEndian.Uint64(payload[16:]))
	if err != nil {
		return stringTemplatePayload{}, err
	}
	secondPayloadLen, err := checkedInt("string template second payload length", binary.LittleEndian.Uint64(payload[24:]))
	if err != nil {
		return stringTemplatePayload{}, err
	}
	literalsLen, err := checkedAddInt("string template literal length", prefixLen, middleLen)
	if err != nil {
		return stringTemplatePayload{}, err
	}
	literalsLen, err = checkedAddInt("string template literal length", literalsLen, suffixLen)
	if err != nil {
		return stringTemplatePayload{}, err
	}
	firstStart, err := checkedAddInt("string template payload length", stringTemplateHeaderLen, literalsLen)
	if err != nil {
		return stringTemplatePayload{}, err
	}
	secondStart, err := checkedAddInt("string template payload length", firstStart, firstPayloadLen)
	if err != nil {
		return stringTemplatePayload{}, err
	}
	totalLen, err := checkedAddInt("string template payload length", secondStart, secondPayloadLen)
	if err != nil {
		return stringTemplatePayload{}, err
	}
	if len(payload) != totalLen {
		return stringTemplatePayload{}, fmt.Errorf("string template payload has %d bytes, want %d", len(payload), totalLen)
	}
	prefixStart := stringTemplateHeaderLen
	middleStart := prefixStart + prefixLen
	suffixStart := middleStart + middleLen
	firstPayloadStart := suffixStart + suffixLen
	return stringTemplatePayload{
		prefix:        string(payload[prefixStart:middleStart]),
		middle:        string(payload[middleStart:suffixStart]),
		suffix:        string(payload[suffixStart:firstPayloadStart]),
		firstRadix:    firstRadix,
		secondRadix:   secondRadix,
		firstCodec:    firstCodec,
		secondCodec:   secondCodec,
		firstPayload:  payload[firstPayloadStart:secondStart],
		secondPayload: payload[secondStart:],
	}, nil
}

func validStringTemplateRadix(radix int) bool {
	return radix == 10 || radix == 36
}

func validStringTemplateInt64Codec(codec Codec) bool {
	return codec == CodecPlain || codec == CodecDictionary || codec == CodecInt64Sequence
}

func parseStringPrefixPayload(payload []byte, rows int) (prefix []byte, suffix []byte, middlePayload []byte, err error) {
	if len(payload) < stringPrefixHeaderLen {
		return nil, nil, nil, fmt.Errorf("short string prefix header")
	}
	prefixLen, err := checkedInt("string prefix length", uint64(binary.LittleEndian.Uint32(payload)))
	if err != nil {
		return nil, nil, nil, err
	}
	suffixLen, err := checkedInt("string suffix length", uint64(binary.LittleEndian.Uint32(payload[4:])))
	if err != nil {
		return nil, nil, nil, err
	}
	middleLen, err := checkedInt("string prefix middle section length", binary.LittleEndian.Uint64(payload[8:]))
	if err != nil {
		return nil, nil, nil, err
	}
	sharedLen, err := checkedAddInt("string prefix metadata length", prefixLen, suffixLen)
	if err != nil {
		return nil, nil, nil, err
	}
	metadataLen, err := checkedAddInt("string prefix metadata length", stringPrefixHeaderLen, sharedLen)
	if err != nil {
		return nil, nil, nil, err
	}
	totalLen, err := checkedAddInt("string prefix payload length", metadataLen, middleLen)
	if err != nil {
		return nil, nil, nil, err
	}
	if len(payload) != totalLen {
		return nil, nil, nil, fmt.Errorf("string prefix payload has %d bytes, want %d", len(payload), totalLen)
	}
	minMiddleLen, err := checkedMulInt("string prefix offset payload length", rows+1, stringOffsetWidth)
	if err != nil {
		return nil, nil, nil, err
	}
	if middleLen < minMiddleLen {
		return nil, nil, nil, fmt.Errorf("string prefix middle section has %d bytes, want at least %d", middleLen, minMiddleLen)
	}
	prefixStart := stringPrefixHeaderLen
	suffixStart := prefixStart + prefixLen
	middleStart := suffixStart + suffixLen
	return payload[prefixStart:suffixStart], payload[suffixStart:middleStart], payload[middleStart:], nil
}

func countStringPrefixEqualPayload(payload []byte, rows int, value string) (count int, skipped bool, err error) {
	prefix, suffix, middlePayload, err := parseStringPrefixPayload(payload, rows)
	if err != nil {
		return 0, false, err
	}
	sharedLen := len(prefix) + len(suffix)
	if len(value) < sharedLen {
		return 0, true, nil
	}
	if !util.BytesEqualString(prefix, value[:len(prefix)]) {
		return 0, true, nil
	}
	suffixStart := len(value) - len(suffix)
	if !util.BytesEqualString(suffix, value[suffixStart:]) {
		return 0, true, nil
	}
	middleValue := value[len(prefix):suffixStart]
	count, err = countStringEqualPlain(middlePayload, rows, middleValue)
	return count, false, err
}

func countStringTemplateEqualPayload(payload []byte, rows int, value string) (count int, skipped bool, err error) {
	t, err := parseStringTemplatePayload(payload, rows)
	if err != nil {
		return 0, false, err
	}
	firstText, secondText, ok := splitStringTemplateValue(value, t.prefix, t.middle, t.suffix)
	if !ok {
		return 0, true, nil
	}
	firstValue, ok := parseStringTemplateNumber(firstText, t.firstRadix)
	if !ok {
		return 0, true, nil
	}
	secondValue, ok := parseStringTemplateNumber(secondText, t.secondRadix)
	if !ok {
		return 0, true, nil
	}
	if !stringTemplateNumberMatches(firstText, firstValue, t.firstRadix) || !stringTemplateNumberMatches(secondText, secondValue, t.secondRadix) {
		return 0, true, nil
	}
	firstValues, err := decodeInt64Payload(t.firstCodec, t.firstPayload, rows)
	if err != nil {
		return 0, false, fmt.Errorf("decode first template field: %w", err)
	}
	secondValues, err := decodeInt64Payload(t.secondCodec, t.secondPayload, rows)
	if err != nil {
		return 0, false, fmt.Errorf("decode second template field: %w", err)
	}
	for row := 0; row < rows; row++ {
		if firstValues.Values[row] == firstValue && secondValues.Values[row] == secondValue {
			count++
		}
	}
	return count, false, nil
}

func parseStringDictionaryPayload(payload []byte, rows int) (data []byte, ranges []uint64, counts []byte, ids []byte, idEncoding int, dictCount int, err error) {
	if len(payload) < stringDictionaryHeaderLen {
		return nil, nil, nil, nil, 0, 0, fmt.Errorf("short string dictionary header")
	}
	dictCount, err = checkedInt("string dictionary count", uint64(binary.LittleEndian.Uint32(payload)))
	if err != nil {
		return nil, nil, nil, nil, 0, 0, err
	}
	idEncoding = int(payload[4])
	if err := validateDictionaryIDEncoding("string", rows, dictCount, idEncoding); err != nil {
		return nil, nil, nil, nil, 0, 0, err
	}
	dictSectionLen, err := checkedInt("string dictionary section length", binary.LittleEndian.Uint64(payload[5:]))
	if err != nil {
		return nil, nil, nil, nil, 0, 0, err
	}
	metadataLen, err := stringDictionaryMetadataLen(dictSectionLen, dictCount)
	if err != nil {
		return nil, nil, nil, nil, 0, 0, err
	}
	idsLen, err := dictionaryIDsLen(rows, idEncoding)
	if err != nil {
		return nil, nil, nil, nil, 0, 0, err
	}
	payloadLen, err := checkedAddInt("string dictionary payload length", metadataLen, idsLen)
	if err != nil {
		return nil, nil, nil, nil, 0, 0, err
	}
	if len(payload) != payloadLen {
		return nil, nil, nil, nil, 0, 0, fmt.Errorf("string dictionary payload has %d bytes, want %d", len(payload), payloadLen)
	}
	dictStart := stringDictionaryHeaderLen
	dictSection := payload[dictStart : dictStart+dictSectionLen]
	data, ranges, err = parseStringDictionarySection(dictSection, dictCount)
	if err != nil {
		return nil, nil, nil, nil, 0, 0, err
	}
	countsStart := dictStart + dictSectionLen
	idsStart := metadataLen
	counts = payload[countsStart:idsStart]
	if err := validateDictionaryCounts("string", counts, dictCount, rows); err != nil {
		return nil, nil, nil, nil, 0, 0, err
	}
	return data, ranges, counts, payload[idsStart:], idEncoding, dictCount, nil
}

func parseStringDictionarySection(section []byte, dictCount int) ([]byte, []uint64, error) {
	offsetBytes, err := checkedMulInt("string dictionary offset payload length", dictCount+1, stringOffsetWidth)
	if err != nil {
		return nil, nil, err
	}
	if len(section) < offsetBytes {
		return nil, nil, fmt.Errorf("string dictionary section has %d bytes, want at least %d", len(section), offsetBytes)
	}
	if first := binary.LittleEndian.Uint32(section); first != 0 {
		return nil, nil, fmt.Errorf("first string dictionary offset is %d, want 0", first)
	}
	data := section[offsetBytes:]
	dataLen := uint64(len(data))
	if dataLen > uint64(^uint32(0)) {
		return nil, nil, fmt.Errorf("string dictionary data length %d overflows uint32 offsets", len(data))
	}
	if last := uint64(binary.LittleEndian.Uint32(section[dictCount*stringOffsetWidth:])); last != dataLen {
		return nil, nil, fmt.Errorf("last string dictionary offset is %d, want %d", last, dataLen)
	}
	ranges := make([]uint64, dictCount)
	for id := 0; id < dictCount; id++ {
		start := uint64(binary.LittleEndian.Uint32(section[id*stringOffsetWidth:]))
		end := uint64(binary.LittleEndian.Uint32(section[(id+1)*stringOffsetWidth:]))
		if start > end || end > dataLen {
			return nil, nil, fmt.Errorf("string dictionary offsets at value %d are out of bounds", id)
		}
		ranges[id] = vector.StringRange(uint32(start), uint32(end-start))
	}
	return data, ranges, nil
}

func countStringDictionaryEqualMetadata(metadata []byte, rows int, value string) (int, error) {
	if len(metadata) < stringDictionaryHeaderLen {
		return 0, fmt.Errorf("short string dictionary header")
	}
	dictCount, err := checkedInt("string dictionary count", uint64(binary.LittleEndian.Uint32(metadata)))
	if err != nil {
		return 0, err
	}
	idEncoding := int(metadata[4])
	if err := validateDictionaryIDEncoding("string", rows, dictCount, idEncoding); err != nil {
		return 0, err
	}
	dictSectionLen, err := checkedInt("string dictionary section length", binary.LittleEndian.Uint64(metadata[5:]))
	if err != nil {
		return 0, err
	}
	metadataLen, err := stringDictionaryMetadataLen(dictSectionLen, dictCount)
	if err != nil {
		return 0, err
	}
	if len(metadata) != metadataLen {
		return 0, fmt.Errorf("string dictionary metadata has %d bytes, want %d", len(metadata), metadataLen)
	}
	dictStart := stringDictionaryHeaderLen
	dictSection := metadata[dictStart : dictStart+dictSectionLen]
	offsetBytes, err := checkedMulInt("string dictionary offset payload length", dictCount+1, stringOffsetWidth)
	if err != nil {
		return 0, err
	}
	if len(dictSection) < offsetBytes {
		return 0, fmt.Errorf("string dictionary section has %d bytes, want at least %d", len(dictSection), offsetBytes)
	}
	if first := binary.LittleEndian.Uint32(dictSection); first != 0 {
		return 0, fmt.Errorf("first string dictionary offset is %d, want 0", first)
	}
	data := dictSection[offsetBytes:]
	dataLen := uint64(len(data))
	if dataLen > uint64(^uint32(0)) {
		return 0, fmt.Errorf("string dictionary data length %d overflows uint32 offsets", len(data))
	}
	if last := uint64(binary.LittleEndian.Uint32(dictSection[dictCount*stringOffsetWidth:])); last != dataLen {
		return 0, fmt.Errorf("last string dictionary offset is %d, want %d", last, dataLen)
	}
	counts := metadata[dictStart+dictSectionLen:]
	if err := validateDictionaryCounts("string", counts, dictCount, rows); err != nil {
		return 0, err
	}
	for id := 0; id < dictCount; id++ {
		start := uint64(binary.LittleEndian.Uint32(dictSection[id*stringOffsetWidth:]))
		end := uint64(binary.LittleEndian.Uint32(dictSection[(id+1)*stringOffsetWidth:]))
		if start > end || end > dataLen {
			return 0, fmt.Errorf("string dictionary offsets at value %d are out of bounds", id)
		}
		if int(end-start) == len(value) && util.BytesEqualString(data[start:end], value) {
			count, err := checkedInt("string dictionary count", dictionaryCountAt(counts, id))
			if err != nil {
				return 0, err
			}
			return count, nil
		}
	}
	return 0, nil
}

func groupStringDictionaryCountsMetadata(metadata []byte, rows int, counts map[string]int, keyScratch []string) ([]string, error) {
	if len(metadata) < stringDictionaryHeaderLen {
		return keyScratch, fmt.Errorf("short string dictionary header")
	}
	dictCount, err := checkedInt("string dictionary count", uint64(binary.LittleEndian.Uint32(metadata)))
	if err != nil {
		return keyScratch, err
	}
	idEncoding := int(metadata[4])
	if err := validateDictionaryIDEncoding("string", rows, dictCount, idEncoding); err != nil {
		return keyScratch, err
	}
	dictSectionLen, err := checkedInt("string dictionary section length", binary.LittleEndian.Uint64(metadata[5:]))
	if err != nil {
		return keyScratch, err
	}
	metadataLen, err := stringDictionaryMetadataLen(dictSectionLen, dictCount)
	if err != nil {
		return keyScratch, err
	}
	if len(metadata) != metadataLen {
		return keyScratch, fmt.Errorf("string dictionary metadata has %d bytes, want %d", len(metadata), metadataLen)
	}
	dictStart := stringDictionaryHeaderLen
	dictSection := metadata[dictStart : dictStart+dictSectionLen]
	offsetBytes, err := checkedMulInt("string dictionary offset payload length", dictCount+1, stringOffsetWidth)
	if err != nil {
		return keyScratch, err
	}
	if len(dictSection) < offsetBytes {
		return keyScratch, fmt.Errorf("string dictionary section has %d bytes, want at least %d", len(dictSection), offsetBytes)
	}
	if first := binary.LittleEndian.Uint32(dictSection); first != 0 {
		return keyScratch, fmt.Errorf("first string dictionary offset is %d, want 0", first)
	}
	data := dictSection[offsetBytes:]
	dataLen := uint64(len(data))
	if dataLen > uint64(^uint32(0)) {
		return keyScratch, fmt.Errorf("string dictionary data length %d overflows uint32 offsets", len(data))
	}
	if last := uint64(binary.LittleEndian.Uint32(dictSection[dictCount*stringOffsetWidth:])); last != dataLen {
		return keyScratch, fmt.Errorf("last string dictionary offset is %d, want %d", last, dataLen)
	}
	countBytes := metadata[dictStart+dictSectionLen:]
	if err := validateDictionaryCounts("string", countBytes, dictCount, rows); err != nil {
		return keyScratch, err
	}
	keyIndex := stringGroupKeyIndex(counts, keyScratch, dictCount)
	for id := 0; id < dictCount; id++ {
		count, err := checkedInt("string dictionary count", dictionaryCountAt(countBytes, id))
		if err != nil {
			return keyScratch, err
		}
		if count == 0 {
			continue
		}
		start := uint64(binary.LittleEndian.Uint32(dictSection[id*stringOffsetWidth:]))
		end := uint64(binary.LittleEndian.Uint32(dictSection[(id+1)*stringOffsetWidth:]))
		if start > end || end > dataLen {
			return keyScratch, fmt.Errorf("string dictionary offsets at value %d are out of bounds", id)
		}
		keyScratch, err = addBytesGroupCount(counts, data[start:end], count, keyScratch, keyIndex)
		if err != nil {
			return keyScratch, err
		}
	}
	return keyScratch, nil
}

func groupStringCountsPlain(payload []byte, rows int, counts map[string]int) error {
	offsetBytes, err := checkedMulInt("string offset payload length", rows+1, stringOffsetWidth)
	if err != nil {
		return err
	}
	if len(payload) < offsetBytes {
		return fmt.Errorf("plain string payload has %d bytes, want at least %d", len(payload), offsetBytes)
	}
	if first := binary.LittleEndian.Uint32(payload); first != 0 {
		return fmt.Errorf("first string offset is %d, want 0", first)
	}
	data := payload[offsetBytes:]
	dataLen := uint64(len(data))
	if dataLen > uint64(^uint32(0)) {
		return fmt.Errorf("plain string data length %d overflows uint32 offsets", len(data))
	}
	if last := uint64(binary.LittleEndian.Uint32(payload[rows*stringOffsetWidth:])); last != dataLen {
		return fmt.Errorf("last string offset is %d, want %d", last, dataLen)
	}

	for row := 0; row < rows; row++ {
		start := uint64(binary.LittleEndian.Uint32(payload[row*stringOffsetWidth:]))
		end := uint64(binary.LittleEndian.Uint32(payload[(row+1)*stringOffsetWidth:]))
		if start > end || end > dataLen {
			return fmt.Errorf("string offsets at row %d are out of bounds", row)
		}
		if err := addStringGroupCount(counts, string(data[start:end]), 1); err != nil {
			return err
		}
	}
	return nil
}

func groupStringCountsPrefix(payload []byte, rows int, counts map[string]int) error {
	values, err := decodeStringPrefix(payload, rows)
	if err != nil {
		return err
	}
	for row := 0; row < values.Len(); row++ {
		if err := addStringGroupCount(counts, values.Value(row), 1); err != nil {
			return err
		}
	}
	return nil
}

func groupStringCountsTemplate(payload []byte, rows int, counts map[string]int) error {
	values, err := decodeStringTemplate(payload, rows)
	if err != nil {
		return err
	}
	for row := 0; row < values.Len(); row++ {
		if err := addStringGroupCount(counts, values.Value(row), 1); err != nil {
			return err
		}
	}
	return nil
}

func stringGroupKeyIndex(counts map[string]int, keyScratch []string, extra int) map[string]string {
	keyIndex := make(map[string]string, len(counts)+len(keyScratch)+extra)
	for key := range counts {
		keyIndex[key] = key
	}
	for _, key := range keyScratch {
		keyIndex[key] = key
	}
	return keyIndex
}

func addBytesGroupCount(counts map[string]int, value []byte, count int, keyScratch []string, keyIndex map[string]string) ([]string, error) {
	if key, ok := keyIndex[string(value)]; ok {
		return keyScratch, addStringGroupCount(counts, key, count)
	}
	key := string(value)
	counts[key] = count
	keyIndex[key] = key
	return append(keyScratch, key), nil
}

func addStringGroupCount(counts map[string]int, key string, count int) error {
	next, err := checkedAddInt("string group count", counts[key], count)
	if err != nil {
		return err
	}
	counts[key] = next
	return nil
}

func parseStringDictionaryMetadata(metadata []byte, rows int) (data []byte, ranges []uint64, counts []byte, idEncoding int, dictCount int, err error) {
	if len(metadata) < stringDictionaryHeaderLen {
		return nil, nil, nil, 0, 0, fmt.Errorf("short string dictionary header")
	}
	dictCount, err = checkedInt("string dictionary count", uint64(binary.LittleEndian.Uint32(metadata)))
	if err != nil {
		return nil, nil, nil, 0, 0, err
	}
	idEncoding = int(metadata[4])
	if err := validateDictionaryIDEncoding("string", rows, dictCount, idEncoding); err != nil {
		return nil, nil, nil, 0, 0, err
	}
	dictSectionLen, err := checkedInt("string dictionary section length", binary.LittleEndian.Uint64(metadata[5:]))
	if err != nil {
		return nil, nil, nil, 0, 0, err
	}
	metadataLen, err := stringDictionaryMetadataLen(dictSectionLen, dictCount)
	if err != nil {
		return nil, nil, nil, 0, 0, err
	}
	if len(metadata) != metadataLen {
		return nil, nil, nil, 0, 0, fmt.Errorf("string dictionary metadata has %d bytes, want %d", len(metadata), metadataLen)
	}
	dictStart := stringDictionaryHeaderLen
	data, ranges, err = parseStringDictionarySection(metadata[dictStart:dictStart+dictSectionLen], dictCount)
	if err != nil {
		return nil, nil, nil, 0, 0, err
	}
	counts = metadata[dictStart+dictSectionLen:]
	if err := validateDictionaryCounts("string", counts, dictCount, rows); err != nil {
		return nil, nil, nil, 0, 0, err
	}
	return data, ranges, counts, idEncoding, dictCount, nil
}
