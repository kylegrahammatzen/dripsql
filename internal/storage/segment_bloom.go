package storage

import "github.com/kylegrahammatzen/dripsql/internal/vector"

const (
	minInt64BloomRows             = 4096
	int64BloomBitsPerRow          = 6
	int64BloomHashes              = 4
	maxInt64BloomByteSize         = 256 << 10
	int64BloomDenseSpanMultiplier = 4

	minStringBloomRows     = 4096
	stringBloomBitsPerRow  = 6
	stringBloomHashes      = 4
	stringBloomSampleRows  = 4096
	maxStringBloomAvgBytes = 17
	maxStringBloomByteSize = 256 << 10
)

// Int64BloomFilter is a persisted segment-level membership filter for int64 equality pruning.
type Int64BloomFilter struct {
	Bits   int
	Hashes int
	Data   []byte
}

// StringBloomFilter is a persisted segment-level membership filter for string equality pruning.
type StringBloomFilter struct {
	Bits   int
	Hashes int
	Data   []byte
}

// BuildInt64Bloom builds a segment-level membership filter for sparse int64 equality pruning.
func BuildInt64Bloom(values vector.Int64) (*Int64BloomFilter, error) {
	if values.Len() < minInt64BloomRows || !shouldBuildInt64Bloom(values) {
		return nil, nil
	}
	byteLen, err := bloomByteLen(values.Len(), int64BloomBitsPerRow, maxInt64BloomByteSize, "int64 bloom")
	if err != nil || byteLen == 0 {
		return nil, err
	}
	bloom := &Int64BloomFilter{Bits: byteLen * 8, Hashes: int64BloomHashes, Data: make([]byte, byteLen)}
	for _, value := range values.Values {
		bloom.add(value)
	}
	return bloom, nil
}

// BuildStringBloom builds a segment-level membership filter for string equality pruning.
func BuildStringBloom(values vector.String) (*StringBloomFilter, error) {
	if values.Len() < minStringBloomRows || !shouldBuildStringBloom(values) {
		return nil, nil
	}
	byteLen, err := bloomByteLen(values.Len(), stringBloomBitsPerRow, maxStringBloomByteSize, "string bloom")
	if err != nil || byteLen == 0 {
		return nil, err
	}
	bloom := &StringBloomFilter{Bits: byteLen * 8, Hashes: stringBloomHashes, Data: make([]byte, byteLen)}
	for row := 0; row < values.Len(); row++ {
		bloom.add(values.Value(row))
	}
	return bloom, nil
}

// CanSkipInt64Equal reports whether metadata proves value is absent from this int64 column segment.
func CanSkipInt64Equal(col Column, value int64) bool {
	if col.Kind != vector.KindInt64 {
		return false
	}
	if col.HasMinMax && (value < col.MinInt64 || value > col.MaxInt64) {
		return true
	}
	if col.Int64Bloom == nil {
		return false
	}
	return !col.Int64Bloom.mayContain(value)
}

// CanSkipStringEqual reports whether metadata proves value is absent from this string column segment.
func CanSkipStringEqual(col Column, value string) bool {
	if col.Kind != vector.KindString || col.StringBloom == nil {
		return false
	}
	return !col.StringBloom.mayContain(value)
}

func shouldBuildInt64Bloom(values vector.Int64) bool {
	minValue, maxValue, ok := values.MinMax()
	if !ok {
		return false
	}
	return int64SpanExceedsDensity(minValue, maxValue, values.Len(), int64BloomDenseSpanMultiplier)
}

func int64SpanExceedsDensity(minValue int64, maxValue int64, count int, multiplier int) bool {
	if count <= 0 || multiplier <= 0 || maxValue < minValue {
		return false
	}
	limit := uint64(count) * uint64(multiplier)
	if minValue >= 0 {
		return uint64(maxValue-minValue)+1 > limit
	}
	if maxValue < 0 {
		return uint64(maxValue-minValue)+1 > limit
	}
	negativeSpan := uint64(-(minValue + 1)) + 1
	positiveSpan := uint64(maxValue) + 1
	return negativeSpan > limit || positiveSpan > limit || negativeSpan+positiveSpan > limit
}

func shouldBuildStringBloom(values vector.String) bool {
	count := min(values.Len(), stringBloomSampleRows)
	if count == 0 {
		return false
	}
	total := 0
	for row := 0; row < count; row++ {
		total += len(values.Value(sampleBloomRow(row, count, values.Len())))
	}
	return total <= count*maxStringBloomAvgBytes
}

func sampleBloomRow(sample int, sampleCount int, rows int) int {
	if sampleCount >= rows || sampleCount <= 1 {
		return sample
	}
	return sample * (rows - 1) / (sampleCount - 1)
}

func bloomByteLen(count int, bitsPerRow int, maxBytes int, label string) (int, error) {
	bitsLen, err := checkedMulInt(label+" bits", count, bitsPerRow)
	if err != nil {
		return 0, err
	}
	bytesLen, err := checkedAddInt(label+" bytes", bitsLen, 7)
	if err != nil {
		return 0, err
	}
	bytesLen /= 8
	if bytesLen > maxBytes {
		return maxBytes, nil
	}
	return bytesLen, nil
}

func (b *Int64BloomFilter) add(value int64) {
	base, step := int64BloomHashesFor(value)
	for i := 0; i < b.Hashes; i++ {
		setBloomBit(b.Data, int((base+uint64(i)*step)%uint64(b.Bits)))
	}
}

func (b *Int64BloomFilter) mayContain(value int64) bool {
	if b == nil || b.Bits <= 0 || b.Hashes <= 0 || len(b.Data) == 0 || b.Bits > len(b.Data)*8 {
		return true
	}
	base, step := int64BloomHashesFor(value)
	for i := 0; i < b.Hashes; i++ {
		if !hasBloomBit(b.Data, int((base+uint64(i)*step)%uint64(b.Bits))) {
			return false
		}
	}
	return true
}

func (b *StringBloomFilter) add(value string) {
	base, step := stringBloomHashesFor(value)
	for i := 0; i < b.Hashes; i++ {
		setBloomBit(b.Data, int((base+uint64(i)*step)%uint64(b.Bits)))
	}
}

func (b *StringBloomFilter) mayContain(value string) bool {
	if b == nil || b.Bits <= 0 || b.Hashes <= 0 || len(b.Data) == 0 || b.Bits > len(b.Data)*8 {
		return true
	}
	base, step := stringBloomHashesFor(value)
	for i := 0; i < b.Hashes; i++ {
		if !hasBloomBit(b.Data, int((base+uint64(i)*step)%uint64(b.Bits))) {
			return false
		}
	}
	return true
}

func int64BloomHashesFor(value int64) (uint64, uint64) {
	base := mix64(uint64(value))
	step := mix64(base ^ 0x517cc1b727220a95)
	if step == 0 {
		step = 0x517cc1b727220a95
	}
	return base, step
}
