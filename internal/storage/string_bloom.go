package storage

import (
	"math/bits"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

const (
	minStringBloomRows     = 4096
	stringBloomBitsPerRow  = 6
	stringBloomHashes      = 4
	maxStringBloomByteSize = 256 << 10
)

// StringBloomFilter is a persisted segment-level membership filter for string equality pruning.
type StringBloomFilter struct {
	Bits   int
	Hashes int
	Data   []byte
}

func buildStringBloom(values vector.String) (*StringBloomFilter, error) {
	if values.Len() < minStringBloomRows {
		return nil, nil
	}
	byteLen, err := stringBloomByteLen(values.Len())
	if err != nil {
		return nil, err
	}
	if byteLen == 0 {
		return nil, nil
	}
	bloom := &StringBloomFilter{Bits: byteLen * 8, Hashes: stringBloomHashes, Data: make([]byte, byteLen)}
	for row := 0; row < values.Len(); row++ {
		bloom.add(values.Value(row))
	}
	return bloom, nil
}

func stringBloomByteLen(count int) (int, error) {
	bitsLen, err := checkedMulInt("string bloom bits", count, stringBloomBitsPerRow)
	if err != nil {
		return 0, err
	}
	bytesLen, err := checkedAddInt("string bloom bytes", bitsLen, 7)
	if err != nil {
		return 0, err
	}
	bytesLen /= 8
	if bytesLen > maxStringBloomByteSize {
		return maxStringBloomByteSize, nil
	}
	return bytesLen, nil
}

// CanSkipStringEqual reports whether stats prove value is absent from this string column segment.
func CanSkipStringEqual(stats ColumnStats, value string) bool {
	if stats.Kind != vector.KindString || stats.Encoding.StringBloom == nil {
		return false
	}
	return !stats.Encoding.StringBloom.mayContain(value)
}

func (b *StringBloomFilter) add(value string) {
	base, step := stringBloomHashesFor(value)
	for i := 0; i < b.Hashes; i++ {
		b.setBit(b.bitIndex(base, step, i))
	}
}

func (b *StringBloomFilter) mayContain(value string) bool {
	if !b.valid() {
		return true
	}
	base, step := stringBloomHashesFor(value)
	for i := 0; i < b.Hashes; i++ {
		if !b.hasBit(b.bitIndex(base, step, i)) {
			return false
		}
	}
	return true
}

func (b *StringBloomFilter) valid() bool {
	return b != nil && b.Bits > 0 && b.Hashes > 0 && len(b.Data) > 0 && b.Bits <= len(b.Data)*8
}

func (b *StringBloomFilter) bitIndex(base uint64, step uint64, hash int) int {
	return int((base + uint64(hash)*step) % uint64(b.Bits))
}

func (b *StringBloomFilter) setBit(bit int) {
	b.Data[bit/8] |= 1 << uint(bit%8)
}

func (b *StringBloomFilter) hasBit(bit int) bool {
	return b.Data[bit/8]&(1<<uint(bit%8)) != 0
}

func stringBloomHashesFor(value string) (uint64, uint64) {
	base := fnv1aString(value)
	step := mix64(base ^ 0x9e3779b97f4a7c15)
	if step == 0 {
		step = 0x9e3779b97f4a7c15
	}
	return base, step
}

func fnv1aString(value string) uint64 {
	const (
		offset uint64 = 14695981039346656037
		prime  uint64 = 1099511628211
	)
	hash := offset
	for i := 0; i < len(value); i++ {
		hash ^= uint64(value[i])
		hash *= prime
	}
	return hash
}

func mix64(value uint64) uint64 {
	value ^= value >> 30
	value *= 0xbf58476d1ce4e5b9
	value ^= value >> 27
	value *= 0x94d049bb133111eb
	value ^= value >> 31
	return bits.RotateLeft64(value, 17)
}
