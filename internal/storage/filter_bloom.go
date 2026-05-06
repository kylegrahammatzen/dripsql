package storage

import (
	"encoding/binary"
	"fmt"
	"math/bits"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

const (
	pageBloomHeaderLen       = 8
	pageBloomBitsPerRow      = 6
	pageBloomHashes          = 4
	pageBloomSampleRows      = 4096
	maxPageBloomAvgValueSize = 16

	pageBloomHashSeed = 0x9e3779b97f4a7c15
)

func encodeStringPageBloom(values vector.String, pages []Page) ([]byte, error) {
	rows := values.Len()
	if len(pages) == 0 || rows < defaultPageRows || !shouldBuildStringPageBloom(values) {
		return nil, nil
	}
	if uint64(len(pages)) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("page bloom count %d overflows uint32", len(pages))
	}
	size := pageBloomHeaderLen
	for i := range pages {
		page := pages[i]
		if err := validatePageBloomPage(page, rows); err != nil {
			return nil, err
		}
		byteLen, err := pageBloomByteLen(page.Count, pageBloomBitsPerRow)
		if err != nil {
			return nil, err
		}
		size, err = checkedAddInt("page bloom length", size, byteLen)
		if err != nil {
			return nil, err
		}
	}
	out := make([]byte, size)
	binary.LittleEndian.PutUint32(out, uint32(len(pages)))
	out[4] = pageBloomHashes
	out[5] = pageBloomBitsPerRow

	off := pageBloomHeaderLen
	for i := range pages {
		page := pages[i]
		byteLen, err := pageBloomByteLen(page.Count, pageBloomBitsPerRow)
		if err != nil {
			return nil, err
		}
		filter := pageBloomFilter{bits: byteLen * 8, hashes: pageBloomHashes, data: out[off : off+byteLen]}
		end := page.StartRow + page.Count
		for row := page.StartRow; row < end; row++ {
			filter.add(values.Value(row))
		}
		off += byteLen
	}
	return out, nil
}

func shouldBuildStringPageBloom(values vector.String) bool {
	count := min(values.Len(), pageBloomSampleRows)
	if count == 0 {
		return false
	}
	total := 0
	for row := 0; row < count; row++ {
		total += len(values.Value(sampleStringPageBloomRow(row, count, values.Len())))
	}
	return total <= count*maxPageBloomAvgValueSize
}

func sampleStringPageBloomRow(sample int, sampleCount int, rows int) int {
	if sampleCount >= rows || sampleCount <= 1 {
		return sample
	}
	return sample * (rows - 1) / (sampleCount - 1)
}

func validatePageBloomPage(page Page, rows int) error {
	if page.StartRow < 0 || page.Count <= 0 || page.StartRow > rows-page.Count {
		return fmt.Errorf("invalid page bloom page start=%d count=%d rows=%d", page.StartRow, page.Count, rows)
	}
	return nil
}

func pageBloomByteLen(rows int, bitsPerRow int) (int, error) {
	if rows < 0 {
		return 0, fmt.Errorf("negative page bloom row count %d", rows)
	}
	if bitsPerRow <= 0 {
		return 0, fmt.Errorf("invalid page bloom bits per row %d", bitsPerRow)
	}
	bitsLen, err := checkedMulInt("page bloom bits", rows, bitsPerRow)
	if err != nil {
		return 0, err
	}
	bytesLen, err := checkedAddInt("page bloom bytes", bitsLen, 7)
	if err != nil {
		return 0, err
	}
	return bytesLen / 8, nil
}

func pageBloomHeader(buf []byte) (pageCount int, hashes int, bitsPerRow int, err error) {
	if len(buf) < pageBloomHeaderLen {
		return 0, 0, 0, fmt.Errorf("short page bloom header")
	}
	pageCount, err = checkedInt("page bloom count", uint64(binary.LittleEndian.Uint32(buf)))
	if err != nil {
		return 0, 0, 0, err
	}
	hashes = int(buf[4])
	bitsPerRow = int(buf[5])
	if hashes <= 0 {
		return 0, 0, 0, fmt.Errorf("invalid page bloom hash count %d", hashes)
	}
	if bitsPerRow <= 0 {
		return 0, 0, 0, fmt.Errorf("invalid page bloom bits per row %d", bitsPerRow)
	}
	return pageCount, hashes, bitsPerRow, nil
}

type pageBloomFilter struct {
	bits   int
	hashes int
	data   []byte
}

func (f pageBloomFilter) add(value string) {
	base, step := stringBloomHashesFor(value)
	bitCount := uint64(f.bits)
	for i, hash := 0, base; i < f.hashes; i, hash = i+1, hash+step {
		setBloomBit(f.data, int(hash%bitCount))
	}
}

func (f pageBloomFilter) mayContain(value string) bool {
	if !f.valid() {
		return true
	}
	base, step := stringBloomHashesFor(value)
	bitCount := uint64(f.bits)
	for i, hash := 0, base; i < f.hashes; i, hash = i+1, hash+step {
		if !hasBloomBit(f.data, int(hash%bitCount)) {
			return false
		}
	}
	return true
}

func (f pageBloomFilter) valid() bool {
	return f.bits > 0 && f.hashes > 0 && len(f.data) > 0 && f.bits <= len(f.data)*8
}

func setBloomBit(data []byte, bit int) {
	data[bit>>3] |= byte(1 << uint(bit&7))
}

func hasBloomBit(data []byte, bit int) bool {
	return data[bit>>3]&byte(1<<uint(bit&7)) != 0
}

func stringBloomHashesFor(value string) (uint64, uint64) {
	base := fnv1aString(value)
	step := mix64(base ^ pageBloomHashSeed)
	if step == 0 {
		step = pageBloomHashSeed
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
