// Bloom filter for int-column membership sidecar. ~10 bits/key, k=8 (Kirsch-Mitzenmacher
// double hashing from splitmix64). Linear single-pass build with no iterative retry.
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
)

const (
	bloomBitsPerKey = 10
	bloomNumHashes  = 8
	bloomMinBits    = 64
)

type bloomFilter struct {
	bits []uint64
	mask uint64
}

func newBloomFilter(keys []uint64) *bloomFilter {
	n := len(keys)
	if n == 0 {
		return nil
	}
	target := uint64(n * bloomBitsPerKey)
	if target < bloomMinBits {
		target = bloomMinBits
	}
	bitCount := uint64(1) << (64 - bits.LeadingZeros64(target-1))
	f := &bloomFilter{
		bits: make([]uint64, bitCount/64),
		mask: bitCount - 1,
	}
	for _, k := range keys {
		f.add(k)
	}
	return f
}

func (f *bloomFilter) add(k uint64) {
	h1 := splitmix64(k)
	h2 := splitmix64(h1 ^ 0xC6BC279692B5C323)
	for i := range uint64(bloomNumHashes) {
		bit := (h1 + i*h2) & f.mask
		f.bits[bit>>6] |= 1 << (bit & 63)
	}
}

func (f *bloomFilter) contains(k uint64) bool {
	if f == nil {
		return true
	}
	h1 := splitmix64(k)
	h2 := splitmix64(h1 ^ 0xC6BC279692B5C323)
	for i := range uint64(bloomNumHashes) {
		bit := (h1 + i*h2) & f.mask
		if f.bits[bit>>6]&(1<<(bit&63)) == 0 {
			return false
		}
	}
	return true
}

func splitmix64(x uint64) uint64 {
	x += 0x9E3779B97F4A7C15
	x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9
	x = (x ^ (x >> 27)) * 0x94D049BB133111EB
	return x ^ (x >> 31)
}

func (f *bloomFilter) marshal() []byte {
	if f == nil {
		return nil
	}
	out := make([]byte, 8+len(f.bits)*8)
	binary.LittleEndian.PutUint64(out[0:8], f.mask)
	for i, w := range f.bits {
		binary.LittleEndian.PutUint64(out[8+i*8:8+(i+1)*8], w)
	}
	return out
}

func unmarshalBloomFilter(data []byte) (*bloomFilter, error) {
	if len(data) < 8 {
		return nil, errors.New("bloom: header truncated")
	}
	mask := binary.LittleEndian.Uint64(data[0:8])
	body := data[8:]
	if len(body)%8 != 0 {
		return nil, errors.New("bloom: body not aligned")
	}
	words := len(body) / 8
	expected := int((mask + 1) / 64)
	if words != expected {
		return nil, fmt.Errorf("bloom: word count %d != expected %d", words, expected)
	}
	f := &bloomFilter{bits: make([]uint64, words), mask: mask}
	for i := range words {
		f.bits[i] = binary.LittleEndian.Uint64(body[i*8 : (i+1)*8])
	}
	return f, nil
}
