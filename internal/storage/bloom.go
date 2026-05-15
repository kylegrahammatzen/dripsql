// Per-segment Bloom filter sidecar (.bf) for int equality pruning. Built at seal time
// for every Int16/Int32/Int64-class column; consulted by boundEqInt64.PruneSegment when
// the value falls inside the column min/max range but is not in the per-segment set.
// Verify-after-prediction per goals.md non-negotiables: Bloom only proves absence;
// false positives fall through to the regular scan.
package storage

import (
	"fmt"
	"math"
	"os"
	"path/filepath"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

const (
	bloomMagic        = "BFV1"
	bloomFileSuffix   = ".bf"
	bloomBitsPerEntry = 10 // ~1% false positive at k = 7
	bloomMaxK         = 8
)

type IntBloom struct {
	Bits []uint64
	M    uint32 // bit count, always a multiple of 64
	K    uint8
}

type IntBlooms map[string]*IntBloom

func bloomPath(segPath string) string { return segPath + bloomFileSuffix }

// newIntBloom sizes the filter for n distinct keys at the target bits-per-entry. m is
// rounded up to a multiple of 64 so reset / get can address whole words.
func newIntBloom(n int) *IntBloom {
	if n <= 0 {
		n = 1
	}
	m := uint32(n) * bloomBitsPerEntry
	if m < 64 {
		m = 64
	}
	if rem := m % 64; rem != 0 {
		m += 64 - rem
	}
	return &IntBloom{Bits: make([]uint64, m/64), M: m, K: bloomMaxK}
}

// Add records `v` in the filter. Uses double-hashing over splitmix64 derivatives so
// neighbouring values do not collide trivially.
func (b *IntBloom) Add(v int64) {
	h1, h2 := bloomHashes(uint64(v))
	for i := uint8(0); i < b.K; i++ {
		bit := uint32((h1 + uint64(i)*h2)) % b.M
		b.Bits[bit>>6] |= uint64(1) << (bit & 63)
	}
}

// Contains reports whether `v` MIGHT be present. False positives possible (~1%); a false
// return is a hard absence proof for the segment.
func (b *IntBloom) Contains(v int64) bool {
	h1, h2 := bloomHashes(uint64(v))
	for i := uint8(0); i < b.K; i++ {
		bit := uint32((h1 + uint64(i)*h2)) % b.M
		if b.Bits[bit>>6]&(uint64(1)<<(bit&63)) == 0 {
			return false
		}
	}
	return true
}

func bloomHashes(x uint64) (uint64, uint64) {
	h1 := x
	h1 ^= h1 >> 30
	h1 *= 0xbf58476d1ce4e5b9
	h1 ^= h1 >> 27
	h1 *= 0x94d049bb133111eb
	h1 ^= h1 >> 31
	h2 := x + 0x9e3779b97f4a7c15
	h2 ^= h2 >> 30
	h2 *= 0xbf58476d1ce4e5b9
	h2 ^= h2 >> 27
	if h2 == 0 {
		h2 = 1
	}
	return h1, h2
}

// buildIntBlooms scans every column whose kind decodes into int64 keys and builds a
// per-column Bloom. Columns with no rows or all-null are skipped.
func buildIntBlooms(pages []types.Batch) IntBlooms {
	if len(pages) == 0 {
		return nil
	}
	out := IntBlooms{}
	for ci, col := range pages[0].Columns {
		if !bloomEligibleKind(col.V.Kind) {
			continue
		}
		var total int
		for _, p := range pages {
			total += int(p.Columns[ci].V.Len)
		}
		if total == 0 {
			continue
		}
		bloom := newIntBloom(total)
		any := false
		for _, p := range pages {
			c := p.Columns[ci]
			rows := int(c.V.Len)
			valid := c.V.Valid
			for r := range rows {
				if valid != nil && !valid.IsValid(r) {
					continue
				}
				bloom.Add(readInt64Key(c.V, r))
				any = true
			}
		}
		if any {
			out[types.NormalizeName(col.Name)] = bloom
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func bloomEligibleKind(k types.VecKind) bool {
	switch k {
	case types.VecInt16, types.VecInt32, types.VecInt64,
		types.VecDate, types.VecTimestamp, types.VecTime, types.VecDecimal64:
		return true
	}
	return false
}

func readInt64Key(v types.Vec, row int) int64 {
	switch v.Kind {
	case types.VecInt16:
		return int64(v.I16()[row])
	case types.VecInt32, types.VecDate:
		return int64(v.I32()[row])
	}
	return v.I64()[row]
}

func writeBloomSidecar(segPath string, blooms IntBlooms) error {
	if len(blooms) == 0 {
		return nil
	}
	path := bloomPath(segPath)
	tmpPath := path + ".tmp"
	_ = os.Remove(tmpPath)
	buf := encodeBlooms(blooms)
	if err := os.WriteFile(tmpPath, buf, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return syncDir(filepath.Dir(path))
}

func encodeBlooms(blooms IntBlooms) []byte {
	approx := 16 + len(blooms)*256
	w := newWireBuffer(approx)
	w.Raw([]byte(bloomMagic))
	w.U32(uint32(len(blooms)))
	for name, b := range blooms {
		w.LenPrefixedString(name)
		w.U32(b.M)
		w.U8(b.K)
		w.U32(uint32(len(b.Bits)))
		for _, word := range b.Bits {
			w.U64(word)
		}
	}
	return w.Bytes()
}

// LoadIntBlooms reads a .bf sidecar if present. Missing file returns nil without error.
func LoadIntBlooms(segPath string) (IntBlooms, error) {
	path := bloomPath(segPath)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if len(data) < 8 || string(data[:4]) != bloomMagic {
		return nil, fmt.Errorf("bloom: bad magic at %s", path)
	}
	r := newWireReader(data[4:])
	cols := int(r.U32())
	out := make(IntBlooms, cols)
	for range cols {
		name := r.LenPrefixedString()
		m := r.U32()
		k := r.U8()
		words := int(r.U32())
		if k > bloomMaxK || m == 0 || words > math.MaxInt32 {
			return nil, fmt.Errorf("bloom: malformed entry %q", name)
		}
		bits := make([]uint64, words)
		for i := range bits {
			bits[i] = r.U64()
		}
		out[types.NormalizeName(name)] = &IntBloom{Bits: bits, M: m, K: k}
	}
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("bloom: decode %s: %w", path, err)
	}
	if !r.AtEnd() {
		return nil, fmt.Errorf("bloom: trailing bytes at %s", path)
	}
	return out, nil
}
