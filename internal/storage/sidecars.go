// Optional sidecars encoded inline into the segment container and consulted at OpenSegment.
// Generic Sidecar[T] plumbing, the per-tag container framer, and the concrete sidecars all live here.
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type Sidecar[T any] struct {
	Magic      string
	Suffix     string
	BufHint    int
	EncodeBody func(name string, v T, w *wireBuffer) error
	DecodeBody func(name string, r *wireReader) (T, error)
}

// Encode returns the body bytes (magic+colCount+entries) for embedding as a section inside a container.
func (s Sidecar[T]) Encode(entries map[string]T) ([]byte, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	hint := s.BufHint
	if hint <= 0 {
		hint = 64
	}
	w := newWireBuffer(hint + len(entries)*64)
	w.Raw([]byte(s.Magic))
	w.U32(uint32(len(entries)))
	for name, v := range entries {
		w.LenPrefixedString(name)
		if err := s.EncodeBody(name, v, w); err != nil {
			return nil, fmt.Errorf("%s: encode body %q: %w", s.Suffix, name, err)
		}
	}
	return w.Bytes(), nil
}

// Decode parses bytes previously produced by Encode. Returns (nil, nil) for empty
// input so callers can treat absent sections as no-op.
func (s Sidecar[T]) Decode(data []byte) (map[string]T, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) < 8 || string(data[:4]) != s.Magic {
		return nil, fmt.Errorf("%s: bad magic", s.Suffix)
	}
	r := newWireReader(data[4:])
	cols := int(r.U32())
	out := make(map[string]T, cols)
	for range cols {
		name := r.LenPrefixedString()
		if err := r.Err(); err != nil {
			return nil, fmt.Errorf("%s: decode: %w", s.Suffix, err)
		}
		v, err := s.DecodeBody(name, r)
		if err != nil {
			return nil, fmt.Errorf("%s: decode body %q: %w", s.Suffix, name, err)
		}
		out[schema.NormalizeName(name)] = v
	}
	if err := r.Err(); err != nil {
		return nil, fmt.Errorf("%s: decode: %w", s.Suffix, err)
	}
	if !r.AtEnd() {
		return nil, fmt.Errorf("%s: trailing bytes (remaining=%d)", s.Suffix, r.Remaining())
	}
	return out, nil
}

const sidecarContainerMagic = "DSCV1"

// Tag 5 is the identity sidecar declared in segment_read.go.
const (
	sidecarSectionDictHist  uint8 = 1
	sidecarSectionIntFilter uint8 = 2
	sidecarSectionNumSum    uint8 = 3
	sidecarSectionVarBloom  uint8 = 4
	sidecarSectionGroupSums uint8 = 6
)

const sidecarSectionHeaderSize = 1 + 4

// EncodeSidecarContainer serializes sections to a single byte buffer suitable for
// embedding inline at the end of a segment file. Empty sections are skipped.
// Returns nil when every section is empty.
func EncodeSidecarContainer(sections map[uint8][]byte) []byte {
	live := make([]uint8, 0, len(sections))
	total := 0
	for tag, body := range sections {
		if len(body) == 0 {
			continue
		}
		live = append(live, tag)
		total += sidecarSectionHeaderSize + len(body)
	}
	if len(live) == 0 {
		return nil
	}
	hdr := len(sidecarContainerMagic) + 1
	buf := make([]byte, 0, hdr+total)
	buf = append(buf, sidecarContainerMagic...)
	buf = append(buf, uint8(len(live)))
	for _, tag := range live {
		body := sections[tag]
		buf = append(buf, tag)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(body)))
		buf = append(buf, body...)
	}
	return buf
}

// DecodeSidecarContainer parses bytes previously produced by EncodeSidecarContainer.
// Empty input returns (nil, nil). Bad magic or truncated section is an error.
func DecodeSidecarContainer(data []byte) (map[uint8][]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	hdr := len(sidecarContainerMagic) + 1
	if len(data) < hdr || string(data[:len(sidecarContainerMagic)]) != sidecarContainerMagic {
		return nil, errors.New("sidecar container: bad magic")
	}
	count := int(data[len(sidecarContainerMagic)])
	pos := hdr
	out := make(map[uint8][]byte, count)
	for range count {
		if pos+sidecarSectionHeaderSize > len(data) {
			return nil, fmt.Errorf("sidecar container: header truncated at pos %d", pos)
		}
		tag := data[pos]
		bodyLen := int(binary.LittleEndian.Uint32(data[pos+1 : pos+5]))
		pos += sidecarSectionHeaderSize
		if pos+bodyLen > len(data) {
			return nil, fmt.Errorf("sidecar container: body truncated for tag %d", tag)
		}
		out[tag] = data[pos : pos+bodyLen]
		pos += bodyLen
	}
	if pos != len(data) {
		return nil, errors.New("sidecar container: trailing bytes")
	}
	return out, nil
}

const (
	dictHistMagic         = "DHV1"
	dictHistMaxDistinct   = 4096
	dictHistFileExtension = ".dh"
)

type DictHistogram map[string]uint64

type DictHistograms map[string]DictHistogram

var dictHistSidecar = Sidecar[DictHistogram]{
	Magic:   dictHistMagic,
	Suffix:  dictHistFileExtension,
	BufHint: 64,
	EncodeBody: func(_ string, hist DictHistogram, w *wireBuffer) error {
		w.U32(uint32(len(hist)))
		for value, count := range hist {
			w.U64(count)
			w.U32(uint32(len(value)))
			w.Raw([]byte(value))
		}
		return nil
	},
	DecodeBody: func(_ string, r *wireReader) (DictHistogram, error) {
		entries := int(r.U32())
		hist := make(DictHistogram, entries)
		for range entries {
			count := r.U64()
			vlen := int(r.U32())
			val := r.Raw(vlen)
			hist[string(val)] = count
		}
		return hist, nil
	},
}

// DictSums maps one group value's raw bytes to the accumulated sum of a numeric column.
type DictSums map[string]int64

// GroupSumsCol carries the per numeric column sums for one group column.
type GroupSumsCol map[string]DictSums

// GroupSumsMap is keyed by normalized group column name.
type GroupSumsMap map[string]GroupSumsCol

var groupSumsSidecar = Sidecar[GroupSumsCol]{
	Magic:   "GSV1",
	Suffix:  ".gs",
	BufHint: 64,
	EncodeBody: func(_ string, gc GroupSumsCol, w *wireBuffer) error {
		w.U32(uint32(len(gc)))
		for numName, sums := range gc {
			w.LenPrefixedString(numName)
			w.U32(uint32(len(sums)))
			for value, sum := range sums {
				w.U64(uint64(sum))
				w.U32(uint32(len(value)))
				w.Raw([]byte(value))
			}
		}
		return nil
	},
	DecodeBody: func(_ string, r *wireReader) (GroupSumsCol, error) {
		numCols := int(r.U32())
		gc := make(GroupSumsCol, numCols)
		for range numCols {
			numName := r.LenPrefixedString()
			entries := int(r.U32())
			sums := make(DictSums, entries)
			for range entries {
				sum := int64(r.U64())
				vlen := int(r.U32())
				val := r.Raw(vlen)
				sums[string(val)] = sum
			}
			gc[schema.NormalizeName(numName)] = sums
		}
		return gc, nil
	},
}

// maxGroupSumPairs caps the write cost of the pair pass, chosen pairs follow schema order.
const maxGroupSumPairs = 8

// buildGroupSums pairs histogram eligible group columns with all valid int columns and
// accumulates per group value sums so grouped sum queries can answer from metadata.
// Runs after writePayloads because sink flags gate eligibility.
func buildGroupSums(pages []vector.Batch, cols []writerColumn, sinks []*colSink) GroupSumsMap {
	if len(pages) == 0 {
		return nil
	}
	var groupIdx, numIdx []int
	for ci := range cols {
		s := sinks[ci]
		if s == nil {
			continue
		}
		switch {
		case cols[ci].Kind.IsVarBytes():
			if !s.varHistSkip && len(s.varHist) > 0 {
				groupIdx = append(groupIdx, ci)
			}
		case kindEligibleForIntFilter(cols[ci].Kind):
			if cols[ci].NullCount == 0 {
				numIdx = append(numIdx, ci)
			}
		}
	}
	if len(groupIdx) == 0 || len(numIdx) == 0 {
		return nil
	}
	out := GroupSumsMap{}
	pairs := 0
	for _, gi := range groupIdx {
		if pairs >= maxGroupSumPairs {
			break
		}
		take := min(len(numIdx), maxGroupSumPairs-pairs)
		nums := numIdx[:take]
		pairs += take
		if gc := accumulateGroupSums(pages, gi, nums, cols); len(gc) > 0 {
			out[schema.NormalizeName(cols[gi].Schema.Name)] = gc
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func accumulateGroupSums(pages []vector.Batch, gi int, nums []int, cols []writerColumn) GroupSumsCol {
	type groupAccum struct{ sums []int64 }
	acc := make(map[string]*groupAccum)
	dead := make([]bool, len(nums))
	for _, page := range pages {
		gvec := page.Columns[gi].V
		gv := gvec.Var()
		for r := range page.Len {
			if !isValidRow(gvec, r) {
				continue
			}
			key := gv.Bytes(r)
			a := acc[string(key)]
			if a == nil {
				if len(acc) >= dictHistMaxDistinct {
					return nil
				}
				a = &groupAccum{sums: make([]int64, len(nums))}
				acc[string(key)] = a
			}
			for k, ni := range nums {
				if dead[k] {
					continue
				}
				v := readInt64Key(page.Columns[ni].V, r)
				if addOverflowsInt64(a.sums[k], v) {
					dead[k] = true
					continue
				}
				a.sums[k] += v
			}
		}
	}
	gc := GroupSumsCol{}
	for k, ni := range nums {
		if dead[k] {
			continue
		}
		sums := make(DictSums, len(acc))
		for val, a := range acc {
			sums[val] = a.sums[k]
		}
		gc[schema.NormalizeName(cols[ni].Schema.Name)] = sums
	}
	return gc
}

const (
	intFilterMagic      = "FFV1"
	intFilterFileSuffix = ".bf"
	// Distinct-key cap for the seal-time bloom, past it the filter is skipped so
	// high-cardinality columns in large segments do not hold hundreds of MB.
	intFilterMaxDistinct = 1 << 20
)

type IntFilter struct {
	bloom *bloomFilter
}

type IntFilters map[string]*IntFilter

var intFilterSidecar = Sidecar[*IntFilter]{
	Magic:   intFilterMagic,
	Suffix:  intFilterFileSuffix,
	BufHint: 64,
	EncodeBody: func(_ string, f *IntFilter, w *wireBuffer) error {
		body := f.bloom.marshal()
		w.U32(uint32(len(body)))
		w.Raw(body)
		return nil
	},
	DecodeBody: func(_ string, r *wireReader) (*IntFilter, error) {
		bodyLen := int(r.U32())
		body := r.Raw(bodyLen)
		if err := r.Err(); err != nil {
			return nil, err
		}
		b, err := unmarshalBloomFilter(body)
		if err != nil {
			return nil, err
		}
		return &IntFilter{bloom: b}, nil
	},
}

func (f *IntFilter) Contains(v int64) bool {
	if f == nil || f.bloom == nil {
		return true
	}
	return f.bloom.contains(uint64(v))
}

func kindEligibleForIntFilter(k vector.VecKind) bool {
	switch k {
	case vector.VecInt16, vector.VecInt32, vector.VecInt64,
		vector.VecDate, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
		return true
	}
	return false
}

func readInt64Key(v vector.Vec, row int) int64 {
	switch v.Kind {
	case vector.VecInt16:
		return int64(v.I16()[row])
	case vector.VecInt32, vector.VecDate:
		return int64(v.I32()[row])
	}
	return v.I64()[row]
}

// VarBloom is a per-page bloom filter set for one varbytes column. Index in the
// outer slice is the segment-relative page index. A nil entry means no bloom for
// that page (all-null or build skipped) and callers must treat that as cannot-prune.
type VarBloom struct {
	Pages []*bloomFilter
}

type VarBlooms map[string]*VarBloom

const (
	varBloomMagic      = "TBV1"
	varBloomFileSuffix = ".tbf"
)

var varBloomSidecar = Sidecar[*VarBloom]{
	Magic:   varBloomMagic,
	Suffix:  varBloomFileSuffix,
	BufHint: 256,
	EncodeBody: func(_ string, vb *VarBloom, w *wireBuffer) error {
		w.U32(uint32(len(vb.Pages)))
		for _, b := range vb.Pages {
			if b == nil {
				w.U32(0)
				continue
			}
			body := b.marshal()
			w.U32(uint32(len(body)))
			w.Raw(body)
		}
		return nil
	},
	DecodeBody: func(_ string, r *wireReader) (*VarBloom, error) {
		n := int(r.U32())
		pages := make([]*bloomFilter, n)
		for i := range n {
			bodyLen := int(r.U32())
			if bodyLen == 0 {
				continue
			}
			body := r.Raw(bodyLen)
			if err := r.Err(); err != nil {
				return nil, err
			}
			b, err := unmarshalBloomFilter(body)
			if err != nil {
				return nil, err
			}
			pages[i] = b
		}
		return &VarBloom{Pages: pages}, nil
	},
}

// hashBytesFNV is the byte-string-to-uint64 hash used to feed the per-page bloom.
// Build and query must use the same function and FNV-1a-64 is fast and seedless.
func hashBytesFNV(b []byte) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for _, c := range b {
		h ^= uint64(c)
		h *= prime64
	}
	return h
}

const (
	numSumMagic      = "NSV1"
	numSumFileSuffix = ".sm"
)

type NumericSum struct {
	Sum int64
}

type NumericSums map[string]NumericSum

var numSumSidecar = Sidecar[NumericSum]{
	Magic:   numSumMagic,
	Suffix:  numSumFileSuffix,
	BufHint: 16,
	EncodeBody: func(_ string, s NumericSum, w *wireBuffer) error {
		w.U64(uint64(s.Sum))
		return nil
	},
	DecodeBody: func(_ string, r *wireReader) (NumericSum, error) {
		return NumericSum{Sum: int64(r.U64())}, nil
	},
}

// Materializes the three sidecars from accumulators that analyzePage filled
// during the body encode pass. No re-scan of page data.
func materializeSidecarsFromSinks(cols []writerColumn, sinks []*colSink) (DictHistograms, IntFilters, NumericSums, VarBlooms) {
	if len(cols) == 0 {
		return nil, nil, nil, nil
	}
	dictHists := DictHistograms{}
	intFilters := IntFilters{}
	numSums := NumericSums{}
	varBlooms := VarBlooms{}

	for ci, c := range cols {
		s := sinks[ci]
		if s == nil {
			continue
		}
		name := schema.NormalizeName(c.Schema.Name)
		switch {
		case c.Kind.IsVarBytes():
			if !s.varHistSkip && len(s.varHist) > 0 {
				dictHists[name] = s.varHist
			}
			if len(s.varPageKeys) > 1 {
				pages := make([]*bloomFilter, len(s.varPageKeys))
				any := false
				for i, keys := range s.varPageKeys {
					if len(keys) == 0 {
						continue
					}
					if b := newBloomFilter(keys); b != nil {
						pages[i] = b
						any = true
					}
				}
				if any {
					varBlooms[name] = &VarBloom{Pages: pages}
				}
			}
		case kindEligibleForIntFilter(c.Kind):
			if s.intAny && !s.intOverflow {
				numSums[name] = NumericSum{Sum: s.intSum}
			}
			if !s.intFilterSkip {
				if b := newBloomFilter(s.intKeys); b != nil {
					intFilters[name] = &IntFilter{bloom: b}
				}
			}
		}
	}

	if len(dictHists) == 0 {
		dictHists = nil
	}
	if len(intFilters) == 0 {
		intFilters = nil
	}
	if len(numSums) == 0 {
		numSums = nil
	}
	if len(varBlooms) == 0 {
		varBlooms = nil
	}
	return dictHists, intFilters, numSums, varBlooms
}

func addOverflowsInt64(a, b int64) bool {
	if b > 0 && a > int64(^uint64(0)>>1)-b {
		return true
	}
	if b < 0 && a < -int64(^uint64(0)>>1)-1-b {
		return true
	}
	return false
}
