// Optional sidecars built post-segment-publish and consulted at OpenSegment.
// A failed build or read falls back to the operator scan with no error surfaced.
package storage

import (
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

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


const (
	intFilterMagic      = "FFV1"
	intFilterFileSuffix = ".bf"
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


func kindEligibleForIntFilter(k types.VecKind) bool {
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

// VarBloom is a per-page bloom filter set for one varbytes column. Index in the
// outer slice is the segment-relative page index. A nil entry means no bloom for
// that page (all-null or build skipped); callers must treat that as "cannot prune".
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
// Build and query must use the same function; FNV-1a-64 is fast and seedless.
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
		name := types.NormalizeName(c.Schema.Name)
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
			if b := newBloomFilter(s.intKeys); b != nil {
				intFilters[name] = &IntFilter{bloom: b}
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

