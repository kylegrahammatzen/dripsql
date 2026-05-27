// Optional sidecars built post-segment-publish and consulted at OpenSegment.
// Generic Sidecar[T] plumbing, the per-tag container framer, and the four concrete sidecars all live here.
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

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

func (s Sidecar[T]) Path(segPath string) string { return segPath + s.Suffix }

// Encode returns the body bytes (magic+colCount+entries) for use either as a
// standalone sidecar file or as a section inside a container.
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

// Empty entries produce no file so sidecars stay best-effort.
func (s Sidecar[T]) Write(segPath string, entries map[string]T) error {
	data, err := s.Encode(entries)
	if err != nil {
		return err
	}
	if data == nil {
		return nil
	}
	path := s.Path(segPath)
	tmpPath := path + ".tmp"
	_ = os.Remove(tmpPath)
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return syncDir(filepath.Dir(path))
}

// Missing file returns (nil, nil). Bad magic or trailing bytes are an error so
// silent corruption never serves stale data.
func (s Sidecar[T]) Read(segPath string) (map[string]T, error) {
	path := s.Path(segPath)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return s.Decode(data)
}

const sidecarContainerMagic = "DSCV1"

const (
	sidecarSectionDictHist  uint8 = 1
	sidecarSectionIntFilter uint8 = 2
	sidecarSectionNumSum    uint8 = 3
	sidecarSectionVarBloom  uint8 = 4
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
