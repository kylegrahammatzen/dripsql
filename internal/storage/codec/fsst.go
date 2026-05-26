// FSST varbytes codec. Single-round symbol-table training over the input, then
// greedy longest-prefix encoding with code 0xFF reserved as the escape byte.
package codec

import (
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

const (
	fsstMagicLen   = 4
	fsstMaxSymbols = 255
	fsstMaxSymLen  = 8
	fsstEscape     = 0xFF
)

var fsstMagic = []byte{'F', 'S', 'T', '1'}

func init() { Register(&fsstCodec{}) }

type fsstCodec struct{}

func (fsstCodec) Encoding() schema.Encoding { return schema.EncodingFSST }

func (fsstCodec) Encode(v vector.Vec, ctx *EncodeContext) ([]byte, error) {
	if !v.Kind.IsVarBytes() {
		return nil, ErrSkip
	}
	rows := int(v.Len)
	if rows == 0 {
		return nil, ErrSkip
	}
	vb := v.Var()

	// Aggregate input into one buffer so the trainer can walk one stream and the
	// encoder can emit per-row segments using the same lookup tables.
	var raw []byte
	rowLens := make([]int, rows)
	for i := range rows {
		bs := vb.Bytes(i)
		rowLens[i] = len(bs)
		raw = append(raw, bs...)
	}
	if len(raw) == 0 {
		return nil, ErrSkip
	}

	symbols := trainFSST(raw)
	if len(symbols) == 0 {
		return nil, ErrSkip
	}
	codes := newFSSTCoder(symbols)

	// Encode each row separately so the decoder can rebuild per-row lengths
	// without scanning the entire payload for delimiters.
	encoded := make([][]byte, rows)
	totalEnc := 0
	pos := 0
	for i := range rows {
		end := pos + rowLens[i]
		enc := codes.encode(raw[pos:end])
		encoded[i] = enc
		totalEnc += len(enc)
		pos = end
	}

	// Wire: magic(4) | nsyms(1) | for each symbol: len(1) + bytes |
	// rows(u32) | for each row: enc_len(u32) + enc_bytes
	hdr := fsstMagicLen + 1
	for _, s := range symbols {
		hdr += 1 + len(s)
	}
	hdr += 4 + 4*rows
	out := make([]byte, 0, hdr+totalEnc)
	out = append(out, fsstMagic...)
	out = append(out, uint8(len(symbols)))
	for _, s := range symbols {
		out = append(out, uint8(len(s)))
		out = append(out, s...)
	}
	out = binary.LittleEndian.AppendUint32(out, uint32(rows))
	for _, enc := range encoded {
		out = binary.LittleEndian.AppendUint32(out, uint32(len(enc)))
		out = append(out, enc...)
	}
	return out, nil
}

func (fsstCodec) Decode(payload []byte, kind vector.VecKind, rows, nullCount int, dst *vector.Vec) error {
	if err := validateDecodeArgs(rows, nullCount); err != nil {
		return fmt.Errorf("fsst decode: %w", err)
	}
	if !kind.IsVarBytes() {
		return fmt.Errorf("fsst decode: kind %v not varbytes", kind)
	}
	if len(payload) < fsstMagicLen+1 {
		return fmt.Errorf("fsst decode: header truncated")
	}
	if string(payload[:fsstMagicLen]) != string(fsstMagic) {
		return fmt.Errorf("fsst decode: bad magic")
	}
	pos := fsstMagicLen
	nsyms := int(payload[pos])
	pos++
	symbols := make([][]byte, nsyms)
	for i := range nsyms {
		if pos+1 > len(payload) {
			return fmt.Errorf("fsst decode: symbol header truncated at %d", i)
		}
		sl := int(payload[pos])
		pos++
		if pos+sl > len(payload) {
			return fmt.Errorf("fsst decode: symbol %d body truncated", i)
		}
		symbols[i] = payload[pos : pos+sl]
		pos += sl
	}
	if pos+4 > len(payload) {
		return fmt.Errorf("fsst decode: rows header truncated")
	}
	encRows := int(binary.LittleEndian.Uint32(payload[pos : pos+4]))
	pos += 4
	if encRows != rows {
		return fmt.Errorf("fsst decode: row mismatch wire=%d want=%d", encRows, rows)
	}

	*dst = vector.NewVarVec(kind, rows, 0)
	out := dst.Var()
	row := make([]byte, 0, 64)
	for i := range rows {
		if pos+4 > len(payload) {
			return fmt.Errorf("fsst decode: row %d length truncated", i)
		}
		encLen := int(binary.LittleEndian.Uint32(payload[pos : pos+4]))
		pos += 4
		if pos+encLen > len(payload) {
			return fmt.Errorf("fsst decode: row %d payload truncated", i)
		}
		row = row[:0]
		j := pos
		end := pos + encLen
		for j < end {
			c := payload[j]
			j++
			if c == fsstEscape {
				if j >= end {
					return fmt.Errorf("fsst decode: dangling escape at row %d", i)
				}
				row = append(row, payload[j])
				j++
				continue
			}
			idx := int(c)
			if idx >= len(symbols) {
				return fmt.Errorf("fsst decode: bad code %d at row %d", c, i)
			}
			row = append(row, symbols[idx]...)
		}
		out.AppendBytes(i, row)
		pos = end
	}
	dst.Enc = schema.EncodingFlat
	dst.Valid = nil
	return nil
}

// trainFSST counts 2..8-byte substrings in raw and picks up to 255 by gain
// (count * (length-1)). Symbols longer than 8 bytes are not considered.
func trainFSST(raw []byte) [][]byte {
	const sampleCap = 64 << 10
	sample := raw
	if len(sample) > sampleCap {
		sample = sample[:sampleCap]
	}
	counts := make(map[string]int, 1024)
	for L := 2; L <= fsstMaxSymLen; L++ {
		if L > len(sample) {
			break
		}
		end := len(sample) - L + 1
		for i := range end {
			counts[string(sample[i:i+L])]++
		}
	}
	type cand struct {
		s    string
		gain int
	}
	cands := make([]cand, 0, len(counts))
	for s, c := range counts {
		if c < 2 {
			continue
		}
		cands = append(cands, cand{s: s, gain: c * (len(s) - 1)})
	}
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].gain != cands[j].gain {
			return cands[i].gain > cands[j].gain
		}
		return cands[i].s < cands[j].s
	})
	if len(cands) > fsstMaxSymbols {
		cands = cands[:fsstMaxSymbols]
	}
	out := make([][]byte, len(cands))
	for i, c := range cands {
		out[i] = []byte(c.s)
	}
	return out
}

type fsstCoder struct {
	symbols [][]byte
	// byHead[b] is the list of symbol codes whose first byte is b, sorted by length descending.
	byHead [256][]uint8
}

func newFSSTCoder(symbols [][]byte) *fsstCoder {
	c := &fsstCoder{symbols: symbols}
	for i, s := range symbols {
		head := s[0]
		c.byHead[head] = append(c.byHead[head], uint8(i))
	}
	for b := range 256 {
		ids := c.byHead[b]
		sort.Slice(ids, func(i, j int) bool {
			return len(symbols[ids[i]]) > len(symbols[ids[j]])
		})
	}
	return c
}

func (c *fsstCoder) encode(in []byte) []byte {
	out := make([]byte, 0, len(in))
	i := 0
	for i < len(in) {
		head := in[i]
		matched := false
		for _, id := range c.byHead[head] {
			s := c.symbols[id]
			if i+len(s) <= len(in) && bytesEqual(in[i:i+len(s)], s) {
				if id == fsstEscape {
					continue
				}
				out = append(out, id)
				i += len(s)
				matched = true
				break
			}
		}
		if matched {
			continue
		}
		out = append(out, fsstEscape, head)
		i++
	}
	return out
}

// ParseFSSTHeader extracts the symbol table and the offset of the encoded-rows section.
// Callers use this to encode a comparison literal once before scanning row payloads.
func ParseFSSTHeader(payload []byte) (symbols [][]byte, rowsOff int, rowCount int, err error) {
	if len(payload) < fsstMagicLen+1 {
		return nil, 0, 0, fmt.Errorf("fsst: header truncated")
	}
	if string(payload[:fsstMagicLen]) != string(fsstMagic) {
		return nil, 0, 0, fmt.Errorf("fsst: bad magic")
	}
	pos := fsstMagicLen
	nsyms := int(payload[pos])
	pos++
	symbols = make([][]byte, nsyms)
	for i := range nsyms {
		if pos+1 > len(payload) {
			return nil, 0, 0, fmt.Errorf("fsst: symbol header truncated at %d", i)
		}
		sl := int(payload[pos])
		pos++
		if pos+sl > len(payload) {
			return nil, 0, 0, fmt.Errorf("fsst: symbol %d body truncated", i)
		}
		symbols[i] = payload[pos : pos+sl]
		pos += sl
	}
	if pos+4 > len(payload) {
		return nil, 0, 0, fmt.Errorf("fsst: rows header truncated")
	}
	rowCount = int(binary.LittleEndian.Uint32(payload[pos : pos+4]))
	return symbols, pos + 4, rowCount, nil
}

// EncodeFSSTLiteral encodes literal against the page symbol table; result aliases the coder buffer.
func EncodeFSSTLiteral(symbols [][]byte, literal []byte) []byte {
	return newFSSTCoder(symbols).encode(literal)
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
