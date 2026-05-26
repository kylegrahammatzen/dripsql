// Pcodec splits the page into 1024-row chunks each with its own FOR base and bit-width, beating single-pass FOR on multimodal data.
// Accepts IsFORPackable ints plus float64 via bit-pattern reinterpretation so cascade can pick it when ALP and ALP-RD both ErrSkip.
package codec

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

const (
	pcodecChunkRows  = BitpackBlockSize
	pcodecFileHeader = 4
	pcodecChunkHead  = 9
)

type pcodecCodec struct{}

func init() {
	Register(pcodecCodec{})
}

func (pcodecCodec) Encoding() schema.Encoding { return schema.EncPcodec }

func (c pcodecCodec) Encode(v vector.Vec, ctx *EncodeContext) ([]byte, error) {
	if !pcodecAccepts(v.Kind) {
		return nil, ErrSkip
	}
	rows := int(v.Len)
	if rows < pcodecChunkRows*2 {
		return nil, ErrSkip
	}
	storageBits := int(v.Kind.FixedWidth()) * 8
	residuals := ctxU64s(ctx, rows)
	readPcodecValues(v, residuals)
	chunks := (rows + pcodecChunkRows - 1) / pcodecChunkRows
	bases := make([]int64, chunks)
	widths := make([]int, chunks)
	total := pcodecFileHeader
	for ci := range chunks {
		start := ci * pcodecChunkRows
		end := min(start+pcodecChunkRows, rows)
		base, width := chunkParams(residuals[start:end], storageBits)
		bases[ci] = base
		widths[ci] = width
		total += pcodecChunkHead
		if width > 0 {
			total += PackedSize(end-start, width)
		}
	}
	scratch := ctxTrial(ctx)
	if cap(scratch) < total {
		scratch = make([]byte, total)
	} else {
		scratch = scratch[:total]
	}
	binary.LittleEndian.PutUint16(scratch[0:2], uint16(pcodecChunkRows))
	binary.LittleEndian.PutUint16(scratch[2:4], uint16(chunks))
	off := pcodecFileHeader
	for ci := range chunks {
		start := ci * pcodecChunkRows
		end := min(start+pcodecChunkRows, rows)
		binary.LittleEndian.PutUint64(scratch[off:off+8], uint64(bases[ci]))
		scratch[off+8] = byte(widths[ci])
		off += pcodecChunkHead
		if widths[ci] > 0 {
			chunkRows := end - start
			for i := start; i < end; i++ {
				residuals[i] -= uint64(bases[ci])
			}
			size := PackedSize(chunkRows, widths[ci])
			Pack(widths[ci], residuals[start:end], scratch[off:off+size])
			off += size
		}
	}
	return scratch, nil
}

func (pcodecCodec) Decode(payload []byte, kind vector.VecKind, rows, nullCount int, dst *vector.Vec) error {
	if err := validateDecodeArgs(rows, nullCount); err != nil {
		return fmt.Errorf("pcodec decode: %w", err)
	}
	if !pcodecAccepts(kind) {
		return fmt.Errorf("pcodec decode: kind %v not accepted", kind)
	}
	if rows == 0 {
		if len(payload) != 0 {
			return fmt.Errorf("pcodec decode: zero rows but %d-byte payload", len(payload))
		}
		dst.ResetForDecode(kind)
		_ = dst.EnsureFixedBytes(0)
		return nil
	}
	if len(payload) < pcodecFileHeader {
		return fmt.Errorf("pcodec decode: header truncated, have %d", len(payload))
	}
	chunkRows := int(binary.LittleEndian.Uint16(payload[0:2]))
	chunks := int(binary.LittleEndian.Uint16(payload[2:4]))
	if chunkRows <= 0 {
		return fmt.Errorf("pcodec decode: bad chunkRows %d", chunkRows)
	}
	wantChunks := (rows + chunkRows - 1) / chunkRows
	if chunks != wantChunks {
		return fmt.Errorf("pcodec decode: chunks %d != expected %d", chunks, wantChunks)
	}
	residuals := make([]uint64, rows)
	off := pcodecFileHeader
	for ci := range chunks {
		if off+pcodecChunkHead > len(payload) {
			return fmt.Errorf("pcodec decode: chunk %d head truncated", ci)
		}
		base := int64(binary.LittleEndian.Uint64(payload[off : off+8]))
		width := int(payload[off+8])
		off += pcodecChunkHead
		if width > 64 {
			return fmt.Errorf("pcodec decode: chunk %d width %d out of range", ci, width)
		}
		start := ci * chunkRows
		end := min(start+chunkRows, rows)
		if width == 0 {
			for i := start; i < end; i++ {
				residuals[i] = uint64(base)
			}
			continue
		}
		size := PackedSize(end-start, width)
		if off+size > len(payload) {
			return fmt.Errorf("pcodec decode: chunk %d payload truncated", ci)
		}
		Unpack(width, payload[off:off+size], end-start, residuals[start:end])
		off += size
		for i := start; i < end; i++ {
			residuals[i] = uint64(base + int64(residuals[i]))
		}
	}
	if off != len(payload) {
		return fmt.Errorf("pcodec decode: trailing %d bytes", len(payload)-off)
	}
	dst.ResetForDecode(kind)
	dst.EnsureFixedBytes(rows)
	if err := writePcodecValues(dst, residuals); err != nil {
		return fmt.Errorf("pcodec decode: %w", err)
	}
	return nil
}

func pcodecAccepts(k vector.VecKind) bool {
	return k.IsFORPackable() || k == vector.VecFloat64
}

func readPcodecValues(v vector.Vec, dst []uint64) {
	if v.Kind == vector.VecFloat64 {
		for i, x := range v.F64() {
			dst[i] = math.Float64bits(x)
		}
		return
	}
	readFORValues(v, dst)
}

func writePcodecValues(dst *vector.Vec, residuals []uint64) error {
	if dst.Kind == vector.VecFloat64 {
		out := dst.F64()
		for i, r := range residuals {
			out[i] = math.Float64frombits(r)
		}
		return nil
	}
	return writeFORValues(dst, residuals, 0)
}

func chunkParams(residuals []uint64, storageBits int) (base int64, width int) {
	minVal := int64(residuals[0])
	maxVal := minVal
	for i := 1; i < len(residuals); i++ {
		val := int64(residuals[i])
		if val < minVal {
			minVal = val
		}
		if val > maxVal {
			maxVal = val
		}
	}
	span := uint64(maxVal) - uint64(minVal)
	if span == 0 {
		return minVal, 0
	}
	w := min(64-bits.LeadingZeros64(span), storageBits)
	return minVal, w
}
