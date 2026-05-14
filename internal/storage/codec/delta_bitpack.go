package codec

import (
	"encoding/binary"
	"fmt"
	"math/bits"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

// DeltaBitPack stores monotonic-ish integer columns as (first value, then
// bit-packed first-order differences). The deltas of clustered, near-sorted
// data are small even when the absolute values span a wide range, so a delta
// pass before BitPack saves more bits than plain FOR on sorted timestamps,
// IDs, and counters.
//
// On non-monotonic data Estimate returns a size larger than plain
// FOR+BitPack, so PickSmallest naturally avoids it.
type DeltaBitPack struct{}

func (DeltaBitPack) Encoding() types.Encoding { return types.EncodingDeltaBitPack }

func (DeltaBitPack) Encode(v types.Vec) (Page, error) {
	prepared, ok := (DeltaBitPack{}).Prepare(v)
	if !ok {
		return Page{}, fmt.Errorf("delta+bitpack cannot encode %s/%s", v.Kind, v.Encoding)
	}
	return prepared.EncodeInto(nil)
}

func (DeltaBitPack) Prepare(v types.Vec) (PreparedEncoding, bool) {
	first, deltas, ok := deltaBitPackBuild(v)
	if !ok {
		return nil, false
	}
	base, maxDelta := deltaRange(deltas)
	width := bits.Len64(uint64(maxDelta) - uint64(base))
	if width == 0 {
		// Degenerate (constant deltas) — width 1 keeps the format trivial.
		width = 1
	}
	return preparedDeltaBitPack{
		vec:    v,
		first:  first,
		deltas: deltas,
		base:   base,
		width:  width,
		size:   deltaBitPackPayloadSize(v.Len, v.Valid, width),
	}, true
}

type preparedDeltaBitPack struct {
	vec    types.Vec
	first  int64
	deltas []int64
	base   int64
	width  int
	size   int
}

func (p preparedDeltaBitPack) Encoding() types.Encoding { return types.EncodingDeltaBitPack }

func (p preparedDeltaBitPack) Size() int { return p.size }

func (p preparedDeltaBitPack) EncodeInto(scratch []byte) (Page, error) {
	payload := preparedPayload(scratch, p.size)
	pos := writeValidity(payload, p.vec.Valid)
	binary.LittleEndian.PutUint64(payload[pos:pos+8], uint64(p.first))
	pos += 8
	binary.LittleEndian.PutUint64(payload[pos:pos+8], uint64(p.base))
	pos += 8
	payload[pos] = byte(p.width)
	pos++
	// Pack deltas[0..n-2] bit-packed at width bits each.
	for i, d := range p.deltas {
		offset := uint64(d) - uint64(p.base)
		forBitPackSet(payload[pos:], i, p.width, offset)
	}
	return Page{Kind: p.vec.Kind, Encoding: types.EncodingDeltaBitPack, Rows: p.vec.Len, NullCount: types.NullCount(p.vec.Valid, p.vec.Len), Payload: payload}, nil
}

func (DeltaBitPack) Decode(page Page) (types.Vec, error) {
	var v types.Vec
	if err := (DeltaBitPack{}).DecodeInto(page, &v); err != nil {
		return types.Vec{}, err
	}
	return v, nil
}

func (DeltaBitPack) DecodeInto(page Page, dst *types.Vec) error {
	if dst == nil {
		return fmt.Errorf("delta+bitpack decode destination is nil")
	}
	if page.Encoding != types.EncodingDeltaBitPack {
		return fmt.Errorf("delta+bitpack cannot decode %s", page.Encoding)
	}
	if !page.Kind.IsFORPackable() || page.Kind == types.VecEnum32 {
		// Deltas only make sense for ordered integer-shaped columns; enums
		// are categorical so reject them here even though FOR supports them.
		return fmt.Errorf("delta+bitpack unsupported kind %s", page.Kind)
	}
	valid, pos, err := readValidityInto(page.Payload, page.Rows, page.NullCount, dst.Valid)
	if err != nil {
		return err
	}
	if len(page.Payload)-pos < 17 {
		return fmt.Errorf("delta+bitpack header truncated")
	}
	first := int64(binary.LittleEndian.Uint64(page.Payload[pos : pos+8]))
	pos += 8
	base := int64(binary.LittleEndian.Uint64(page.Payload[pos : pos+8]))
	pos += 8
	width := int(page.Payload[pos])
	pos++
	if width <= 0 || width > 64 {
		return fmt.Errorf("delta+bitpack invalid width %d", width)
	}
	deltaCount := max(page.Rows-1, 0)
	packedBytes := (deltaCount*width + 7) / 8
	if len(page.Payload)-pos < packedBytes {
		return fmt.Errorf("delta+bitpack values truncated")
	}
	resetVecForDecode(dst, page.Kind, false)
	dst.Kind = page.Kind
	dst.Encoding = types.EncodingFlat
	dst.Len = page.Rows
	dst.Valid = valid
	data := page.Payload[pos : pos+packedBytes]
	value := first
	switch page.Kind {
	case types.VecInt16:
		dst.I16 = resizeSlice(dst.I16, page.Rows)
		if page.Rows > 0 {
			dst.I16[0] = int16(value)
		}
		for i := 0; i < deltaCount; i++ {
			value += base + int64(forBitPackGetNaive(data, i, width))
			dst.I16[i+1] = int16(value)
		}
	case types.VecInt32, types.VecDate:
		dst.I32 = resizeSlice(dst.I32, page.Rows)
		if page.Rows > 0 {
			dst.I32[0] = int32(value)
		}
		for i := 0; i < deltaCount; i++ {
			value += base + int64(forBitPackGetNaive(data, i, width))
			dst.I32[i+1] = int32(value)
		}
	case types.VecInt64, types.VecTimestamp, types.VecTime:
		dst.I64 = resizeSlice(dst.I64, page.Rows)
		if page.Rows > 0 {
			dst.I64[0] = value
		}
		for i := 0; i < deltaCount; i++ {
			value += base + int64(forBitPackGetNaive(data, i, width))
			dst.I64[i+1] = value
		}
	}
	return nil
}

func (DeltaBitPack) Estimate(v types.Vec) (int, bool) {
	_, deltas, ok := deltaBitPackBuild(v)
	if !ok {
		return 0, false
	}
	base, maxDelta := deltaRange(deltas)
	width := bits.Len64(uint64(maxDelta) - uint64(base))
	if width == 0 {
		width = 1
	}
	return deltaBitPackPayloadSize(v.Len, v.Valid, width), true
}

func deltaBitPackPayloadSize(rows int, valid types.Validity, width int) int {
	deltas := max(rows-1, 0)
	return validityBytes(valid) + 8 + 8 + 1 + (deltas*width+7)/8
}

// deltaBitPackBuild produces the first value and the per-row deltas. Nulls
// short-circuit (deltas are undefined when interleaved with nulls), so the
// codec rejects vectors containing null rows entirely.
func deltaBitPackBuild(v types.Vec) (int64, []int64, bool) {
	if v.Encoding != types.EncodingFlat || v.Len <= 0 {
		return 0, nil, false
	}
	switch v.Kind {
	case types.VecInt16, types.VecInt32, types.VecDate, types.VecInt64, types.VecTimestamp, types.VecTime:
	default:
		return 0, nil, false
	}
	if v.Valid != nil && types.NullCount(v.Valid, v.Len) > 0 {
		return 0, nil, false
	}
	switch v.Kind {
	case types.VecInt16:
		return deltasOver(v.I16[:v.Len])
	case types.VecInt32, types.VecDate:
		return deltasOver(v.I32[:v.Len])
	case types.VecInt64, types.VecTimestamp, types.VecTime:
		return deltasOver(v.I64[:v.Len])
	}
	return 0, nil, false
}

// deltasOver returns the first value and per-row deltas. Hoists the kind
// switch out of the per-row loop so the inner subtraction runs on a typed
// slice without a per-row dispatch.
func deltasOver[T int16 | int32 | int64](values []T) (int64, []int64, bool) {
	first := int64(values[0])
	if len(values) == 1 {
		return first, nil, true
	}
	deltas := make([]int64, len(values)-1)
	prev := values[0]
	for i, v := range values[1:] {
		deltas[i] = int64(v) - int64(prev)
		prev = v
	}
	return first, deltas, true
}

func deltaRange(deltas []int64) (int64, int64) {
	if len(deltas) == 0 {
		return 0, 0
	}
	minValue, maxValue := deltas[0], deltas[0]
	for _, d := range deltas[1:] {
		if d < minValue {
			minValue = d
		}
		if d > maxValue {
			maxValue = d
		}
	}
	return minValue, maxValue
}
