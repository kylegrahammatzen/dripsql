package codec

import (
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type Page struct {
	Kind      types.VecKind
	Encoding  types.Encoding
	Rows      int
	NullCount int
	Payload   []byte
}

// Codec is the base codec contract every implementation satisfies. Decode
// writes into a caller-provided Vec so the segment reader can amortize slice
// allocations across pages; callers that don't yet have a Vec pass a fresh
// zero value.
type Codec interface {
	Encoding() types.Encoding
	Encode(v types.Vec) (Page, error)
	DecodeInto(page Page, dst *types.Vec) error
	Estimate(v types.Vec) (int, bool)
}

// PreparedEncoding is the prepared-state handle returned by PreparableCodec.
// Prepare. It captures the cascade picker's per-column scratch (FOR base,
// dictionary build, etc.) so encode does not re-scan the vector after
// Estimate.
type PreparedEncoding interface {
	Encoding() types.Encoding
	Size() int
	EncodeInto(scratch []byte) (Page, error)
}

// PreparableCodec is implemented by codecs whose Estimate produces durable
// state that Encode would otherwise recompute (FOR base/width, dictionary
// build). Cascade picking uses Prepare so the winning codec encodes without
// a second scan.
type PreparableCodec interface {
	Codec
	Prepare(v types.Vec) (PreparedEncoding, bool)
}

// SelectedCodec is implemented by codecs with a per-row decode fast path that
// can skip non-selected rows.
type SelectedCodec interface {
	Codec
	DecodeSelected(page Page, sel types.SelectionMask) (types.Vec, error)
}

// EncodedCodec is implemented by codecs whose payload can be exposed in its
// non-flat form so predicate eval and aggregate kernels can operate on
// encoded bytes directly.
type EncodedCodec interface {
	Codec
	DecodeEncoded(page Page) (types.Vec, error)
}

func PickSmallest(v types.Vec, codecs ...Codec) (Codec, bool) {
	var best Codec
	bestSize := 0
	for _, c := range codecs {
		if c == nil {
			continue
		}
		size, ok := c.Estimate(v)
		if !ok {
			continue
		}
		if best == nil || size < bestSize {
			best = c
			bestSize = size
		}
	}
	return best, best != nil
}

func PickSmallestPrepared(v types.Vec, codecs ...Codec) (PreparedEncoding, bool) {
	var best PreparedEncoding
	for _, c := range codecs {
		prepared, ok := prepareEncoding(c, v)
		if !ok {
			continue
		}
		if best == nil || prepared.Size() < best.Size() {
			best = prepared
		}
	}
	return best, best != nil
}

func prepareEncoding(c Codec, v types.Vec) (PreparedEncoding, bool) {
	if c == nil {
		return nil, false
	}
	if preparable, ok := c.(PreparableCodec); ok {
		return preparable.Prepare(v)
	}
	size, ok := c.Estimate(v)
	if !ok {
		return nil, false
	}
	return estimatedEncoding{codec: c, vec: v, size: size}, true
}

type estimatedEncoding struct {
	codec Codec
	vec   types.Vec
	size  int
}

func (e estimatedEncoding) Encoding() types.Encoding { return e.codec.Encoding() }

func (e estimatedEncoding) Size() int { return e.size }

func (e estimatedEncoding) EncodeInto(_ []byte) (Page, error) { return e.codec.Encode(e.vec) }

func preparedPayload(scratch []byte, size int) []byte {
	if cap(scratch) < size {
		return make([]byte, size)
	}
	payload := scratch[:size]
	clear(payload)
	return payload
}
