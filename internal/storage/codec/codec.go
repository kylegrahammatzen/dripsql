package codec

import (
	"fmt"
	"sync"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type Page struct {
	Kind      types.VecKind
	Encoding  types.Encoding
	Rows      int
	NullCount int
	Payload   []byte
}

type Codec interface {
	Encoding() types.Encoding
	Encode(v types.Vec) (Page, error)
	Decode(page Page) (types.Vec, error)
	Estimate(v types.Vec) (int, bool)
}

type PreparedEncoding interface {
	Encoding() types.Encoding
	Size() int
	EncodeInto(scratch []byte) (Page, error)
}

type PreparableCodec interface {
	Codec
	Prepare(v types.Vec) (PreparedEncoding, bool)
}

type ReusableCodec interface {
	Codec
	DecodeInto(page Page, dst *types.Vec) error
}

type SelectedCodec interface {
	Codec
	DecodeSelected(page Page, sel types.SelectionMask) (types.Vec, error)
}

type EncodedCodec interface {
	Codec
	DecodeEncoded(page Page) (types.Vec, error)
}

var registry struct {
	sync.RWMutex
	codecs map[types.Encoding]Codec
}

func init() {
	registry.codecs = make(map[types.Encoding]Codec)
}

func Register(c Codec) error {
	if c == nil {
		return fmt.Errorf("codec is nil")
	}
	registry.Lock()
	defer registry.Unlock()
	enc := c.Encoding()
	if _, ok := registry.codecs[enc]; ok {
		return fmt.Errorf("codec %s already registered", enc)
	}
	registry.codecs[enc] = c
	return nil
}

func Lookup(enc types.Encoding) (Codec, bool) {
	registry.RLock()
	defer registry.RUnlock()
	c, ok := registry.codecs[enc]
	return c, ok
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
