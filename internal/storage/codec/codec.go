// Codec interface plus the EncodeContext, ScratchPool, PageFacts, and ErrSkip plumbing the
// cascade and codec implementations share. Codecs self-register via init().
package codec

import (
	"errors"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// Codecs return ErrSkip when their preconditions don't fit the Vec. Other
// errors are fatal and stop the cascade.
var ErrSkip = errors.New("codec: not applicable to this Vec")

type EncodeContext struct {
	Scratch       *ScratchPool
	Facts         *PageFacts
	MaxEncodedLen int
}

// Producer fills IntFacts or VarBytesFacts and codecs fall back to scanning when Facts is nil.
type PageFacts struct {
	Rows     int
	Nulls    int
	Kind     vector.VecKind
	Int      *IntFacts
	VarBytes *VarBytesFacts
}

type IntFacts struct {
	Min          int64
	Max          int64
	Sum          int64
	SumOverflow  bool
	ConstantOK   bool
	SequenceOK   bool
	SequenceStep int64
	ForBase      int64
	ForWidth     int
	First        int64
	DeltaBase    int64
	DeltaWidth   int
	DeltaOK      bool
}

type VarBytesFacts struct {
	DistinctCount int
	HistTruncated bool
	// DictFits is true only when every row in the page mapped to one of less than 256 entries.
	// Dictionary codec reads these to skip its own buildDict scan.
	DictFits    bool
	DictBytes   int
	DictEntries [][]byte
	DictIndices []byte
}

// Codecs may mutate trial, u64s, and dictMap. Best is reserved for the
// cascade. Returned payloads must alias trial or fresh bytes, never best.
type ScratchPool struct {
	trial   []byte
	best    []byte
	u64s    []uint64
	dictMap map[string]uint8
}

func NewScratchPool() *ScratchPool { return &ScratchPool{} }

func (s *ScratchPool) Trial() []byte {
	if s == nil {
		return nil
	}
	return s.trial[:0]
}

func (s *ScratchPool) SaveTrial(p []byte) {
	if s != nil {
		s.trial = p
	}
}

// U64s returns a []uint64 of at least n elements, reusing the underlying
// array. Used by FOR/Delta for residuals.
func (s *ScratchPool) U64s(n int) []uint64 {
	if s == nil {
		return make([]uint64, n)
	}
	if cap(s.u64s) < n {
		s.u64s = make([]uint64, n)
	}
	return s.u64s[:n]
}

func (s *ScratchPool) DictMap() map[string]uint8 {
	if s == nil {
		return make(map[string]uint8, 16)
	}
	if s.dictMap == nil {
		s.dictMap = make(map[string]uint8, 16)
		return s.dictMap
	}
	clear(s.dictMap)
	return s.dictMap
}

// Nil-safe so test callers can pass Encode(v, nil) without a setup ritual.
func ctxTrial(ctx *EncodeContext) []byte {
	if ctx == nil || ctx.Scratch == nil {
		return nil
	}
	return ctx.Scratch.Trial()
}

func ctxU64s(ctx *EncodeContext, n int) []uint64 {
	if ctx == nil || ctx.Scratch == nil {
		return make([]uint64, n)
	}
	return ctx.Scratch.U64s(n)
}

func ctxMaxEncodedLen(ctx *EncodeContext) (int, bool) {
	if ctx == nil || ctx.MaxEncodedLen <= 0 {
		return 0, false
	}
	return ctx.MaxEncodedLen, true
}

type Codec interface {
	Encoding() schema.Encoding
	Encode(v vector.Vec, ctx *EncodeContext) (payload []byte, err error)
	Decode(payload []byte, kind vector.VecKind, rows int, nullCount int, dst *vector.Vec) error
}

var registry = map[schema.Encoding]Codec{}

func Register(c Codec) {
	e := c.Encoding()
	if e == schema.EncInvalid {
		panic("codec: cannot register EncInvalid sentinel")
	}
	if _, dup := registry[e]; dup {
		panic(fmt.Sprintf("codec: %v already registered", e))
	}
	registry[e] = c
}

func Lookup(e schema.Encoding) (Codec, error) {
	c, ok := registry[e]
	if !ok {
		return nil, fmt.Errorf("codec: no codec registered for %v", e)
	}
	return c, nil
}

// Test-only helper. Production code calls Encode directly via cascade.
func Estimate(c Codec, v vector.Vec) (int, bool) {
	ctx := &EncodeContext{Scratch: NewScratchPool()}
	p, err := c.Encode(v, ctx)
	if errors.Is(err, ErrSkip) {
		return 0, false
	}
	if err != nil {
		return 0, false
	}
	return len(p), true
}
