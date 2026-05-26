// Per-page summary facts that codecs may read to skip their own row scan.
// Producer fills IntFacts or VarBytesFacts and codecs fall back to scanning when Facts is nil.
package codec

import (
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

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
	// Per-page dictionary state. DictFits is true only when every row in the
	// page mapped to one of <= 256 entries. Dictionary codec reads these to
	// skip its own buildDict scan.
	DictFits    bool
	DictBytes   int
	DictEntries [][]byte
	DictIndices []byte
}
