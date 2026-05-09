// Package storage contains DripSQL's immutable columnar segment store.
package storage

import (
	"fmt"
	"path/filepath"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

const (
	DefaultPageRows    = vector.StandardBatchRows
	DefaultSegmentRows = 64 * vector.StandardBatchRows
	TextStatsMaxValues = 64
	TextStatsMaxBytes  = 4096
)

type SegmentID uint64

type Codec uint8

const (
	CodecInvalid Codec = iota
	CodecPlain
	CodecConstant
	CodecRLE
	CodecBitPacked
	CodecFrameOfReference
	CodecDictionary
)

type SegmentMeta struct {
	ID            SegmentID             `json:"id"`
	TableID       catalog.TableID       `json:"table_id"`
	SchemaVersion catalog.SchemaVersion `json:"schema_version"`
	Path          string                `json:"path"`
	Rows          uint32                `json:"rows"`
	PageRows      uint32                `json:"page_rows"`
	Columns       []ColumnMeta          `json:"columns"`
}

// AbsPath returns the absolute on-disk path of the segment file, given the
// table directory. SegmentMeta.Path is stored relative (slash-form) so it
// stays portable across platforms; this method composes the absolute path on
// demand instead of caching it on the struct.
func (m SegmentMeta) AbsPath(dir string) string {
	return filepath.Join(dir, filepath.FromSlash(m.Path))
}

type ColumnMeta struct {
	ColumnID   catalog.ColumnID `json:"column_id"`
	Name       string           `json:"name"`
	Type       sqltype.Type     `json:"type"`
	EnumLabels []string         `json:"enum_labels,omitempty"`
	Codec      Codec            `json:"codec"`
	Rows       uint32           `json:"rows"`
	NullCount  uint32           `json:"null_count"`
	AllValid   bool             `json:"all_valid"`
	AllNull    bool             `json:"all_null"`
	Int32      *Int32Stats      `json:"int32,omitempty"`
	Int64      *Int64Stats      `json:"int64,omitempty"`
	Text       *TextStats       `json:"text,omitempty"`
	Pages      []PageMeta       `json:"pages"`
}

type PageMeta struct {
	RowStart  uint32      `json:"row_start"`
	Rows      uint32      `json:"rows"`
	Offset    uint64      `json:"offset"`
	Length    uint64      `json:"length"`
	Codec     Codec       `json:"codec,omitempty"`
	NullCount uint32      `json:"null_count"`
	AllValid  bool        `json:"all_valid"`
	AllNull   bool        `json:"all_null"`
	Int32     *Int32Stats `json:"int32,omitempty"`
	Int64     *Int64Stats `json:"int64,omitempty"`
	Text      *TextStats  `json:"text,omitempty"`
}

type Int32Stats struct {
	Min int32 `json:"min"`
	Max int32 `json:"max"`
}

func (s Int32Stats) MayContain(value int32) bool {
	return s.Min <= value && value <= s.Max
}

func (s Int32Stats) MayIntersect(lo int32, hi int32) bool {
	return s.Min <= hi && lo <= s.Max
}

// observeInt32 folds v into s. A nil s is treated as "no values yet" and a
// fresh Int32Stats with Min=Max=v is returned. The returned pointer is always
// non-nil after observation, so callers can chain across loop iterations.
func observeInt32(s *Int32Stats, v int32) *Int32Stats {
	if s == nil {
		return &Int32Stats{Min: v, Max: v}
	}
	if v < s.Min {
		s.Min = v
	}
	if v > s.Max {
		s.Max = v
	}
	return s
}

type Int64Stats struct {
	Min int64 `json:"min"`
	Max int64 `json:"max"`
}

func (s Int64Stats) MayContain(value int64) bool {
	return s.Min <= value && value <= s.Max
}

func (s Int64Stats) MayIntersect(lo int64, hi int64) bool {
	return s.Min <= hi && lo <= s.Max
}

// observeInt64 folds v into s. See observeInt32 for nil semantics.
func observeInt64(s *Int64Stats, v int64) *Int64Stats {
	if s == nil {
		return &Int64Stats{Min: v, Max: v}
	}
	if v < s.Min {
		s.Min = v
	}
	if v > s.Max {
		s.Max = v
	}
	return s
}

type TextStats struct {
	Complete bool             `json:"complete"`
	Values   []TextValueCount `json:"values,omitempty"`
}

type TextValueCount struct {
	Value string `json:"value"`
	Count uint32 `json:"count"`
}

func (s TextStats) Count(value string) (uint32, bool) {
	if !s.Complete {
		return 0, false
	}
	for _, item := range s.Values {
		if item.Value == value {
			return item.Count, true
		}
	}
	return 0, true
}

func (s TextStats) MayContain(value string) bool {
	count, ok := s.Count(value)
	return !ok || count != 0
}

// mergeInt64Stats folds next into current in place and returns current. Either
// argument may be nil; a non-nil next with a nil current produces a new owned
// copy so the caller's stats slot can hold the result.
func mergeInt64Stats(current *Int64Stats, next *Int64Stats) *Int64Stats {
	if next == nil {
		return current
	}
	if current == nil {
		merged := *next
		return &merged
	}
	if next.Min < current.Min {
		current.Min = next.Min
	}
	if next.Max > current.Max {
		current.Max = next.Max
	}
	return current
}

// mergeInt32Stats folds next into current in place and returns current.
// See mergeInt64Stats for nil semantics.
func mergeInt32Stats(current *Int32Stats, next *Int32Stats) *Int32Stats {
	if next == nil {
		return current
	}
	if current == nil {
		merged := *next
		return &merged
	}
	if next.Min < current.Min {
		current.Min = next.Min
	}
	if next.Max > current.Max {
		current.Max = next.Max
	}
	return current
}

// mergeTextStats folds next into current. If either side is incomplete the
// result is incomplete; otherwise values are unioned subject to the
// TextStatsMaxValues / TextStatsMaxBytes caps.
func mergeTextStats(current *TextStats, next *TextStats) *TextStats {
	if next == nil {
		return current
	}
	if !next.Complete {
		return incompleteTextStats()
	}
	if current == nil {
		merged := TextStats{Complete: true, Values: append([]TextValueCount(nil), next.Values...)}
		return &merged
	}
	if !current.Complete {
		return current
	}
	counts := make(map[string]uint32, len(current.Values)+len(next.Values))
	ordered := make([]string, 0, len(current.Values))
	totalBytes := 0
	for _, item := range current.Values {
		counts[item.Value] = item.Count
		ordered = append(ordered, item.Value)
		totalBytes += len(item.Value)
	}
	for _, item := range next.Values {
		if _, ok := counts[item.Value]; !ok {
			if len(ordered) == TextStatsMaxValues || totalBytes+len(item.Value) > TextStatsMaxBytes {
				return incompleteTextStats()
			}
			ordered = append(ordered, item.Value)
			totalBytes += len(item.Value)
		}
		counts[item.Value] += item.Count
	}
	current.Values = current.Values[:0]
	for _, value := range ordered {
		current.Values = append(current.Values, TextValueCount{Value: value, Count: counts[value]})
	}
	return current
}

func incompleteTextStats() *TextStats {
	return &TextStats{Complete: false}
}

// buildTextStats observes a page's text values and returns either complete
// histogram stats (when within size limits) or an incomplete marker. Used by
// the encoder to attach per-page stats during segment seal.
func buildTextStats(values vector.VarBytes, valid vector.Validity, start int, rows int) *TextStats {
	stats := &TextStats{Complete: true}
	totalBytes := 0
	for row := 0; row < rows; row++ {
		if !vector.IsValid(valid, row) {
			continue
		}
		valueBytes := values.Bytes(start + row)
		if index := textStatsIndex(stats.Values, valueBytes); index >= 0 {
			stats.Values[index].Count++
			continue
		}
		if len(stats.Values) == TextStatsMaxValues || totalBytes+len(valueBytes) > TextStatsMaxBytes {
			return incompleteTextStats()
		}
		value := string(valueBytes)
		stats.Values = append(stats.Values, TextValueCount{Value: value, Count: 1})
		totalBytes += len(valueBytes)
	}
	return stats
}

func textStatsIndex(values []TextValueCount, value []byte) int {
	for i, item := range values {
		if bytesEqualString(value, item.Value) {
			return i
		}
	}
	return -1
}

// maySegmentMatch returns false when the column's segment-level stats prove
// the predicate cannot match any row, allowing the caller to skip the segment.
// A true result is conservative: the segment may or may not match.
func maySegmentMatch(col ColumnMeta, pred Predicate) bool {
	if col.AllNull {
		return false
	}
	switch col.Type.Kind {
	case sqltype.KindInt16, sqltype.KindInt32, sqltype.KindDate:
		if col.Int32 == nil {
			return true
		}
		return mayInt32StatsMatch(*col.Int32, pred)
	case sqltype.KindInt64, sqltype.KindTimestamp:
		if col.Int64 == nil {
			return true
		}
		return mayInt64StatsMatch(*col.Int64, pred)
	case sqltype.KindText:
		if col.Text == nil {
			return true
		}
		return mayTextStatsMatch(col.Text, pred)
	default:
		return true
	}
}

// mayPageMatch is the per-page analogue of maySegmentMatch; col is the
// containing column so the kind dispatch can pick the right stats field.
func mayPageMatch(col ColumnMeta, page PageMeta, pred Predicate) bool {
	if page.AllNull {
		return false
	}
	switch col.Type.Kind {
	case sqltype.KindInt16, sqltype.KindInt32, sqltype.KindDate:
		if page.Int32 == nil {
			return true
		}
		return mayInt32StatsMatch(*page.Int32, pred)
	case sqltype.KindInt64, sqltype.KindTimestamp:
		if page.Int64 == nil {
			return true
		}
		return mayInt64StatsMatch(*page.Int64, pred)
	case sqltype.KindText:
		if page.Text == nil {
			return true
		}
		return mayTextStatsMatch(page.Text, pred)
	default:
		return true
	}
}

// mayTextStatsMatch checks pred against text stats; only text-eq and text-in
// can be pruned, everything else is conservatively true.
func mayTextStatsMatch(stats *TextStats, pred Predicate) bool {
	switch pred.Op {
	case PredicateOpEq:
		return stats.MayContain(pred.Text)
	case PredicateOpIn:
		return textStatsMayContainAny(stats, pred.Texts)
	default:
		return true
	}
}

func textStatsMayContainAny(stats *TextStats, values []string) bool {
	for _, value := range values {
		if stats.MayContain(value) {
			return true
		}
	}
	return false
}

func mayInt64StatsMatch(stats Int64Stats, pred Predicate) bool {
	switch pred.Op {
	case PredicateOpEq:
		return stats.MayContain(pred.Int64)
	case PredicateOpBetween:
		return stats.MayIntersect(pred.Lo, pred.Hi)
	default:
		return true
	}
}

func mayInt32StatsMatch(stats Int32Stats, pred Predicate) bool {
	switch pred.Op {
	case PredicateOpEq:
		return stats.MayContain(pred.Int32)
	case PredicateOpBetween:
		return stats.MayIntersect(pred.Lo32, pred.Hi32)
	default:
		return true
	}
}

func supportedType(t sqltype.Type) bool {
	return t.Kind == sqltype.KindBool || t.Kind == sqltype.KindInt16 || t.Kind == sqltype.KindInt32 || t.Kind == sqltype.KindInt64 || t.Kind == sqltype.KindFloat32 || t.Kind == sqltype.KindFloat64 || t.Kind == sqltype.KindText || t.Kind == sqltype.KindBytes || t.Kind == sqltype.KindUUID || t.Kind == sqltype.KindTimestamp || t.Kind == sqltype.KindDate || t.Kind == sqltype.KindNamed
}

func vectorKindForType(t sqltype.Type) (vector.Kind, error) {
	switch t.Kind {
	case sqltype.KindBool:
		return vector.Bool, nil
	case sqltype.KindInt16:
		return vector.Int16, nil
	case sqltype.KindInt32:
		return vector.Int32, nil
	case sqltype.KindInt64:
		return vector.Int64, nil
	case sqltype.KindFloat32:
		return vector.Float32, nil
	case sqltype.KindFloat64:
		return vector.Float64, nil
	case sqltype.KindTimestamp:
		return vector.Timestamp, nil
	case sqltype.KindDate:
		return vector.Date, nil
	case sqltype.KindText:
		return vector.Text, nil
	case sqltype.KindBytes:
		return vector.Bytes, nil
	case sqltype.KindUUID:
		return vector.UUID, nil
	case sqltype.KindNamed:
		return vector.Enum32, nil
	default:
		return vector.Invalid, fmt.Errorf("unsupported storage type %s", t)
	}
}

// AccessCode names the storage path taken for one referenced column during a
// query. Engine code maps an AccessCode to the human-readable label used by
// the explain renderer (see internal/explain/access.go for the canonical
// strings).
type AccessCode uint8

const (
	AccessNone AccessCode = iota
	AccessSegmentMinMax
	AccessPageMinMax
	AccessTextSummary
	AccessRawCountLoop
	AccessRawIntSumLoop
	AccessRawIntMinMaxLoop
	AccessTextPayloadGrouped
	AccessGroupedScanCount
	AccessPayloadForExpr
	AccessCountedAfterRowFilter
	AccessMetadataAnswered
	AccessMetadataNonNullCount
)

// ExecStats accumulates execution counters for a single query. A nil
// ExecStats means "do not collect"; the hot scan/count/sum/group paths must
// stay zero-allocation when stats is nil. Callers attach an ExecStats to
// QueryScratch before invoking Count / SumInt / GroupStringCounts / etc.; the
// counters are populated at segment and page granularity, never per row.
type ExecStats struct {
	SegmentsTotal     uint64
	SegmentsCandidate uint64
	PagesTotal        uint64
	PagesCandidate    uint64
	RowsTotal         uint64
	RowsCandidate     uint64
	RowsMatched       uint64
	PayloadBytesRead  uint64
	PredPayloadBytes  uint64
	AggPayloadBytes   uint64
	MetadataBytesRead uint64
	MetadataAnswered  uint64
	PerColumn         map[catalog.ColumnID]AccessCode
}

// SetAccess records an access decision for col when stats is non-nil. The
// PerColumn map is allocated lazily on first set so a fresh ExecStats has no
// map until needed.
func (s *ExecStats) SetAccess(col catalog.ColumnID, code AccessCode) {
	if s == nil {
		return
	}
	if s.PerColumn == nil {
		s.PerColumn = make(map[catalog.ColumnID]AccessCode)
	}
	s.PerColumn[col] = code
}
