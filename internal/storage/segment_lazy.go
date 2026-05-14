package storage

import (
	"fmt"
	"io"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type byteRange struct{ start, end int }

// LoadColumn parses column i's heavy fields (Pages, UUID/Text/Int*Values
// stats) on first call. sync.Once-gated and safe for concurrent callers.
// No-op when not lazily decoded (e.g. metas from WriteSegment).
func (m *SegmentMeta) LoadColumn(i int) error {
	if m == nil || i < 0 || i >= len(m.lazyOnce) {
		return nil
	}
	m.lazyOnce[i].Do(func() {
		rng := m.lazyRanges[i]
		if rng.start >= rng.end {
			return
		}
		r := segmentMetaReader{data: m.lazyBody[rng.start:rng.end], version: m.lazyVersion}
		if err := parseColumnHeavy(&r, &m.Columns[i]); err != nil {
			m.lazyErrs[i] = err
		}
	})
	return m.lazyErrs[i]
}

// LoadAllColumns loads every column's heavy fields and resets lazy state so
// the meta compares equal to one returned by WriteSegment. Not safe to call
// concurrently with LoadColumn on the same SegmentMeta.
func (m *SegmentMeta) LoadAllColumns() error {
	if m == nil {
		return nil
	}
	for i := range m.Columns {
		if err := m.LoadColumn(i); err != nil {
			return err
		}
	}
	m.lazyBody = nil
	m.lazyRanges = nil
	m.lazyOnce = nil
	m.lazyErrs = nil
	m.lazyVersion = 0
	return nil
}

func parseColumnHeavy(r *segmentMetaReader, col *ColumnMeta) error {
	col.UUID = r.readUUIDStats()
	col.Text = r.readTextStats()
	if r.version >= 3 {
		col.Int32Values = r.readInt32ValueStats()
		col.Int64Values = r.readInt64ValueStats()
	}
	pages := r.readU32()
	if r.err != nil {
		return fmt.Errorf("segment footer truncated: %w", r.err)
	}
	col.Pages = make([]PageMeta, int(pages))
	for j := range col.Pages {
		page := &col.Pages[j]
		page.RowStart = r.readU32()
		page.Rows = r.readU32()
		page.NullCount = r.readU32()
		page.Offset = r.readU64()
		page.Length = r.readU64()
		page.Kind = types.VecKind(r.readByte())
		page.Encoding = types.Encoding(r.readByte())
		page.AllValid = r.readBool()
		page.AllNull = r.readBool()
		page.Bool = r.readBoolStats()
		page.Int32 = r.readInt32Stats()
		page.Int64 = r.readInt64Stats()
		page.Int32Values = r.readInt32ValueStats()
		page.Int64Values = r.readInt64ValueStats()
		page.UUID = r.readUUIDStats()
		page.Text = r.readTextStats()
	}
	if r.err != nil {
		return fmt.Errorf("segment footer truncated: %w", r.err)
	}
	return nil
}

// skip and skip*Stats advance over heavy fields without allocating. Used by
// unmarshalSegmentMeta to defer per-column heavy decoding until LoadColumn.
// Byte counts match the corresponding write/read helpers in segment.go.

func (r *segmentMetaReader) skip(n int) {
	if r.err != nil {
		return
	}
	if n < 0 || r.pos+n > len(r.data) {
		r.err = io.ErrUnexpectedEOF
		return
	}
	r.pos += n
}

func (r *segmentMetaReader) skipString() { r.skip(int(r.readU32())) }
func (r *segmentMetaReader) skipBoolStats() {
	if r.readBool() {
		r.skip(2)
	}
}
func (r *segmentMetaReader) skipInt32Stats() {
	if r.readBool() {
		r.skip(17)
	}
}
func (r *segmentMetaReader) skipInt64Stats() {
	if r.readBool() {
		r.skip(25)
	}
}
func (r *segmentMetaReader) skipUUIDStats() {
	if r.readBool() {
		r.skip(int(r.readU32()) * 8)
	}
}

func (r *segmentMetaReader) skipInt32ValueStats() { r.skipValueStats(4) }
func (r *segmentMetaReader) skipInt64ValueStats() { r.skipValueStats(8) }

func (r *segmentMetaReader) skipValueStats(elementBytes int) {
	if !r.readBool() {
		return
	}
	r.skip(1)
	r.skip(int(r.readU32()) * elementBytes)
	if r.version >= 3 {
		r.skip(int(r.readU32()) * 8)
	}
}

func (r *segmentMetaReader) skipTextStats() {
	if !r.readBool() {
		return
	}
	r.skip(9)
	values := r.readU32()
	for range values {
		r.skipString()
		r.skip(4)
	}
	r.skip(int(r.readU32()) * 8)
	if r.version < 2 {
		return
	}
	groups := r.readU32()
	for range groups {
		r.skipString()
		r.skip(int(r.readU32()) * 8)
	}
	siblings := r.readU32()
	for range siblings {
		r.skipString()
		vc := r.readU32()
		for range vc {
			r.skipString()
			r.skip(int(r.readU32()) * 8)
		}
	}
}

func (r *segmentMetaReader) skipPageMetaCapturingRows() uint32 {
	r.skip(4)
	rows := r.readU32()
	r.skip(24)
	r.skipBoolStats()
	r.skipInt32Stats()
	r.skipInt64Stats()
	r.skipInt32ValueStats()
	r.skipInt64ValueStats()
	r.skipUUIDStats()
	r.skipTextStats()
	return rows
}
