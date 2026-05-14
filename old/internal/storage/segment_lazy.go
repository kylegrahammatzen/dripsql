package storage

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type byteRange struct {
	start, end int
	pageDir    int
	pageCount  int
}

// LoadColumn parses column i's heavy fields (Pages, UUID/Text/Int*Values
// stats) on first call. sync.Once-gated and safe for concurrent callers.
// No-op when not lazily decoded (e.g. metas from WriteSegment).
func (m *SegmentMeta) LoadColumn(i int) error {
	if m == nil || i < 0 || i >= len(m.lazyOnce) {
		return nil
	}
	m.lazyOnce[i].Do(func() {
		rng := m.lazyRanges[i]
		col := &m.Columns[i]
		if rng.pageCount > 0 {
			col.Pages = make([]PageMeta, rng.pageCount)
			for j := range col.Pages {
				readPageEntry(m.lazyBody[rng.pageDir+j*segmentPageEntryBytes:], &col.Pages[j])
			}
		}
		if rng.start >= rng.end {
			return
		}
		r := segmentMetaReader{data: m.lazyBody[rng.start:rng.end]}
		if err := parseColumnHeavy(&r, col); err != nil {
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
	return nil
}

func readPageEntry(buf []byte, page *PageMeta) {
	page.RowStart = binary.LittleEndian.Uint32(buf[0:])
	page.Rows = binary.LittleEndian.Uint32(buf[4:])
	page.NullCount = binary.LittleEndian.Uint32(buf[8:])
	page.Offset = binary.LittleEndian.Uint64(buf[12:])
	page.Length = binary.LittleEndian.Uint64(buf[20:])
	page.Kind = types.VecKind(buf[28])
	page.Encoding = types.Encoding(buf[29])
}

func parseColumnHeavy(r *segmentMetaReader, col *ColumnMeta) error {
	col.Stats.UUID = r.readUUIDStats()
	col.Stats.Text = r.readTextStats()
	col.Stats.Int32Values = r.readInt32ValueStats()
	col.Stats.Int64Values = r.readInt64ValueStats()
	if r.err != nil {
		return fmt.Errorf("segment footer truncated: %w", r.err)
	}
	for j := range col.Pages {
		page := &col.Pages[j]
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

// skip advances the cursor past a span of bytes whose length is known but
// whose contents we are not parsing yet (the per-column heavy section).
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
