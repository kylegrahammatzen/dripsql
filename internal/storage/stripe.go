// Coalesced page reads for sequential scans. A stripe is one pread covering a run of
// consecutive needed pages of one column, served back page by page from memory.
package storage

import (
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// stripeMaxBytes bounds one stripe pread so sparse scans never prefetch unbounded bytes.
const stripeMaxBytes = 1 << 20

type colStripe struct {
	firstPage int
	lastPage  int
	baseOff   uint64
	buf       []byte
}

type stripeSet struct {
	seg    *Segment
	needed []bool
	cols   []colStripe
}

// raw returns the full payload bytes for one page, loading a coalesced stripe on miss.
// The slice aliases the stripe buffer and stays valid until the column's next load.
func (ss *stripeSet) raw(ci, pi int) ([]byte, error) {
	page := ss.seg.Cols[ci].Pages[pi]
	if page.PayloadLength == 0 {
		return nil, nil
	}
	st := &ss.cols[ci]
	if st.buf == nil || pi < st.firstPage || pi > st.lastPage {
		if err := ss.load(ci, pi); err != nil {
			return nil, err
		}
	}
	rel := page.PayloadOffset - st.baseOff
	return st.buf[rel : rel+page.PayloadLength], nil
}

// load reads one stripe starting at pi, extended across following needed pages while
// payloads stay byte-contiguous and the stripe stays under stripeMaxBytes.
func (ss *stripeSet) load(ci, pi int) error {
	pages := ss.seg.Cols[ci].Pages
	start := pages[pi].PayloadOffset
	end := start + pages[pi].PayloadLength
	last := pi
	for next := pi + 1; next < len(pages); next++ {
		if next < len(ss.needed) && !ss.needed[next] {
			break
		}
		p := pages[next]
		if p.PayloadLength == 0 {
			last = next
			continue
		}
		if p.PayloadOffset != end || end+p.PayloadLength-start > stripeMaxBytes {
			break
		}
		end += p.PayloadLength
		last = next
	}
	size := int(end - start)
	st := &ss.cols[ci]
	if cap(st.buf) < size {
		st.buf = make([]byte, size)
	} else {
		st.buf = st.buf[:size]
	}
	ioStart := nowNanos()
	if _, err := ss.seg.f.ReadAt(st.buf, int64(start)); err != nil {
		st.buf = nil
		return fmt.Errorf("scan: read stripe col %d pages [%d,%d]: %w", ci, pi, last, err)
	}
	addIORead(nowNanos() - ioStart)
	st.firstPage = pi
	st.lastPage = last
	st.baseOff = start
	return nil
}

// pageSource hands predicate evaluators their payload bytes either straight from the
// segment or from a scan's stripe buffers. The embedded Segment keeps stats access working.
type pageSource struct {
	*Segment
	stripes *stripeSet
}

func (p pageSource) ReadPagePayload(colIdx, pageIdx int, scratch []byte) ([]byte, vector.Validity, bool, []byte, error) {
	if p.stripes == nil {
		return p.Segment.ReadPagePayload(colIdx, pageIdx, scratch)
	}
	raw, err := p.stripes.raw(colIdx, pageIdx)
	if err != nil {
		return nil, nil, false, scratch, err
	}
	page := p.Cols[colIdx].Pages[pageIdx]
	inner, valid, allNull, err := parsePageBytes(page, raw)
	return inner, valid, allNull, scratch, err
}
