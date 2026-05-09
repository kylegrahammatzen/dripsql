package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/bits"
	"os"
	"slices"
	"unsafe"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// QueryScratch carries reusable per-query storage buffers.
// It is intentionally caller-owned; do not copy it after first use.
type QueryScratch struct {
	page  pageScratch
	Stats *ExecStats
}

// Count returns the number of rows matching pred (no predicate when
// pred.Op == PredicateNone). scratch may be nil; when non-nil it is reused
// across page reads to avoid per-call allocations.
func (s *Store) Count(ctx context.Context, table catalog.TableDef, pred Predicate, scratch *QueryScratch) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := s.beginOp(); err != nil {
		return 0, err
	}
	defer s.endOp()
	state, err := s.ensureTableState(table)
	if err != nil {
		return 0, err
	}
	if err := waitBufferIdle(state); err != nil {
		return 0, err
	}

	if pred.Op == PredicateNone {
		var count uint64
		state.mu.RLock()
		for _, segment := range state.segments {
			count += uint64(segment.Rows)
		}
		state.mu.RUnlock()
		buffered, err := countBufferedRows(table, state.buffer, pred)
		state.bufferMu.Unlock()
		if err != nil {
			return 0, err
		}
		return count + buffered, nil
	}

	if err := validatePredicate(table, pred); err != nil {
		state.bufferMu.Unlock()
		return 0, err
	}
	page := &pageScratch{}
	if scratch != nil {
		page = &scratch.page
	}
	state.mu.RLock()
	dir := state.dir
	segments := page.segmentSnapshot(state.segments)
	state.mu.RUnlock()
	buffered, err := countBufferedRows(table, state.buffer, pred)
	state.bufferMu.Unlock()
	if err != nil {
		return 0, err
	}

	count := buffered
	stats := scratchStats(scratch)
	if stats != nil {
		stats.RowsMatched += buffered
	}
	compound := pred.Op == PredicateAnd || pred.Op == PredicateOr || pred.Op == PredicateNot
	for _, segment := range segments {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		var n uint64
		if compound {
			n, err = s.countCompoundSegment(segment.AbsPath(dir), segment, pred, stats)
		} else {
			n, err = s.countLeafSegment(segment.AbsPath(dir), segment, pred, page, stats)
		}
		if err != nil {
			return 0, err
		}
		count += n
	}
	return count, nil
}

// scratchStats returns the optional ExecStats pointer attached to scratch, or
// nil when stats collection is disabled. Hot paths use this once per call to
// hoist the nil-check out of inner loops.
func scratchStats(scratch *QueryScratch) *ExecStats {
	if scratch == nil {
		return nil
	}
	return scratch.Stats
}

// countCompoundSegment counts rows matching an AND/OR/NOT predicate by reading
// every page of every referenced column.
func (s *Store) countCompoundSegment(path string, segment SegmentMeta, pred Predicate, stats *ExecStats) (uint64, error) {
	file, err := s.files.Acquire(path)
	if err != nil {
		return 0, err
	}
	defer func() { _ = s.files.Release(path) }()
	if stats != nil {
		stats.SegmentsTotal++
		stats.SegmentsCandidate++ // compound predicates always read every page
		stats.RowsTotal += uint64(segment.Rows)
	}
	var count uint64
	pageCount := segmentPageCount(segment)
	for pageIndex := 0; pageIndex < pageCount; pageIndex++ {
		n, err := countCompoundPredicatePage(file, segment, pageIndex, pred)
		if err != nil {
			return 0, err
		}
		if stats != nil {
			stats.PagesTotal++
			stats.PagesCandidate++
			rows, bytes := compoundPageWork(segment, pageIndex, pred)
			stats.RowsCandidate += rows
			stats.PayloadBytesRead += bytes
			stats.PredPayloadBytes += bytes
			stats.RowsMatched += uint64(n)
		}
		count += uint64(n)
	}
	return count, nil
}

// compoundPageWork returns the rows and payload bytes touched at one page
// across all columns referenced by an AND/OR/NOT predicate. Used by the
// stats path; matches the actual read shape of countCompoundPredicatePage.
func compoundPageWork(segment SegmentMeta, pageIndex int, pred Predicate) (rows uint64, bytes uint64) {
	for _, colMeta := range segment.Columns {
		if !predicateReferencesColumn(pred, colMeta.ColumnID) {
			continue
		}
		if pageIndex < len(colMeta.Pages) {
			page := colMeta.Pages[pageIndex]
			if rows == 0 {
				rows = uint64(page.Rows)
			}
			bytes += page.Length
		}
	}
	return rows, bytes
}

// predicateReferencesColumn returns true if id appears in pred's column ID set.
// Compound predicates carry their child predicates in pred.Predicates.
func predicateReferencesColumn(pred Predicate, id catalog.ColumnID) bool {
	if pred.Op == PredicateAnd || pred.Op == PredicateOr || pred.Op == PredicateNot {
		for i := range pred.Predicates {
			if predicateReferencesColumn(pred.Predicates[i], id) {
				return true
			}
		}
		return false
	}
	return pred.ColumnID == id
}

// countLeafSegment counts rows matching a single-column predicate, applying
// segment- and page-level metadata skips before any file read. When stats is
// non-nil the per-segment / per-page counters are populated; the inner page
// kernels never see the stats argument so the zero-allocation count paths
// stay zero-allocation when stats is nil.
func (s *Store) countLeafSegment(path string, segment SegmentMeta, pred Predicate, page *pageScratch, stats *ExecStats) (uint64, error) {
	if stats != nil {
		stats.SegmentsTotal++
		stats.RowsTotal += uint64(segment.Rows)
	}
	colMeta, _, ok := columnMetaByID(segment, pred.ColumnID)
	if !ok {
		return 0, fmt.Errorf("segment %d missing column ID %d", segment.ID, pred.ColumnID)
	}
	if !maySegmentMatch(colMeta, pred) {
		if stats != nil {
			stats.SetAccess(pred.ColumnID, accessSegmentPruneCode(colMeta.Type.Kind))
		}
		return 0, nil
	}
	if stats != nil {
		stats.SegmentsCandidate++
	}
	if n, ok := countPredicateSegmentMeta(colMeta, pred); ok {
		if stats != nil {
			stats.MetadataAnswered++
			stats.RowsMatched += uint64(n)
			stats.SetAccess(pred.ColumnID, AccessTextSummary)
		}
		return uint64(n), nil
	}
	var file *os.File
	acquired := false
	defer func() {
		if acquired {
			_ = s.files.Release(path)
		}
	}()
	var count uint64
	for _, p := range colMeta.Pages {
		if stats != nil {
			stats.PagesTotal++
		}
		if !mayPageMatch(colMeta, p, pred) {
			if stats != nil {
				stats.SetAccess(pred.ColumnID, accessPagePruneCode(colMeta.Type.Kind))
			}
			continue
		}
		if stats != nil {
			stats.PagesCandidate++
			stats.RowsCandidate += uint64(p.Rows)
		}
		if n, ok := countPredicatePageMeta(colMeta, p, pred); ok {
			count += uint64(n)
			if stats != nil {
				stats.MetadataAnswered++
				stats.RowsMatched += uint64(n)
				stats.SetAccess(pred.ColumnID, AccessTextSummary)
			}
			continue
		}
		if !acquired {
			var err error
			file, err = s.files.Acquire(path)
			if err != nil {
				return 0, err
			}
			acquired = true
		}
		n, err := countPredicatePage(file, colMeta, p, pred, page)
		if err != nil {
			return 0, err
		}
		count += uint64(n)
		if stats != nil {
			stats.PayloadBytesRead += p.Length
			stats.PredPayloadBytes += p.Length
			stats.RowsMatched += uint64(n)
			stats.SetAccess(pred.ColumnID, AccessRawCountLoop)
		}
	}
	return count, nil
}

// accessSegmentPruneCode returns the AccessCode that best names a
// segment-level metadata skip on a column of the given kind.
func accessSegmentPruneCode(kind sqltype.Kind) AccessCode {
	if kind == sqltype.KindText {
		return AccessTextSummary
	}
	return AccessSegmentMinMax
}

func accessPagePruneCode(kind sqltype.Kind) AccessCode {
	if kind == sqltype.KindText {
		return AccessTextSummary
	}
	return AccessPageMinMax
}

func countCompoundPredicatePage(file *os.File, segment SegmentMeta, pageIndex int, pred Predicate) (int, error) {
	metas, err := predicateColumnMetas(segment, pred)
	if err != nil {
		return 0, err
	}
	cols, err := readPredicateColumns(file, metas, pageIndex)
	if err != nil {
		return 0, err
	}
	if len(cols) == 0 {
		return 0, nil
	}
	count := 0
	for row := 0; row < cols[0].V.Len; row++ {
		matched, err := matchPredicateColumns(cols, row, pred)
		if err != nil {
			return 0, err
		}
		if matched {
			count++
		}
	}
	return count, nil
}

// pageScratch holds reusable per-query buffers shared across aggregate paths
// (count, sum, etc.). It lives on QueryScratch and stays caller-owned so
// concurrent queries don't trample each other (`QueryScratch` doc).
type pageScratch struct {
	payload     []byte
	predPayload []byte
	segments    []SegmentMeta
}

func (s *pageScratch) pagePayload(n int) []byte {
	s.payload = ensureLen(s.payload, n)
	return s.payload
}

func (s *pageScratch) predicatePagePayload(n int) []byte {
	s.predPayload = ensureLen(s.predPayload, n)
	return s.predPayload
}

func (s *pageScratch) segmentSnapshot(src []SegmentMeta) []SegmentMeta {
	s.segments = ensureLen(s.segments, len(src))
	copy(s.segments, src)
	return s.segments
}

// countPredicatePage dispatches to the right per-kind kernel; falls back to
// a per-row materialized scan for kinds without a tuned byte-level path.
func countPredicatePage(file *os.File, col ColumnMeta, page PageMeta, pred Predicate, scratch *pageScratch) (int, error) {
	switch col.Type.Kind {
	case sqltype.KindBool:
		if pred.Op == PredicateOpEq {
			return countBoolEqPage(file, col, page, pred.Bool, scratch)
		}
	case sqltype.KindInt32, sqltype.KindDate:
		if pred.Op == PredicateOpEq || pred.Op == PredicateOpBetween {
			return countInt32PredicatePage(file, col, page, pred, scratch)
		}
	case sqltype.KindInt64, sqltype.KindTimestamp:
		return countInt64PredicatePage(file, col, page, pred, scratch)
	case sqltype.KindText:
		if pred.Op == PredicateOpEq && page.Codec != CodecDictionary {
			return countTextEqPage(file, col, page, pred.Text, scratch)
		}
	}
	return countPredicatePageByRows(file, col, page, pred)
}

func countPredicatePageByRows(file *os.File, colMeta ColumnMeta, page PageMeta, pred Predicate) (int, error) {
	col, err := readColumnPageFromFile(file, colMeta, page)
	if err != nil {
		return 0, err
	}
	count := 0
	for row := 0; row < col.V.Len; row++ {
		matched, err := matchPredicateRow(col, row, pred)
		if err != nil {
			return 0, err
		}
		if matched {
			count++
		}
	}
	return count, nil
}

func countBoolEqPage(file *os.File, col ColumnMeta, page PageMeta, rhs bool, scratch *pageScratch) (int, error) {
	payload := scratch.pagePayload(int(page.Length))
	if _, err := file.ReadAt(payload, int64(page.Offset)); err != nil {
		return 0, err
	}
	rows := int(page.Rows)
	pos := 0
	var validBytes []byte
	if !page.AllValid {
		validLen := vector.ValidityWords(rows) * 8
		if len(payload) < validLen {
			return 0, fmt.Errorf("column %q page payload is too short for validity", col.Name)
		}
		validBytes = payload[:validLen]
		pos = validLen
	}
	valueLen := vector.ValidityWords(rows) * 8
	if len(payload)-pos < valueLen {
		return 0, fmt.Errorf("column %q page payload is too short for bool values", col.Name)
	}
	return countBoolEqPayload(payload[pos:pos+valueLen], validBytes, rows, rhs), nil
}

func countInt32PredicatePage(file *os.File, col ColumnMeta, page PageMeta, pred Predicate, scratch *pageScratch) (int, error) {
	payload := scratch.pagePayload(int(page.Length))
	if _, err := file.ReadAt(payload, int64(page.Offset)); err != nil {
		return 0, err
	}
	rows := int(page.Rows)
	pos := 0
	var validBytes []byte
	if !page.AllValid {
		validLen := vector.ValidityWords(rows) * 8
		if len(payload) < validLen {
			return 0, fmt.Errorf("column %q page payload is too short for validity", col.Name)
		}
		validBytes = payload[:validLen]
		pos = validLen
	}
	valueLen := rows * 4
	if len(payload)-pos < valueLen {
		return 0, fmt.Errorf("column %q page payload is too short for int32 values", col.Name)
	}
	return countInt32PredicatePayload(payload[pos:pos+valueLen], validBytes, rows, pred)
}

func countInt64PredicatePage(file *os.File, col ColumnMeta, page PageMeta, pred Predicate, scratch *pageScratch) (int, error) {
	payload := scratch.pagePayload(int(page.Length))
	if _, err := file.ReadAt(payload, int64(page.Offset)); err != nil {
		return 0, err
	}
	rows := int(page.Rows)
	pos := 0
	var validBytes []byte
	if !page.AllValid {
		validLen := vector.ValidityWords(rows) * 8
		if len(payload) < validLen {
			return 0, fmt.Errorf("column %q page payload is too short for validity", col.Name)
		}
		validBytes = payload[:validLen]
		pos = validLen
	}
	valueLen := rows * 8
	if len(payload)-pos < valueLen {
		return 0, fmt.Errorf("column %q page payload is too short for int64 values", col.Name)
	}
	return countInt64PredicatePayload(payload[pos:pos+valueLen], validBytes, rows, pred)
}

func countTextEqPage(file *os.File, col ColumnMeta, page PageMeta, rhs string, scratch *pageScratch) (int, error) {
	payload := scratch.pagePayload(int(page.Length))
	if _, err := file.ReadAt(payload, int64(page.Offset)); err != nil {
		return 0, err
	}
	rows := int(page.Rows)
	pos := 0
	var validBytes []byte
	if !page.AllValid {
		validLen := vector.ValidityWords(rows) * 8
		if len(payload) < validLen {
			return 0, fmt.Errorf("column %q page payload is too short for validity", col.Name)
		}
		validBytes = payload[:validLen]
		pos = validLen
	}
	offsetLen := (rows + 1) * 4
	if len(payload)-pos < offsetLen {
		return 0, fmt.Errorf("column %q page payload is too short for text offsets", col.Name)
	}
	offsets := payload[pos : pos+offsetLen]
	data := payload[pos+offsetLen:]
	return countTextEqPayload(offsets, data, validBytes, rows, rhs)
}

func countTextEqPayload(offsets []byte, data []byte, valid []byte, rows int, rhs string) (int, error) {
	if rows == 0 {
		return 0, nil
	}
	if !alignedFast4(offsets) {
		return countTextEqPayloadScalar(offsets, data, valid, rows, rhs)
	}
	wantLen := uint32(len(rhs))
	dataLen := uint32(len(data))
	offs := unsafe.Slice((*uint32)(unsafe.Pointer(&offsets[0])), rows+1)
	count := 0
	if len(valid) == 0 {
		for row := 0; row < rows; row++ {
			start := offs[row]
			end := offs[row+1]
			if start > end || end > dataLen {
				return 0, fmt.Errorf("text offsets are out of bounds")
			}
			if end-start != wantLen {
				continue
			}
			if bytesEqualStringUnsafe(data[start:end], rhs) {
				count++
			}
		}
		return count, nil
	}
	for row := 0; row < rows; row++ {
		if !validByte(valid, row) {
			continue
		}
		start := offs[row]
		end := offs[row+1]
		if start > end || end > dataLen {
			return 0, fmt.Errorf("text offsets are out of bounds")
		}
		if end-start != wantLen {
			continue
		}
		if bytesEqualStringUnsafe(data[start:end], rhs) {
			count++
		}
	}
	return count, nil
}

func countTextEqPayloadScalar(offsets []byte, data []byte, valid []byte, rows int, rhs string) (int, error) {
	count := 0
	for row := 0; row < rows; row++ {
		if len(valid) != 0 && !validByte(valid, row) {
			continue
		}
		start := int(binary.LittleEndian.Uint32(offsets[row*4:]))
		end := int(binary.LittleEndian.Uint32(offsets[(row+1)*4:]))
		if start > end || end > len(data) {
			return 0, fmt.Errorf("text offsets are out of bounds")
		}
		if bytesEqualString(data[start:end], rhs) {
			count++
		}
	}
	return count, nil
}

// bytesEqualStringUnsafe reinterprets the byte slice header as a string so the compiler emits runtime.memequal without a copy.
func bytesEqualStringUnsafe(data []byte, s string) bool {
	if len(data) != len(s) {
		return false
	}
	return *(*string)(unsafe.Pointer(&data)) == s
}

func validByte(valid []byte, row int) bool {
	word := binary.LittleEndian.Uint64(valid[(row>>6)*8:])
	return word&(uint64(1)<<uint(row&63)) != 0
}

func bytesEqualString(data []byte, s string) bool {
	if len(data) != len(s) {
		return false
	}
	for i, b := range data {
		if b != s[i] {
			return false
		}
	}
	return true
}

func countBoolEqPayload(values []byte, valid []byte, rows int, rhs bool) int {
	count := 0
	for base := 0; base < rows; base += 64 {
		word := binary.LittleEndian.Uint64(values[(base>>6)*8:])
		limit := min(64, rows-base)
		if limit < 64 {
			word &= (uint64(1) << uint(limit)) - 1
		}
		if len(valid) != 0 {
			validWord := binary.LittleEndian.Uint64(valid[(base>>6)*8:])
			if limit < 64 {
				validWord &= (uint64(1) << uint(limit)) - 1
			}
			if rhs {
				count += bits.OnesCount64(word & validWord)
			} else {
				count += bits.OnesCount64(^word & validWord)
			}
			continue
		}
		set := bits.OnesCount64(word)
		if rhs {
			count += set
		} else {
			count += limit - set
		}
	}
	return count
}

func countInt64PredicatePayload(values []byte, valid []byte, rows int, pred Predicate) (int, error) {
	switch pred.Op {
	case PredicateOpEq:
		if len(valid) == 0 {
			return countInt64EqAllValid(values, rows, pred.Int64), nil
		}
		return countInt64EqValid(values, valid, rows, pred.Int64), nil
	case PredicateOpBetween:
		if len(valid) == 0 {
			return countInt64BetweenAllValid(values, rows, pred.Lo, pred.Hi), nil
		}
		return countInt64BetweenValid(values, valid, rows, pred.Lo, pred.Hi), nil
	case PredicateOpNotEq:
		return countInt64Match(values, valid, rows, func(value int64) bool { return value != pred.Int64 }), nil
	case PredicateOpIn:
		return countInt64Match(values, valid, rows, func(value int64) bool { return slices.Contains(pred.Int64s, value) }), nil
	case PredicateOpNotIn:
		return countInt64Match(values, valid, rows, func(value int64) bool { return !slices.Contains(pred.Int64s, value) }), nil
	case PredicateOpLess:
		return countInt64Match(values, valid, rows, func(value int64) bool { return value < pred.Int64 }), nil
	case PredicateOpLessEqual:
		return countInt64Match(values, valid, rows, func(value int64) bool { return value <= pred.Int64 }), nil
	case PredicateOpGreater:
		return countInt64Match(values, valid, rows, func(value int64) bool { return value > pred.Int64 }), nil
	case PredicateOpGreaterEqual:
		return countInt64Match(values, valid, rows, func(value int64) bool { return value >= pred.Int64 }), nil
	default:
		return 0, fmt.Errorf("unsupported predicate op %d", pred.Op)
	}
}

func countInt64Match(values []byte, valid []byte, rows int, match func(int64) bool) int {
	if rows == 0 {
		return 0
	}
	if !alignedFast8(values) {
		return countInt64MatchScalar(values, valid, rows, match)
	}
	xs := unsafe.Slice((*int64)(unsafe.Pointer(&values[0])), rows)
	count := 0
	if len(valid) == 0 {
		for i := 0; i < rows; i++ {
			count += b2i(match(xs[i]))
		}
		return count
	}
	for base := 0; base < rows; base += 64 {
		word := binary.LittleEndian.Uint64(valid[(base>>6)*8:])
		limit := min(64, rows-base)
		for bit := 0; bit < limit; bit++ {
			if word&(uint64(1)<<uint(bit)) == 0 {
				continue
			}
			count += b2i(match(xs[base+bit]))
		}
	}
	return count
}

func countInt64MatchScalar(values []byte, valid []byte, rows int, match func(int64) bool) int {
	count := 0
	for row := 0; row < rows; row++ {
		if len(valid) != 0 && !validByte(valid, row) {
			continue
		}
		pos := row * 8
		count += b2i(match(int64(binary.LittleEndian.Uint64(values[pos : pos+8]))))
	}
	return count
}

func countInt32PredicatePayload(values []byte, valid []byte, rows int, pred Predicate) (int, error) {
	switch pred.Op {
	case PredicateOpEq:
		if len(valid) == 0 {
			return countInt32EqAllValid(values, rows, pred.Int32), nil
		}
		return countInt32EqValid(values, valid, rows, pred.Int32), nil
	case PredicateOpBetween:
		if len(valid) == 0 {
			return countInt32BetweenAllValid(values, rows, pred.Lo32, pred.Hi32), nil
		}
		return countInt32BetweenValid(values, valid, rows, pred.Lo32, pred.Hi32), nil
	default:
		return 0, fmt.Errorf("unsupported predicate op %d", pred.Op)
	}
}

// b2i is the standard branch-to-int trick. The compiler emits a SETcc + ADD
// instead of a conditional jump, so per-row mispredicts on random data go away.
func b2i(cond bool) int {
	if cond {
		return 1
	}
	return 0
}

// alignedFast4 reports whether b's data pointer is 4-byte aligned.
// Heap-allocated byte slices satisfy this; the check guards against future
// callers that pass sub-slices at odd offsets (mmap, custom buffers, etc.).
func alignedFast4(b []byte) bool {
	return len(b) == 0 || uintptr(unsafe.Pointer(&b[0]))&3 == 0
}

// alignedFast8 reports whether b's data pointer is 8-byte aligned.
func alignedFast8(b []byte) bool {
	return len(b) == 0 || uintptr(unsafe.Pointer(&b[0]))&7 == 0
}

func countInt32EqAllValid(values []byte, rows int, rhs int32) int {
	if rows == 0 {
		return 0
	}
	if !alignedFast4(values) {
		return countInt32EqAllValidScalar(values, rows, rhs)
	}
	xs := unsafe.Slice((*int32)(unsafe.Pointer(&values[0])), rows)
	count := 0
	i := 0
	for ; i+8 <= rows; i += 8 {
		count += b2i(xs[i] == rhs)
		count += b2i(xs[i+1] == rhs)
		count += b2i(xs[i+2] == rhs)
		count += b2i(xs[i+3] == rhs)
		count += b2i(xs[i+4] == rhs)
		count += b2i(xs[i+5] == rhs)
		count += b2i(xs[i+6] == rhs)
		count += b2i(xs[i+7] == rhs)
	}
	for ; i < rows; i++ {
		count += b2i(xs[i] == rhs)
	}
	return count
}

func countInt32EqAllValidScalar(values []byte, rows int, rhs int32) int {
	count := 0
	for row := 0; row < rows; row++ {
		pos := row * 4
		if int32(binary.LittleEndian.Uint32(values[pos:pos+4])) == rhs {
			count++
		}
	}
	return count
}

func countInt32EqValid(values []byte, valid []byte, rows int, rhs int32) int {
	count := 0
	for base := 0; base < rows; base += 64 {
		word := binary.LittleEndian.Uint64(valid[(base>>6)*8:])
		limit := min(64, rows-base)
		for bit := 0; bit < limit; bit++ {
			if word&(uint64(1)<<uint(bit)) == 0 {
				continue
			}
			pos := (base + bit) * 4
			if int32(binary.LittleEndian.Uint32(values[pos:pos+4])) == rhs {
				count++
			}
		}
	}
	return count
}

func countInt32BetweenAllValid(values []byte, rows int, lo int32, hi int32) int {
	count := 0
	for row := 0; row < rows; row++ {
		pos := row * 4
		value := int32(binary.LittleEndian.Uint32(values[pos : pos+4]))
		if lo <= value && value <= hi {
			count++
		}
	}
	return count
}

func countInt32BetweenValid(values []byte, valid []byte, rows int, lo int32, hi int32) int {
	count := 0
	for base := 0; base < rows; base += 64 {
		word := binary.LittleEndian.Uint64(valid[(base>>6)*8:])
		limit := min(64, rows-base)
		for bit := 0; bit < limit; bit++ {
			if word&(uint64(1)<<uint(bit)) == 0 {
				continue
			}
			pos := (base + bit) * 4
			value := int32(binary.LittleEndian.Uint32(values[pos : pos+4]))
			if lo <= value && value <= hi {
				count++
			}
		}
	}
	return count
}

func countInt64EqAllValid(values []byte, rows int, rhs int64) int {
	if rows == 0 {
		return 0
	}
	if !alignedFast8(values) {
		return countInt64EqAllValidScalar(values, rows, rhs)
	}
	xs := unsafe.Slice((*int64)(unsafe.Pointer(&values[0])), rows)
	count := 0
	i := 0
	for ; i+4 <= rows; i += 4 {
		count += b2i(xs[i] == rhs)
		count += b2i(xs[i+1] == rhs)
		count += b2i(xs[i+2] == rhs)
		count += b2i(xs[i+3] == rhs)
	}
	for ; i < rows; i++ {
		count += b2i(xs[i] == rhs)
	}
	return count
}

func countInt64EqAllValidScalar(values []byte, rows int, rhs int64) int {
	count := 0
	for row := 0; row < rows; row++ {
		pos := row * 8
		if int64(binary.LittleEndian.Uint64(values[pos:pos+8])) == rhs {
			count++
		}
	}
	return count
}

// countInt64EqValid uses the typed-slice view + per-validity-word fast paths:
// fully-zero word skips 64 rows in one branch; fully-set word runs the unrolled
// scanner; mixed words fall back to the predictable bit-by-bit loop (preserving
// V1's branch-predictor behavior on regular null patterns).
func countInt64EqValid(values []byte, valid []byte, rows int, rhs int64) int {
	if rows == 0 {
		return 0
	}
	if !alignedFast8(values) {
		return countInt64EqValidScalar(values, valid, rows, rhs)
	}
	xs := unsafe.Slice((*int64)(unsafe.Pointer(&values[0])), rows)
	count := 0
	for base := 0; base < rows; base += 64 {
		validWord := binary.LittleEndian.Uint64(valid[(base>>6)*8:])
		limit := min(64, rows-base)
		if limit < 64 {
			validWord &= (uint64(1) << uint(limit)) - 1
		}
		if validWord == 0 {
			continue
		}
		if validWord == ^uint64(0) {
			i := base
			end := base + 64
			for ; i+4 <= end; i += 4 {
				count += b2i(xs[i] == rhs)
				count += b2i(xs[i+1] == rhs)
				count += b2i(xs[i+2] == rhs)
				count += b2i(xs[i+3] == rhs)
			}
			continue
		}
		for bit := 0; bit < limit; bit++ {
			if validWord&(uint64(1)<<uint(bit)) == 0 {
				continue
			}
			if xs[base+bit] == rhs {
				count++
			}
		}
	}
	return count
}

func countInt64EqValidScalar(values []byte, valid []byte, rows int, rhs int64) int {
	count := 0
	for base := 0; base < rows; base += 64 {
		word := binary.LittleEndian.Uint64(valid[(base>>6)*8:])
		limit := min(64, rows-base)
		for bit := 0; bit < limit; bit++ {
			if word&(uint64(1)<<uint(bit)) == 0 {
				continue
			}
			pos := (base + bit) * 8
			if int64(binary.LittleEndian.Uint64(values[pos:pos+8])) == rhs {
				count++
			}
		}
	}
	return count
}

func countInt64BetweenAllValid(values []byte, rows int, lo int64, hi int64) int {
	if rows == 0 {
		return 0
	}
	if !alignedFast8(values) {
		return countInt64BetweenAllValidScalar(values, rows, lo, hi)
	}
	xs := unsafe.Slice((*int64)(unsafe.Pointer(&values[0])), rows)
	count := 0
	i := 0
	for ; i+4 <= rows; i += 4 {
		count += b2i(xs[i] >= lo && xs[i] <= hi)
		count += b2i(xs[i+1] >= lo && xs[i+1] <= hi)
		count += b2i(xs[i+2] >= lo && xs[i+2] <= hi)
		count += b2i(xs[i+3] >= lo && xs[i+3] <= hi)
	}
	for ; i < rows; i++ {
		count += b2i(xs[i] >= lo && xs[i] <= hi)
	}
	return count
}

func countInt64BetweenAllValidScalar(values []byte, rows int, lo int64, hi int64) int {
	count := 0
	for row := 0; row < rows; row++ {
		pos := row * 8
		value := int64(binary.LittleEndian.Uint64(values[pos : pos+8]))
		if lo <= value && value <= hi {
			count++
		}
	}
	return count
}

func countInt64BetweenValid(values []byte, valid []byte, rows int, lo int64, hi int64) int {
	count := 0
	for base := 0; base < rows; base += 64 {
		word := binary.LittleEndian.Uint64(valid[(base>>6)*8:])
		limit := min(64, rows-base)
		for bit := 0; bit < limit; bit++ {
			if word&(uint64(1)<<uint(bit)) == 0 {
				continue
			}
			pos := (base + bit) * 8
			value := int64(binary.LittleEndian.Uint64(values[pos : pos+8]))
			if lo <= value && value <= hi {
				count++
			}
		}
	}
	return count
}

