package storage

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"unsafe"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// GroupAggregate selects the per-key aggregator used by GroupAggregateAny.
type GroupAggregate uint8

const (
	GroupAggregateCountStar GroupAggregate = iota
	GroupAggregateCountNonNull
	GroupAggregateSum
	GroupAggregateMin
	GroupAggregateMax
)

// GroupStringCounts returns count(*) per text key, optionally filtered by pred.
// scratch may be nil; when non-nil it is reused across page reads.
func (s *Store) GroupStringCounts(ctx context.Context, table catalog.TableDef, colID catalog.ColumnID, pred Predicate, scratch *QueryScratch) (map[string]uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.beginOp(); err != nil {
		return nil, err
	}
	defer s.endOp()

	state, err := s.ensureTableState(table)
	if err != nil {
		return nil, err
	}
	if err := waitBufferIdle(state); err != nil {
		return nil, err
	}
	_, groupIndex, err := requireTextColumn(state, table, colID, "GROUP BY")
	if err != nil {
		return nil, err
	}
	predIndexes, err := validatePredicateLocked(state, table, pred)
	if err != nil {
		return nil, err
	}

	counts := make(map[string]uint64)
	if err := processBufferGroupText(state.buffer, groupIndex, -1, predIndexes, pred, counts, stepGroupCountStar); err != nil {
		state.bufferMu.Unlock()
		return nil, err
	}
	dir, segments := snapshotSegments(state)

	page := &pageScratch{}
	if scratch != nil {
		page = &scratch.page
	}
	stats := scratchStats(scratch)
	if stats != nil {
		stats.SetAccess(colID, AccessTextPayloadGrouped)
	}
	for _, segment := range segments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		groupMeta, _, ok := columnMetaByID(segment, colID)
		if !ok {
			return nil, fmt.Errorf("segment %d missing grouped column ID %d", segment.ID, colID)
		}
		predMetas, err := predicateColumnMetas(segment, pred)
		if err != nil {
			return nil, err
		}
		if err := s.addStringGroupCountsForSegment(segment.AbsPath(dir), segment.ID, groupMeta, predMetas, colID, pred, counts, page); err != nil {
			return nil, err
		}
	}
	if stats != nil {
		var total uint64
		for _, n := range counts {
			total += n
		}
		stats.RowsMatched = total
	}
	return counts, nil
}

// GroupStringCountNonNull returns count(non-null countColID) per text key.
func (s *Store) GroupStringCountNonNull(ctx context.Context, table catalog.TableDef, groupColID catalog.ColumnID, countColID catalog.ColumnID, pred Predicate) (map[string]uint64, error) {
	return runGroupStringText(ctx, s, table, groupColID, countColID, pred, "count", false, make(map[string]uint64), stepGroupCountNonNull)
}

// GroupStringSumsInt returns sum(int_col) per text key with overflow detection.
func (s *Store) GroupStringSumsInt(ctx context.Context, table catalog.TableDef, groupColID catalog.ColumnID, sumColID catalog.ColumnID, pred Predicate) (map[string]int64, error) {
	return runGroupStringText(ctx, s, table, groupColID, sumColID, pred, "sum", true, make(map[string]int64), stepGroupSumInt)
}

// GroupStringMinMaxInt returns min (max=false) or max of int_col per text key.
func (s *Store) GroupStringMinMaxInt(ctx context.Context, table catalog.TableDef, groupColID catalog.ColumnID, aggColID catalog.ColumnID, pred Predicate, max bool) (map[string]int64, error) {
	return runGroupStringText(ctx, s, table, groupColID, aggColID, pred, "aggregate", true, make(map[string]int64), stepGroupMinMaxInt(max))
}

// runGroupStringText is the shared driver for text-keyed aggregates other
// than count(*); intRequired forces aggColID to be int32 or int64.
func runGroupStringText[V any](
	ctx context.Context,
	s *Store,
	table catalog.TableDef,
	groupColID catalog.ColumnID,
	aggColID catalog.ColumnID,
	pred Predicate,
	role string,
	intRequired bool,
	values map[string]V,
	step groupTextStep[V],
) (map[string]V, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.beginOp(); err != nil {
		return nil, err
	}
	defer s.endOp()

	state, err := s.ensureTableState(table)
	if err != nil {
		return nil, err
	}
	if err := waitBufferIdle(state); err != nil {
		return nil, err
	}
	_, groupIndex, err := requireTextColumn(state, table, groupColID, "GROUP BY")
	if err != nil {
		return nil, err
	}
	var aggIndex int
	if intRequired {
		_, aggIndex, err = requireIntColumn(state, table, aggColID, role)
	} else {
		_, aggIndex, err = requireColumn(state, table, aggColID, role)
	}
	if err != nil {
		return nil, err
	}
	predIndexes, err := validatePredicateLocked(state, table, pred)
	if err != nil {
		return nil, err
	}

	if err := processBufferGroupText(state.buffer, groupIndex, aggIndex, predIndexes, pred, values, step); err != nil {
		state.bufferMu.Unlock()
		return nil, err
	}
	dir, segments := snapshotSegments(state)

	for _, segment := range segments {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		groupMeta, _, ok := columnMetaByID(segment, groupColID)
		if !ok {
			return nil, fmt.Errorf("segment %d missing grouped column ID %d", segment.ID, groupColID)
		}
		aggMeta, _, ok := columnMetaByID(segment, aggColID)
		if !ok {
			return nil, fmt.Errorf("segment %d missing %s column ID %d", segment.ID, role, aggColID)
		}
		predMetas, err := predicateColumnMetas(segment, pred)
		if err != nil {
			return nil, err
		}
		if err := processSegmentGroupText(s, segment.AbsPath(dir), segment.ID, groupMeta, aggMeta, true, predMetas, pred, values, step); err != nil {
			return nil, err
		}
	}
	return values, nil
}

// GroupAggregateAny is the polymorphic GROUP BY path supporting any of the
// 10 groupable key kinds (text, bytes, uuid, int16, int32, int64, bool, date,
// timestamp, enum) with any of the five aggregates; result keys/values are
// boxed into map[any]any.
func (s *Store) GroupAggregateAny(ctx context.Context, table catalog.TableDef, groupColID catalog.ColumnID, aggColID catalog.ColumnID, pred Predicate, aggregate GroupAggregate) (map[any]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.beginOp(); err != nil {
		return nil, err
	}
	defer s.endOp()

	state, err := s.ensureTableState(table)
	if err != nil {
		return nil, err
	}
	if err := waitBufferIdle(state); err != nil {
		return nil, err
	}
	groupCol, groupIndex, err := requireColumn(state, table, groupColID, "GROUP BY")
	if err != nil {
		return nil, err
	}
	if !isGroupableKind(groupCol.Type.Kind) {
		state.bufferMu.Unlock()
		return nil, fmt.Errorf("column %q is %s, want text, bytes, uuid, int16, int32, int64, bool, date, timestamp, or enum", groupCol.Name, groupCol.Type)
	}
	aggIndex := -1
	if aggregate != GroupAggregateCountStar {
		aggCol, idx, err := requireColumn(state, table, aggColID, "aggregate")
		if err != nil {
			return nil, err
		}
		if aggregate != GroupAggregateCountNonNull && aggCol.Type.Kind != sqltype.KindInt32 && aggCol.Type.Kind != sqltype.KindInt64 {
			state.bufferMu.Unlock()
			return nil, fmt.Errorf("aggregate column %q is %s, want int32 or int64", aggCol.Name, aggCol.Type)
		}
		aggIndex = idx
	}
	predIndexes, err := validatePredicateLocked(state, table, pred)
	if err != nil {
		return nil, err
	}

	groups := make(map[any]any)
	if err := forEachBufferBatch(state.buffer, func(batch vector.Batch, rows int) error {
		return accumulateGroupAnyBatch(batch, groupIndex, aggIndex, predIndexes, pred, aggregate, groups, rows)
	}); err != nil {
		state.bufferMu.Unlock()
		return nil, err
	}
	dir, segments := snapshotSegments(state)

	for _, segment := range segments {
		groupMeta, _, ok := columnMetaByID(segment, groupColID)
		if !ok {
			return nil, fmt.Errorf("segment %d missing grouped column ID %d", segment.ID, groupColID)
		}
		var aggMeta ColumnMeta
		if aggregate != GroupAggregateCountStar {
			aggMeta, _, ok = columnMetaByID(segment, aggColID)
			if !ok {
				return nil, fmt.Errorf("segment %d missing aggregate column ID %d", segment.ID, aggColID)
			}
		}
		predMetas, err := predicateColumnMetas(segment, pred)
		if err != nil {
			return nil, err
		}
		if err := s.accumulateGroupAnySegment(segment.AbsPath(dir), segment.ID, groupMeta, aggMeta, predMetas, pred, aggregate, groups); err != nil {
			return nil, err
		}
	}
	return groups, nil
}

// groupTextStep accumulates one row into values; aggCol/row are unused for COUNT(*).
type groupTextStep[V any] func(values map[string]V, key string, aggCol vector.Column, row int) error

// applyGroupTextRows runs step over each row [0, rows) where pred matches
// and the text group key is non-null.
func applyGroupTextRows[V any](groupCol vector.Column, aggCol vector.Column, predCols []vector.Column, pred Predicate, rows int, values map[string]V, step groupTextStep[V]) error {
	for row := 0; row < rows; row++ {
		if pred.Op != PredicateNone {
			matched, err := matchPredicateColumns(predCols, row, pred)
			if err != nil {
				return err
			}
			if !matched {
				continue
			}
		}
		if !vector.IsValid(groupCol.V.Valid, row) {
			continue
		}
		if err := step(values, groupCol.V.Var.String(row), aggCol, row); err != nil {
			return err
		}
	}
	return nil
}

// processBufferGroupText walks the ingest buffer, applying step to each row.
func processBufferGroupText[V any](buffer *IngestBuffer, groupIndex int, aggIndex int, predIndexes []int, pred Predicate, values map[string]V, step groupTextStep[V]) error {
	return forEachBufferBatch(buffer, func(batch vector.Batch, rows int) error {
		if groupIndex >= len(batch.Columns) {
			return fmt.Errorf("missing buffered grouped column index %d", groupIndex)
		}
		if aggIndex >= 0 && aggIndex >= len(batch.Columns) {
			return fmt.Errorf("missing buffered aggregate column index %d", aggIndex)
		}
		groupCol := batch.Columns[groupIndex]
		var aggCol vector.Column
		if aggIndex >= 0 {
			aggCol = batch.Columns[aggIndex]
		}
		predCols, err := predicateColumnsFromBatch(batch, predIndexes, pred)
		if err != nil {
			return err
		}
		return applyGroupTextRows(groupCol, aggCol, predCols, pred, rows, values, step)
	})
}

// processSegmentGroupText walks one sealed segment, applying step to each row.
func processSegmentGroupText[V any](s *Store, path string, segmentID SegmentID, groupMeta ColumnMeta, aggMeta ColumnMeta, needsAgg bool, predMetas []ColumnMeta, pred Predicate, values map[string]V, step groupTextStep[V]) error {
	visit := func(file *os.File, groupPage PageMeta, aggPage PageMeta, pageIndex int) error {
		groupCol, aggCol, predCols, err := readGroupPage(file, groupMeta, groupPage, aggMeta, aggPage, needsAgg, predMetas, pageIndex, segmentID)
		if err != nil {
			return err
		}
		return applyGroupTextRows(groupCol, aggCol, predCols, pred, groupCol.V.Len, values, step)
	}
	if needsAgg {
		return s.walkSegmentPagesPaired(path, segmentID, groupMeta, aggMeta, predMetas, visit)
	}
	return s.walkSegmentPages(path, segmentID, groupMeta, predMetas, nil, func(file *os.File, groupPage PageMeta, pageIndex int) error {
		return visit(file, groupPage, PageMeta{}, pageIndex)
	})
}

// readGroupPage decodes the group, optional aggregate, and predicate columns
// for one segment page, verifying their row counts align.
func readGroupPage(file *os.File, groupMeta ColumnMeta, groupPage PageMeta, aggMeta ColumnMeta, aggPage PageMeta, needsAgg bool, predMetas []ColumnMeta, pageIndex int, segmentID SegmentID) (vector.Column, vector.Column, []vector.Column, error) {
	groupCol, err := readColumnPageFromFile(file, groupMeta, groupPage)
	if err != nil {
		return vector.Column{}, vector.Column{}, nil, err
	}
	var aggCol vector.Column
	if needsAgg {
		aggCol, err = readColumnPageFromFile(file, aggMeta, aggPage)
		if err != nil {
			return vector.Column{}, vector.Column{}, nil, err
		}
		if groupCol.V.Len != aggCol.V.Len {
			return vector.Column{}, vector.Column{}, nil, fmt.Errorf("segment %d grouped page row mismatch", segmentID)
		}
	}
	predCols, err := readPredicateColumns(file, predMetas, pageIndex)
	if err != nil {
		return vector.Column{}, vector.Column{}, nil, err
	}
	for _, predCol := range predCols {
		if groupCol.V.Len != predCol.V.Len {
			return vector.Column{}, vector.Column{}, nil, fmt.Errorf("segment %d grouped page row mismatch", segmentID)
		}
	}
	return groupCol, aggCol, predCols, nil
}

// stepGroupCountStar increments the count for key (predicate already gated).
func stepGroupCountStar(values map[string]uint64, key string, _ vector.Column, _ int) error {
	values[key]++
	return nil
}

// stepGroupCountNonNull increments the count when aggCol[row] is non-null.
func stepGroupCountNonNull(values map[string]uint64, key string, aggCol vector.Column, row int) error {
	if vector.IsValid(aggCol.V.Valid, row) {
		values[key]++
	}
	return nil
}

// stepGroupSumInt accumulates sum, returning ErrSumOverflow on wrap.
func stepGroupSumInt(values map[string]int64, key string, aggCol vector.Column, row int) error {
	if !vector.IsValid(aggCol.V.Valid, row) {
		return nil
	}
	value, err := intValueAt(aggCol, row)
	if err != nil {
		return err
	}
	sum, err := addInt64(values[key], value)
	if err != nil {
		return err
	}
	values[key] = sum
	return nil
}

// stepGroupMinMaxInt builds a min/max accumulator (max=true picks max).
func stepGroupMinMaxInt(max bool) groupTextStep[int64] {
	return func(values map[string]int64, key string, aggCol vector.Column, row int) error {
		if !vector.IsValid(aggCol.V.Valid, row) {
			return nil
		}
		value, err := intValueAt(aggCol, row)
		if err != nil {
			return err
		}
		current, found := values[key]
		values[key] = mergeMinMax(current, found, value, max)
		return nil
	}
}

// addStringGroupCountsForSegment routes one segment through the right path:
// metadata-only when the predicate is text-eq on the group column, byte-level
// joint scan for text-key + single int64 predicate, otherwise the engine path.
func (s *Store) addStringGroupCountsForSegment(path string, segmentID SegmentID, groupMeta ColumnMeta, predMetas []ColumnMeta, groupColID catalog.ColumnID, pred Predicate, counts map[string]uint64, scratch *pageScratch) error {
	if pred.Op == PredicateOpEq && pred.ColumnID == groupColID && groupMeta.Type.Kind == sqltype.KindText {
		return s.addStringGroupCountsFromSegmentTextPredicate(path, groupMeta, pred, counts, scratch, segmentID)
	}
	if pred.Op == PredicateNone {
		return processSegmentGroupText(s, path, segmentID, groupMeta, ColumnMeta{}, false, nil, Predicate{}, counts, stepGroupCountStar)
	}
	return s.walkSegmentPages(path, segmentID, groupMeta, predMetas, nil, func(file *os.File, groupPage PageMeta, pageIndex int) error {
		if pred.Op != PredicateAnd && len(predMetas) == 1 {
			predMeta := predMetas[0]
			predPage := predMeta.Pages[pageIndex]
			if _, ok, err := addStringGroupCountsFromTextInt64Pages(file, groupMeta, groupPage, predMeta, predPage, pred, counts, scratch); err != nil {
				return err
			} else if ok {
				return nil
			}
		}
		groupCol, err := readColumnPageFromFile(file, groupMeta, groupPage)
		if err != nil {
			return err
		}
		predCols, err := readPredicateColumns(file, predMetas, pageIndex)
		if err != nil {
			return err
		}
		for _, predCol := range predCols {
			if groupCol.V.Len != predCol.V.Len {
				return fmt.Errorf("segment %d grouped page row mismatch", segmentID)
			}
		}
		for row := 0; row < groupCol.V.Len; row++ {
			matched, err := matchPredicateColumns(predCols, row, pred)
			if err != nil {
				return err
			}
			if !matched || !vector.IsValid(groupCol.V.Valid, row) {
				continue
			}
			counts[groupCol.V.Var.String(row)]++
		}
		return nil
	})
}

// addStringGroupCountsFromTextInt64Pages handles GROUP BY text-key with a
// single int64 predicate by decoding the two pages directly from bytes;
// returns (matched, true, nil) when handled, (0, false, nil) on fallback.
func addStringGroupCountsFromTextInt64Pages(file *os.File, groupCol ColumnMeta, groupPage PageMeta, predCol ColumnMeta, predPage PageMeta, pred Predicate, counts map[string]uint64, scratch *pageScratch) (int, bool, error) {
	if groupCol.Type.Kind != sqltype.KindText || predCol.Type.Kind != sqltype.KindInt64 || !isLeafComparison(pred.Op) {
		return 0, false, nil
	}
	if groupPage.Codec == CodecDictionary {
		return 0, false, nil
	}
	groupPayload := scratch.pagePayload(int(groupPage.Length))
	if _, err := file.ReadAt(groupPayload, int64(groupPage.Offset)); err != nil {
		return 0, true, err
	}
	rows := int(groupPage.Rows)
	groupPos := 0
	var groupValid []byte
	if !groupPage.AllValid {
		validLen := vector.ValidityWords(rows) * 8
		if len(groupPayload) < validLen {
			return 0, true, fmt.Errorf("column %q page payload is too short for validity", groupCol.Name)
		}
		groupValid = groupPayload[:validLen]
		groupPos = validLen
	}
	offsetLen := (rows + 1) * 4
	if len(groupPayload)-groupPos < offsetLen {
		return 0, true, fmt.Errorf("column %q page payload is too short for text offsets", groupCol.Name)
	}
	offsets := groupPayload[groupPos : groupPos+offsetLen]
	data := groupPayload[groupPos+offsetLen:]

	predPayload := scratch.predicatePagePayload(int(predPage.Length))
	if _, err := file.ReadAt(predPayload, int64(predPage.Offset)); err != nil {
		return 0, true, err
	}
	if rows != int(predPage.Rows) {
		return 0, true, fmt.Errorf("group predicate page row mismatch")
	}
	predPos := 0
	var predValid []byte
	if !predPage.AllValid {
		validLen := vector.ValidityWords(rows) * 8
		if len(predPayload) < validLen {
			return 0, true, fmt.Errorf("column %q page payload is too short for validity", predCol.Name)
		}
		predValid = predPayload[:validLen]
		predPos = validLen
	}
	valueLen := rows * 8
	if len(predPayload)-predPos < valueLen {
		return 0, true, fmt.Errorf("column %q page payload is too short for int64 values", predCol.Name)
	}
	values := predPayload[predPos : predPos+valueLen]

	matched := 0
	for row := 0; row < rows; row++ {
		if len(groupValid) != 0 && !validByte(groupValid, row) {
			continue
		}
		if len(predValid) != 0 && !validByte(predValid, row) {
			continue
		}
		value := int64(binary.LittleEndian.Uint64(values[row*8 : row*8+8]))
		if !matchInt64Value(value, pred) {
			continue
		}
		start := int(binary.LittleEndian.Uint32(offsets[row*4:]))
		end := int(binary.LittleEndian.Uint32(offsets[(row+1)*4:]))
		if start > end || end > len(data) {
			return 0, true, fmt.Errorf("text offsets are out of bounds")
		}
		key := ""
		if start != end {
			key = unsafe.String(&data[start], end-start)
		}
		if _, ok := counts[key]; ok {
			counts[key]++
		} else {
			counts[string(data[start:end])] = 1
		}
		matched++
	}
	return matched, true, nil
}

// isLeafComparison reports whether op is a single-column comparison, i.e.
// not None and not a compound (AND/OR/NOT) connective.
func isLeafComparison(op PredicateOp) bool {
	switch op {
	case PredicateOpEq, PredicateOpNotEq,
		PredicateOpLess, PredicateOpLessEqual,
		PredicateOpGreater, PredicateOpGreaterEqual,
		PredicateOpBetween, PredicateOpIn, PredicateOpNotIn:
		return true
	default:
		return false
	}
}

// addStringGroupCountsFromSegmentTextPredicate counts text-eq matches on the
// group column directly from segment/page metadata when available, falling
// back to a per-page metadata scan that bypasses row decoding.
func (s *Store) addStringGroupCountsFromSegmentTextPredicate(path string, colMeta ColumnMeta, pred Predicate, counts map[string]uint64, scratch *pageScratch, segmentID SegmentID) error {
	if !maySegmentMatch(colMeta, pred) {
		return nil
	}
	if n, ok := countPredicateSegmentMeta(colMeta, pred); ok {
		if n != 0 {
			counts[pred.Text] += uint64(n)
		}
		return nil
	}
	file, err := s.files.Acquire(path)
	if err != nil {
		return err
	}
	acquired := true
	defer func() {
		if acquired {
			_ = s.files.Release(path)
		}
	}()

	count := 0
	for _, page := range colMeta.Pages {
		if !mayPageMatch(colMeta, page, pred) {
			continue
		}
		if n, ok := countPredicatePageMeta(colMeta, page, pred); ok {
			count += int(n)
			continue
		}
		n, err := countPredicatePage(file, colMeta, page, pred, scratch)
		if err != nil {
			return err
		}
		count += n
	}
	if count > 0 {
		counts[pred.Text] += uint64(count)
	}
	_ = segmentID
	return nil
}

// applyGroupAnyRows runs the polymorphic accumulator over each row [0, rows)
// where pred matches.
func applyGroupAnyRows(groupCol vector.Column, aggCol vector.Column, predCols []vector.Column, pred Predicate, rows int, aggregate GroupAggregate, groups map[any]any) error {
	for row := 0; row < rows; row++ {
		if pred.Op != PredicateNone {
			matched, err := matchPredicateColumns(predCols, row, pred)
			if err != nil {
				return err
			}
			if !matched {
				continue
			}
		}
		if err := accumulateGroupAnyRow(groupCol, aggCol, row, aggregate, groups); err != nil {
			return err
		}
	}
	return nil
}

// accumulateGroupAnyBatch handles one buffered batch for the polymorphic path.
func accumulateGroupAnyBatch(batch vector.Batch, groupIndex int, aggIndex int, predIndexes []int, pred Predicate, aggregate GroupAggregate, groups map[any]any, rows int) error {
	if groupIndex >= len(batch.Columns) {
		return fmt.Errorf("missing buffered grouped column index %d", groupIndex)
	}
	groupCol := batch.Columns[groupIndex]
	var aggCol vector.Column
	if aggregate != GroupAggregateCountStar {
		if aggIndex >= len(batch.Columns) {
			return fmt.Errorf("missing buffered aggregate column index %d", aggIndex)
		}
		aggCol = batch.Columns[aggIndex]
	}
	predCols, err := predicateColumnsFromBatch(batch, predIndexes, pred)
	if err != nil {
		return err
	}
	return applyGroupAnyRows(groupCol, aggCol, predCols, pred, rows, aggregate, groups)
}

// accumulateGroupAnySegment walks one segment for the polymorphic path.
func (s *Store) accumulateGroupAnySegment(path string, segmentID SegmentID, groupMeta ColumnMeta, aggMeta ColumnMeta, predMetas []ColumnMeta, pred Predicate, aggregate GroupAggregate, groups map[any]any) error {
	needsAgg := aggregate != GroupAggregateCountStar
	visit := func(file *os.File, groupPage PageMeta, aggPage PageMeta, pageIndex int) error {
		groupCol, aggCol, predCols, err := readGroupPage(file, groupMeta, groupPage, aggMeta, aggPage, needsAgg, predMetas, pageIndex, segmentID)
		if err != nil {
			return err
		}
		return applyGroupAnyRows(groupCol, aggCol, predCols, pred, groupCol.V.Len, aggregate, groups)
	}
	if needsAgg {
		return s.walkSegmentPagesPaired(path, segmentID, groupMeta, aggMeta, predMetas, visit)
	}
	return s.walkSegmentPages(path, segmentID, groupMeta, predMetas, nil, func(file *os.File, groupPage PageMeta, pageIndex int) error {
		return visit(file, groupPage, PageMeta{}, pageIndex)
	})
}

// accumulateGroupAnyRow extracts the typed key, then dispatches by aggregate.
func accumulateGroupAnyRow(groupCol vector.Column, aggCol vector.Column, row int, aggregate GroupAggregate, groups map[any]any) error {
	if !vector.IsValid(groupCol.V.Valid, row) {
		return nil
	}
	key, err := groupKeyAt(groupCol, row)
	if err != nil {
		return err
	}
	switch aggregate {
	case GroupAggregateCountStar:
		current, _ := groups[key].(uint64)
		groups[key] = current + 1
	case GroupAggregateCountNonNull:
		if vector.IsValid(aggCol.V.Valid, row) {
			current, _ := groups[key].(uint64)
			groups[key] = current + 1
		}
	case GroupAggregateSum:
		if vector.IsValid(aggCol.V.Valid, row) {
			value, err := intValueAt(aggCol, row)
			if err != nil {
				return err
			}
			current, _ := groups[key].(int64)
			groups[key], err = addInt64(current, value)
			if err != nil {
				return err
			}
		}
	case GroupAggregateMin, GroupAggregateMax:
		if vector.IsValid(aggCol.V.Valid, row) {
			value, err := intValueAt(aggCol, row)
			if err != nil {
				return err
			}
			current, found := groups[key].(int64)
			groups[key] = mergeMinMax(current, found, value, aggregate == GroupAggregateMax)
		}
	default:
		return fmt.Errorf("unsupported grouped aggregate %d", aggregate)
	}
	return nil
}

// isGroupableKind reports whether kind can be used as a GROUP BY key in
// GroupAggregateAny; text-only entry points use requireTextColumn instead.
func isGroupableKind(kind sqltype.Kind) bool {
	switch kind {
	case sqltype.KindText, sqltype.KindBytes, sqltype.KindUUID, sqltype.KindInt16, sqltype.KindInt32, sqltype.KindInt64, sqltype.KindBool, sqltype.KindDate, sqltype.KindTimestamp, sqltype.KindNamed:
		return true
	default:
		return false
	}
}

// groupKeyAt extracts a typed key value from groupCol at row, normalizing
// dates/timestamps/enums to canonical strings for stable map keying.
func groupKeyAt(col vector.Column, row int) (any, error) {
	switch col.Type.Kind {
	case sqltype.KindText:
		return col.V.Var.String(row), nil
	case sqltype.KindBytes:
		return string(col.V.Var.Bytes(row)), nil
	case sqltype.KindUUID:
		return vector.FormatUUID(col.V.UUID[row]), nil
	case sqltype.KindInt16:
		return col.V.I16[row], nil
	case sqltype.KindInt32:
		return col.V.I32[row], nil
	case sqltype.KindDate:
		return formatDateDays(col.V.I32[row]), nil
	case sqltype.KindInt64:
		return col.V.I64[row], nil
	case sqltype.KindTimestamp:
		return formatTimestampNanos(col.V.I64[row]), nil
	case sqltype.KindBool:
		return boolAt(col.V.BoolBits, row), nil
	case sqltype.KindNamed:
		label, ok := enumLabelForCode(col.V.U32[row], col.EnumLabels)
		if !ok {
			return nil, fmt.Errorf("GROUP BY column %q has invalid enum code %d", col.Name, col.V.U32[row])
		}
		return label, nil
	default:
		return nil, fmt.Errorf("column %q is %s, want text, bytes, uuid, int16, int32, int64, bool, date, timestamp, or enum", col.Name, col.Type)
	}
}

