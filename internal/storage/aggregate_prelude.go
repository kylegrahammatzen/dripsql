package storage

import (
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
)

// waitBufferIdle locks state.bufferMu and waits for any in-flight buffered
// ingest publisher to drain. On success the caller still holds state.bufferMu
// and is responsible for releasing it (typically via snapshotSegments). On
// error state.bufferMu has been released.
func waitBufferIdle(state *tableState) error {
	state.bufferMu.Lock()
	for state.publishing {
		state.bufferCond.Wait()
	}
	if state.publishErr != nil {
		err := state.publishErr
		state.bufferMu.Unlock()
		return err
	}
	return nil
}

// snapshotSegments copies the segment list and table directory under state.mu
// and releases both state.mu and state.bufferMu. Caller must hold state.bufferMu
// before calling (typically obtained via waitBufferIdle).
func snapshotSegments(state *tableState) (string, []SegmentMeta) {
	state.mu.RLock()
	dir := state.dir
	segments := make([]SegmentMeta, len(state.segments))
	copy(segments, state.segments)
	state.mu.RUnlock()
	state.bufferMu.Unlock()
	return dir, segments
}

// validatePredicateLocked validates the predicate's column indexes and shape
// while holding state.bufferMu. On error state.bufferMu is released.
func validatePredicateLocked(state *tableState, table catalog.TableDef, pred Predicate) ([]int, error) {
	predIndexes, err := predicateColumnIndexes(table, pred)
	if err != nil {
		state.bufferMu.Unlock()
		return nil, err
	}
	if pred.Op != PredicateNone {
		if err := validatePredicate(table, pred); err != nil {
			state.bufferMu.Unlock()
			return nil, err
		}
	}
	return predIndexes, nil
}

// requireColumn looks up colID in table, releasing state.bufferMu on error.
// role is used to format the not-found error ("aggregate", "GROUP BY", etc.).
func requireColumn(state *tableState, table catalog.TableDef, colID catalog.ColumnID, role string) (catalog.ColumnDef, int, error) {
	col, idx, ok := tableColumnByID(table, colID)
	if !ok {
		state.bufferMu.Unlock()
		return catalog.ColumnDef{}, 0, fmt.Errorf("missing %s column ID %d", role, colID)
	}
	return col, idx, nil
}

// requireIntColumn looks up colID and verifies it is int32 or int64,
// releasing state.bufferMu on error. Uses role for both the not-found and
// kind-mismatch error messages.
func requireIntColumn(state *tableState, table catalog.TableDef, colID catalog.ColumnID, role string) (catalog.ColumnDef, int, error) {
	col, idx, err := requireColumn(state, table, colID, role)
	if err != nil {
		return col, idx, err
	}
	if col.Type.Kind != sqltype.KindInt32 && col.Type.Kind != sqltype.KindInt64 {
		state.bufferMu.Unlock()
		return catalog.ColumnDef{}, 0, fmt.Errorf("%s column %q is %s, want int32 or int64", role, col.Name, col.Type)
	}
	return col, idx, nil
}

// requireTextColumn looks up colID and verifies it is text, releasing
// state.bufferMu on error. The kind-mismatch message intentionally omits the
// role to match the existing error format.
func requireTextColumn(state *tableState, table catalog.TableDef, colID catalog.ColumnID, role string) (catalog.ColumnDef, int, error) {
	col, idx, err := requireColumn(state, table, colID, role)
	if err != nil {
		return col, idx, err
	}
	if col.Type.Kind != sqltype.KindText {
		state.bufferMu.Unlock()
		return catalog.ColumnDef{}, 0, fmt.Errorf("column %q is %s, want text", col.Name, col.Type)
	}
	return col, idx, nil
}
