package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
)

func TestGroupStringCountsFromBufferAndSegments(t *testing.T) {
	ctx := context.Background()
	store, table := newTestStore(t)

	if _, err := store.AppendBatch(ctx, table, makeIntTextBatch(t, []int64{1, 2, 3}, []string{"red", "blue", "green"}, map[int]bool{2: true})); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	state, err := store.ensureTableState(table)
	if err != nil {
		t.Fatalf("ensureTableState: %v", err)
	}
	buffer, err := store.NewIngestBuffer(table, IngestBufferOptions{TargetRows: 1 << 30})
	if err != nil {
		t.Fatalf("NewIngestBuffer: %v", err)
	}
	state.bufferMu.Lock()
	state.buffer = buffer
	state.bufferMu.Unlock()

	if err := store.AppendBuffered(ctx, table, makeIntTextBatch(t, []int64{4, 5, 6}, []string{"blue", "blue", "green"}, map[int]bool{2: true})); err != nil {
		t.Fatalf("AppendBuffered: %v", err)
	}

	counts, err := store.GroupStringCounts(ctx, table, table.Columns[1].ID, Predicate{}, nil)
	if err != nil {
		t.Fatalf("GroupStringCounts: %v", err)
	}

	if got, want := counts, map[string]uint64{"red": 1, "blue": 3}; !equalStringUint64Maps(got, want) {
		t.Fatalf("counts = %#v, want %#v", got, want)
	}
}

func TestGroupStringCountsRejectsBadColumn(t *testing.T) {
	ctx := context.Background()
	store, table := newTestStore(t)
	if _, err := store.AppendBatch(ctx, table, makeIntTextBatch(t, []int64{1}, []string{"a"}, nil)); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	tests := []struct {
		name string
		col  catalog.ColumnID
		err  string
	}{
		{name: "missing", col: 999, err: "missing GROUP BY column ID"},
		{name: "nontypetext", col: table.Columns[0].ID, err: "want text"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := store.GroupStringCounts(ctx, table, tc.col, Predicate{}, nil)
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestGroupStringCountsWhereFromBufferAndSegments(t *testing.T) {
	ctx := context.Background()
	store, table := newTestStore(t)

	if _, err := store.AppendBatch(ctx, table, makeIntTextBatch(t, []int64{1, 2, 1, 3}, []string{"red", "blue", "red", "green"}, nil)); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	state, err := store.ensureTableState(table)
	if err != nil {
		t.Fatalf("ensureTableState: %v", err)
	}
	buffer, err := store.NewIngestBuffer(table, IngestBufferOptions{TargetRows: 1 << 30})
	if err != nil {
		t.Fatalf("NewIngestBuffer: %v", err)
	}
	state.bufferMu.Lock()
	state.buffer = buffer
	state.bufferMu.Unlock()

	if err := store.AppendBuffered(ctx, table, makeIntTextBatch(t, []int64{1, 2, 1}, []string{"blue", "blue", "green"}, nil)); err != nil {
		t.Fatalf("AppendBuffered: %v", err)
	}

	counts, err := store.GroupStringCounts(ctx, table, table.Columns[1].ID, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int64: 1}, nil)
	if err != nil {
		t.Fatalf("GroupStringCounts: %v", err)
	}

	if got, want := counts, map[string]uint64{"red": 2, "blue": 1, "green": 1}; !equalStringUint64Maps(got, want) {
		t.Fatalf("counts = %#v, want %#v", got, want)
	}
}

func TestGroupStringCountsWherePredicateTypes(t *testing.T) {
	ctx := context.Background()

	t.Run("int64 between", func(t *testing.T) {
		store, table := newTestStore(t)
		if _, err := store.AppendBatch(ctx, table, makeIntTextBatch(t, []int64{1, 2, 3, 4}, []string{"a", "b", "b", "c"}, nil)); err != nil {
			t.Fatalf("AppendBatch: %v", err)
		}
		counts, err := store.GroupStringCounts(ctx, table, table.Columns[1].ID, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpBetween, Lo: 2, Hi: 3}, nil)
		if err != nil {
			t.Fatalf("GroupStringCounts: %v", err)
		}
		if got, want := counts, map[string]uint64{"b": 2}; !equalStringUint64Maps(got, want) {
			t.Fatalf("counts = %#v, want %#v", got, want)
		}
	})

	t.Run("int32 between", func(t *testing.T) {
		store, table := newTestStoreInt32(t)
		if _, err := store.AppendBatch(ctx, table, makeInt32TextBatch(t, []int32{1, 2, 3, 4}, []string{"a", "b", "b", "c"}, nil)); err != nil {
			t.Fatalf("AppendBatch: %v", err)
		}
		counts, err := store.GroupStringCounts(ctx, table, table.Columns[1].ID, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpBetween, Lo32: 2, Hi32: 3}, nil)
		if err != nil {
			t.Fatalf("GroupStringCounts: %v", err)
		}
		if got, want := counts, map[string]uint64{"b": 2}; !equalStringUint64Maps(got, want) {
			t.Fatalf("counts = %#v, want %#v", got, want)
		}
	})

	t.Run("bool equal", func(t *testing.T) {
		store, table := newTestStoreBool(t)
		if _, err := store.AppendBatch(ctx, table, makeBoolTextBatch(t, []bool{true, false, true, false}, []string{"a", "b", "a", "c"}, nil)); err != nil {
			t.Fatalf("AppendBatch: %v", err)
		}
		counts, err := store.GroupStringCounts(ctx, table, table.Columns[1].ID, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Bool: true}, nil)
		if err != nil {
			t.Fatalf("GroupStringCounts: %v", err)
		}
		if got, want := counts, map[string]uint64{"a": 2}; !equalStringUint64Maps(got, want) {
			t.Fatalf("counts = %#v, want %#v", got, want)
		}
	})

	t.Run("text equal same column", func(t *testing.T) {
		store, table := newTestStore(t)
		if _, err := store.AppendBatch(ctx, table, makeIntTextBatch(t, []int64{1, 2, 3, 4}, []string{"a", "b", "a", "c"}, nil)); err != nil {
			t.Fatalf("AppendBatch: %v", err)
		}
		counts, err := store.GroupStringCounts(ctx, table, table.Columns[1].ID, Predicate{ColumnID: table.Columns[1].ID, Op: PredicateOpEq, Text: "a"}, nil)
		if err != nil {
			t.Fatalf("GroupStringCounts: %v", err)
		}
		if got, want := counts, map[string]uint64{"a": 2}; !equalStringUint64Maps(got, want) {
			t.Fatalf("counts = %#v, want %#v", got, want)
		}
	})
}

func TestGroupStringCountsWhereNoMatchAndErrors(t *testing.T) {
	ctx := context.Background()
	store, table := newTestStore(t)
	if _, err := store.AppendBatch(ctx, table, makeIntTextBatch(t, []int64{1}, []string{"a"}, nil)); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	counts, err := store.GroupStringCounts(ctx, table, table.Columns[1].ID, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int64: 9}, nil)
	if err != nil {
		t.Fatalf("GroupStringCounts no match: %v", err)
	}
	if len(counts) != 0 {
		t.Fatalf("counts = %#v, want empty", counts)
	}

	tests := []struct {
		name string
		col  catalog.ColumnID
		pred Predicate
		want string
	}{
		{name: "missing group", col: 999, pred: Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int64: 1}, want: "missing GROUP BY column ID"},
		{name: "non text group", col: table.Columns[0].ID, pred: Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpEq, Int64: 1}, want: "want text"},
		{name: "missing predicate", col: table.Columns[1].ID, pred: Predicate{ColumnID: 999, Op: PredicateOpEq, Int64: 1}, want: "missing predicate column ID"},
		{name: "unsupported predicate op", col: table.Columns[1].ID, pred: Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOp(99)}, want: "unsupported predicate op"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := store.GroupStringCounts(ctx, table, tt.col, tt.pred, nil)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestGroupStringCountNonNullFromBufferAndSegments(t *testing.T) {
	ctx := context.Background()
	store, table := newTestStore(t)

	if _, err := store.AppendBatch(ctx, table, makeIntTextBatch(t, []int64{1, 2, 3, 4}, []string{"red", "blue", "red", "green"}, map[int]bool{2: true})); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	state, err := store.ensureTableState(table)
	if err != nil {
		t.Fatalf("ensureTableState: %v", err)
	}
	buffer, err := store.NewIngestBuffer(table, IngestBufferOptions{TargetRows: 1 << 30})
	if err != nil {
		t.Fatalf("NewIngestBuffer: %v", err)
	}
	state.bufferMu.Lock()
	state.buffer = buffer
	state.bufferMu.Unlock()

	if err := store.AppendBuffered(ctx, table, makeIntTextBatch(t, []int64{5, 6, 7}, []string{"blue", "blue", "red"}, map[int]bool{2: true})); err != nil {
		t.Fatalf("AppendBuffered: %v", err)
	}

	counts, err := store.GroupStringCountNonNull(ctx, table, table.Columns[1].ID, table.Columns[0].ID, Predicate{})
	if err != nil {
		t.Fatalf("GroupStringCountNonNull: %v", err)
	}
	if got, want := counts, map[string]uint64{"red": 1, "blue": 3, "green": 1}; !equalStringUint64Maps(got, want) {
		t.Fatalf("counts = %#v, want %#v", got, want)
	}
}

func TestGroupStringCountNonNullWhere(t *testing.T) {
	ctx := context.Background()
	store, table := newTestStore(t)
	if _, err := store.AppendBatch(ctx, table, makeIntTextBatch(t, []int64{1, 2, 3, 4}, []string{"red", "blue", "red", "green"}, nil)); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	counts, err := store.GroupStringCountNonNull(ctx, table, table.Columns[1].ID, table.Columns[0].ID, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpGreater, Int64: 1})
	if err != nil {
		t.Fatalf("GroupStringCountNonNull: %v", err)
	}
	if got, want := counts, map[string]uint64{"blue": 1, "red": 1, "green": 1}; !equalStringUint64Maps(got, want) {
		t.Fatalf("counts = %#v, want %#v", got, want)
	}
}

func TestGroupStringSumsIntFromBufferAndSegments(t *testing.T) {
	ctx := context.Background()
	store, table := newTestStore(t)

	if _, err := store.AppendBatch(ctx, table, makeIntTextBatch(t, []int64{1, 2, 3, 4}, []string{"red", "blue", "red", "green"}, nil)); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	state, err := store.ensureTableState(table)
	if err != nil {
		t.Fatalf("ensureTableState: %v", err)
	}
	buffer, err := store.NewIngestBuffer(table, IngestBufferOptions{TargetRows: 1 << 30})
	if err != nil {
		t.Fatalf("NewIngestBuffer: %v", err)
	}
	state.bufferMu.Lock()
	state.buffer = buffer
	state.bufferMu.Unlock()

	if err := store.AppendBuffered(ctx, table, makeIntTextBatch(t, []int64{5, 6, 7}, []string{"blue", "blue", "red"}, map[int]bool{2: true})); err != nil {
		t.Fatalf("AppendBuffered: %v", err)
	}

	sums, err := store.GroupStringSumsInt(ctx, table, table.Columns[1].ID, table.Columns[0].ID, Predicate{})
	if err != nil {
		t.Fatalf("GroupStringSumsInt: %v", err)
	}

	if got, want := sums, map[string]int64{"red": 4, "blue": 13, "green": 4}; !equalStringInt64Maps(got, want) {
		t.Fatalf("sums = %#v, want %#v", got, want)
	}
}

func TestGroupStringSumsIntWhere(t *testing.T) {
	ctx := context.Background()
	store, table := newTestStore(t)

	if _, err := store.AppendBatch(ctx, table, makeIntTextBatch(t, []int64{1, 2, 3, 4}, []string{"red", "blue", "red", "green"}, nil)); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	sums, err := store.GroupStringSumsInt(ctx, table, table.Columns[1].ID, table.Columns[0].ID, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpBetween, Lo: 2, Hi: 3})
	if err != nil {
		t.Fatalf("GroupStringSumsInt: %v", err)
	}

	if got, want := sums, map[string]int64{"blue": 2, "red": 3}; !equalStringInt64Maps(got, want) {
		t.Fatalf("sums = %#v, want %#v", got, want)
	}
}

func TestMinMaxIntFromBufferAndSegments(t *testing.T) {
	ctx := context.Background()
	store, table := newTestStore(t)

	if _, err := store.AppendBatch(ctx, table, makeIntTextBatch(t, []int64{10, 2, 30}, []string{"a", "b", "c"}, nil)); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	state, err := store.ensureTableState(table)
	if err != nil {
		t.Fatalf("ensureTableState: %v", err)
	}
	buffer, err := store.NewIngestBuffer(table, IngestBufferOptions{TargetRows: 1 << 30})
	if err != nil {
		t.Fatalf("NewIngestBuffer: %v", err)
	}
	state.bufferMu.Lock()
	state.buffer = buffer
	state.bufferMu.Unlock()

	if err := store.AppendBuffered(ctx, table, makeIntTextBatch(t, []int64{1, 40, 5}, []string{"d", "e", "f"}, map[int]bool{2: true})); err != nil {
		t.Fatalf("AppendBuffered: %v", err)
	}

	min, ok, err := store.MinMaxInt(ctx, table, table.Columns[0].ID, Predicate{}, false, nil)
	if err != nil {
		t.Fatalf("MinMaxInt min: %v", err)
	}
	if !ok || min != 1 {
		t.Fatalf("min = %d ok=%v, want 1 true", min, ok)
	}

	max, ok, err := store.MinMaxInt(ctx, table, table.Columns[0].ID, Predicate{}, true, nil)
	if err != nil {
		t.Fatalf("MinMaxInt max: %v", err)
	}
	if !ok || max != 40 {
		t.Fatalf("max = %d ok=%v, want 40 true", max, ok)
	}
}

func TestMinMaxIntWhereNoMatch(t *testing.T) {
	ctx := context.Background()
	store, table := newTestStore(t)
	if _, err := store.AppendBatch(ctx, table, makeIntTextBatch(t, []int64{10, 2, 30}, []string{"a", "b", "c"}, nil)); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	min, ok, err := store.MinMaxInt(ctx, table, table.Columns[0].ID, Predicate{ColumnID: table.Columns[1].ID, Op: PredicateOpEq, Text: "missing"}, false, nil)
	if err != nil {
		t.Fatalf("MinMaxInt no match: %v", err)
	}
	if ok || min != 0 {
		t.Fatalf("min = %d ok=%v, want zero false", min, ok)
	}
}

func TestGroupStringMinMaxIntFromBufferAndSegments(t *testing.T) {
	ctx := context.Background()
	store, table := newTestStore(t)

	if _, err := store.AppendBatch(ctx, table, makeIntTextBatch(t, []int64{10, 2, 30, 8}, []string{"red", "blue", "red", "green"}, nil)); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	state, err := store.ensureTableState(table)
	if err != nil {
		t.Fatalf("ensureTableState: %v", err)
	}
	buffer, err := store.NewIngestBuffer(table, IngestBufferOptions{TargetRows: 1 << 30})
	if err != nil {
		t.Fatalf("NewIngestBuffer: %v", err)
	}
	state.bufferMu.Lock()
	state.buffer = buffer
	state.bufferMu.Unlock()

	if err := store.AppendBuffered(ctx, table, makeIntTextBatch(t, []int64{1, 40, 5}, []string{"blue", "blue", "red"}, map[int]bool{2: true})); err != nil {
		t.Fatalf("AppendBuffered: %v", err)
	}

	mins, err := store.GroupStringMinMaxInt(ctx, table, table.Columns[1].ID, table.Columns[0].ID, Predicate{}, false)
	if err != nil {
		t.Fatalf("GroupStringMinMaxInt min: %v", err)
	}
	if got, want := mins, map[string]int64{"red": 10, "blue": 1, "green": 8}; !equalStringInt64Maps(got, want) {
		t.Fatalf("mins = %#v, want %#v", got, want)
	}

	maxes, err := store.GroupStringMinMaxInt(ctx, table, table.Columns[1].ID, table.Columns[0].ID, Predicate{}, true)
	if err != nil {
		t.Fatalf("GroupStringMinMaxInt max: %v", err)
	}
	if got, want := maxes, map[string]int64{"red": 30, "blue": 40, "green": 8}; !equalStringInt64Maps(got, want) {
		t.Fatalf("maxes = %#v, want %#v", got, want)
	}
}

func TestGroupStringMinMaxIntWhere(t *testing.T) {
	ctx := context.Background()
	store, table := newTestStore(t)
	if _, err := store.AppendBatch(ctx, table, makeIntTextBatch(t, []int64{10, 2, 30, 8}, []string{"red", "blue", "red", "green"}, nil)); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}

	maxes, err := store.GroupStringMinMaxInt(ctx, table, table.Columns[1].ID, table.Columns[0].ID, Predicate{ColumnID: table.Columns[0].ID, Op: PredicateOpBetween, Lo: 3, Hi: 12}, true)
	if err != nil {
		t.Fatalf("GroupStringMinMaxInt max where: %v", err)
	}
	if got, want := maxes, map[string]int64{"red": 10, "green": 8}; !equalStringInt64Maps(got, want) {
		t.Fatalf("maxes = %#v, want %#v", got, want)
	}
}

func equalStringUint64Maps(left, right map[string]uint64) bool {
	if len(left) != len(right) {
		return false
	}
	for k, want := range right {
		if got, ok := left[k]; !ok || got != want {
			return false
		}
	}
	return true
}

func equalStringInt64Maps(left, right map[string]int64) bool {
	if len(left) != len(right) {
		return false
	}
	for k, want := range right {
		if got, ok := left[k]; !ok || got != want {
			return false
		}
	}
	return true
}
