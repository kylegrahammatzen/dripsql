package storage

import (
	"context"
	"testing"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestGroupAggregateAnyInt16FromBufferAndSegments(t *testing.T) {
	ctx := context.Background()
	store, table := newTestStoreWithColumns(t, []catalog.ColumnDef{
		{ID: 1, Name: "bucket", Type: sqltype.Int16, Nullable: true},
		{ID: 2, Name: "score", Type: sqltype.Int64, Nullable: true},
	})

	if _, err := store.AppendBatch(ctx, table, makeInt16ScoreBatch(t, []int16{1, 2, 1}, []int64{10, 7, 3}, nil)); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if err := store.AppendBuffered(ctx, table, makeInt16ScoreBatch(t, []int16{2, 1}, []int64{5, 9}, nil)); err != nil {
		t.Fatalf("AppendBuffered: %v", err)
	}

	sums, err := store.GroupAggregateAny(ctx, table, table.Columns[0].ID, table.Columns[1].ID, Predicate{}, GroupAggregateSum)
	if err != nil {
		t.Fatalf("GroupAggregateAny int16: %v", err)
	}
	want := map[any]any{int16(1): int64(22), int16(2): int64(12)}
	if len(sums) != len(want) {
		t.Fatalf("sums = %#v, want %#v", sums, want)
	}
	for key, wantValue := range want {
		if gotValue, ok := sums[key]; !ok || gotValue != wantValue {
			t.Fatalf("sums[%#v] = %#v ok=%v, want %#v (sums=%#v)", key, gotValue, ok, wantValue, sums)
		}
	}
}

func TestGroupAggregateAnyEnumLabelsFromBufferAndSegments(t *testing.T) {
	ctx := context.Background()
	store, table := newTestStoreEnum(t)

	if _, err := store.AppendBatch(ctx, table, makeEnumTextBatch(t, []uint32{1, 2, 1}, []string{"a", "b", "c"}, nil)); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if err := store.AppendBuffered(ctx, table, makeEnumTextBatch(t, []uint32{3, 2}, []string{"d", "e"}, nil)); err != nil {
		t.Fatalf("AppendBuffered: %v", err)
	}

	counts, err := store.GroupAggregateAny(ctx, table, table.Columns[0].ID, 0, Predicate{}, GroupAggregateCountStar)
	if err != nil {
		t.Fatalf("GroupAggregateAny enum: %v", err)
	}
	want := map[any]any{"new": uint64(2), "done": uint64(2), "archived": uint64(1)}
	if len(counts) != len(want) {
		t.Fatalf("counts = %#v, want %#v", counts, want)
	}
	for key, wantValue := range want {
		if gotValue, ok := counts[key]; !ok || gotValue != wantValue {
			t.Fatalf("counts[%#v] = %#v ok=%v, want %#v (counts=%#v)", key, gotValue, ok, wantValue, counts)
		}
	}
}

func TestGroupAggregateAnyTemporalKeysFromBufferAndSegments(t *testing.T) {
	ctx := context.Background()
	store, table := newTestStoreTimestampDate(t)
	tsA := time.Date(2026, 5, 7, 12, 30, 0, 123, time.UTC).UnixNano()
	tsB := time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC).UnixNano()
	tsC := time.Date(2026, 5, 9, 1, 2, 3, 4, time.UTC).UnixNano()
	dA := dateDays(time.Date(2026, 5, 7, 0, 0, 0, 0, time.UTC))
	dB := dateDays(time.Date(2026, 5, 8, 0, 0, 0, 0, time.UTC))

	if _, err := store.AppendBatch(ctx, table, makeTimestampDateTextBatch(t, []int64{tsA, tsB, tsA}, []int32{dA, dB, dA}, []string{"a", "b", "c"}, nil)); err != nil {
		t.Fatalf("AppendBatch: %v", err)
	}
	if err := store.AppendBuffered(ctx, table, makeTimestampDateTextBatch(t, []int64{tsB, tsC}, []int32{dB, dB}, []string{"d", "e"}, nil)); err != nil {
		t.Fatalf("AppendBuffered: %v", err)
	}

	dateCounts, err := store.GroupAggregateAny(ctx, table, table.Columns[1].ID, 0, Predicate{}, GroupAggregateCountStar)
	if err != nil {
		t.Fatalf("GroupAggregateAny date: %v", err)
	}
	wantDates := map[any]any{"2026-05-07": uint64(2), "2026-05-08": uint64(3)}
	if len(dateCounts) != len(wantDates) {
		t.Fatalf("date counts = %#v, want %#v", dateCounts, wantDates)
	}
	for key, wantValue := range wantDates {
		if gotValue, ok := dateCounts[key]; !ok || gotValue != wantValue {
			t.Fatalf("date counts[%#v] = %#v ok=%v, want %#v (counts=%#v)", key, gotValue, ok, wantValue, dateCounts)
		}
	}

	timestampCounts, err := store.GroupAggregateAny(ctx, table, table.Columns[0].ID, 0, Predicate{}, GroupAggregateCountStar)
	if err != nil {
		t.Fatalf("GroupAggregateAny timestamp: %v", err)
	}
	wantTimestamps := map[any]any{
		"2026-05-07T12:30:00.000000123Z": uint64(2),
		"2026-05-08T00:00:00.000000000Z": uint64(2),
		"2026-05-09T01:02:03.000000004Z": uint64(1),
	}
	if len(timestampCounts) != len(wantTimestamps) {
		t.Fatalf("timestamp counts = %#v, want %#v", timestampCounts, wantTimestamps)
	}
	for key, wantValue := range wantTimestamps {
		if gotValue, ok := timestampCounts[key]; !ok || gotValue != wantValue {
			t.Fatalf("timestamp counts[%#v] = %#v ok=%v, want %#v (counts=%#v)", key, gotValue, ok, wantValue, timestampCounts)
		}
	}
}

func TestGroupAggregateAnyUUIDBytesKeysFromBufferAndSegments(t *testing.T) {
	ctx := context.Background()
	uuidStore, uuidTable := newTestStoreUUID(t)
	ids := mustParseUUIDs(t,
		"550e8400-e29b-41d4-a716-446655440000",
		"550e8400-e29b-41d4-a716-446655440001",
		"550e8400-e29b-41d4-a716-446655440000",
	)
	if _, err := uuidStore.AppendBatch(ctx, uuidTable, makeUUIDTextBatch(t, ids, []string{"a", "b", "c"}, nil)); err != nil {
		t.Fatalf("AppendBatch uuid: %v", err)
	}
	bufferIDs := mustParseUUIDs(t,
		"550e8400-e29b-41d4-a716-446655440001",
		"550e8400-e29b-41d4-a716-446655440002",
	)
	if err := uuidStore.AppendBuffered(ctx, uuidTable, makeUUIDTextBatch(t, bufferIDs, []string{"d", "e"}, nil)); err != nil {
		t.Fatalf("AppendBuffered uuid: %v", err)
	}

	uuidCounts, err := uuidStore.GroupAggregateAny(ctx, uuidTable, uuidTable.Columns[0].ID, 0, Predicate{}, GroupAggregateCountStar)
	if err != nil {
		t.Fatalf("GroupAggregateAny uuid: %v", err)
	}
	wantUUIDs := map[any]any{
		"550e8400-e29b-41d4-a716-446655440000": uint64(2),
		"550e8400-e29b-41d4-a716-446655440001": uint64(2),
		"550e8400-e29b-41d4-a716-446655440002": uint64(1),
	}
	if len(uuidCounts) != len(wantUUIDs) {
		t.Fatalf("uuid counts = %#v, want %#v", uuidCounts, wantUUIDs)
	}
	for key, wantValue := range wantUUIDs {
		if gotValue, ok := uuidCounts[key]; !ok || gotValue != wantValue {
			t.Fatalf("uuid counts[%#v] = %#v ok=%v, want %#v (counts=%#v)", key, gotValue, ok, wantValue, uuidCounts)
		}
	}

	bytesStore, bytesTable := newTestStoreBytes(t)
	if _, err := bytesStore.AppendBatch(ctx, bytesTable, makeBytesTextBatch(t, []string{"aa", "bb", "aa"}, []string{"a", "b", "c"}, nil)); err != nil {
		t.Fatalf("AppendBatch bytes: %v", err)
	}
	if err := bytesStore.AppendBuffered(ctx, bytesTable, makeBytesTextBatch(t, []string{"bb", "cc"}, []string{"d", "e"}, nil)); err != nil {
		t.Fatalf("AppendBuffered bytes: %v", err)
	}
	bytesCounts, err := bytesStore.GroupAggregateAny(ctx, bytesTable, bytesTable.Columns[0].ID, 0, Predicate{}, GroupAggregateCountStar)
	if err != nil {
		t.Fatalf("GroupAggregateAny bytes: %v", err)
	}
	wantBytes := map[any]any{"aa": uint64(2), "bb": uint64(2), "cc": uint64(1)}
	if len(bytesCounts) != len(wantBytes) {
		t.Fatalf("bytes counts = %#v, want %#v", bytesCounts, wantBytes)
	}
	for key, wantValue := range wantBytes {
		if gotValue, ok := bytesCounts[key]; !ok || gotValue != wantValue {
			t.Fatalf("bytes counts[%#v] = %#v ok=%v, want %#v (counts=%#v)", key, gotValue, ok, wantValue, bytesCounts)
		}
	}
}

func makeInt16ScoreBatch(t testing.TB, buckets []int16, scores []int64, nullRows map[int]bool) vector.Batch {
	t.Helper()
	if len(buckets) != len(scores) {
		t.Fatalf("buckets/scores length mismatch")
	}
	rows := len(buckets)
	valid := buildValidity(rows, nullRows)
	return mustNewBatch(t, []vector.Column{
		{Name: "bucket", Type: sqltype.Int16, V: vector.Vec{Kind: vector.Int16, Len: rows, Valid: valid, I16: buckets}},
		{Name: "score", Type: sqltype.Int64, V: vector.Vec{Kind: vector.Int64, Len: rows, Valid: valid, I64: scores}},
	})
}
