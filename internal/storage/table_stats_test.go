package storage

import (
	"context"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
)

func TestTableStatsAggregatesAcrossSegments(t *testing.T) {
	store, table := newTestStore(t)
	first := makeIntTextBatch(t, []int64{1, 2, 3}, []string{"signup", "checkout", "view"}, nil)
	second := makeIntTextBatch(t, []int64{4, 5}, []string{"signup", "checkout"}, nil)
	if _, err := store.AppendBatch(context.Background(), table, first); err != nil {
		t.Fatalf("append first: %v", err)
	}
	if _, err := store.AppendBatch(context.Background(), table, second); err != nil {
		t.Fatalf("append second: %v", err)
	}

	stats, err := store.TableStats(context.Background(), table)
	if err != nil {
		t.Fatalf("TableStats: %v", err)
	}

	if stats.Table != "events" {
		t.Errorf("Table = %q, want events", stats.Table)
	}
	if stats.Segments != 2 {
		t.Errorf("Segments = %d, want 2", stats.Segments)
	}
	if stats.Rows != 5 {
		t.Errorf("Rows = %d, want 5", stats.Rows)
	}
	if stats.TableBytes <= 0 {
		t.Errorf("TableBytes = %d, want positive", stats.TableBytes)
	}
	if stats.ColumnPayloadBytes <= 0 {
		t.Errorf("ColumnPayloadBytes = %d, want positive", stats.ColumnPayloadBytes)
	}
	if stats.PlainEstimate <= 0 {
		t.Errorf("PlainEstimate = %d, want positive", stats.PlainEstimate)
	}
	if stats.StorageOverhead < 0 {
		t.Errorf("StorageOverhead = %d, want non-negative", stats.StorageOverhead)
	}

	if len(stats.Columns) != 2 {
		t.Fatalf("Columns = %d, want 2", len(stats.Columns))
	}

	tenant := stats.Columns[0]
	if tenant.Name != "tenant_id" {
		t.Errorf("col[0].Name = %q, want tenant_id", tenant.Name)
	}
	if tenant.Type.Kind != sqltype.KindInt64 {
		t.Errorf("col[0].Type = %s, want int64", tenant.Type)
	}
	if tenant.Rows != 5 {
		t.Errorf("col[0].Rows = %d, want 5", tenant.Rows)
	}
	if tenant.Pages == 0 {
		t.Errorf("col[0].Pages = 0, want > 0")
	}
	if tenant.StoredBytes <= 0 {
		t.Errorf("col[0].StoredBytes = %d, want positive", tenant.StoredBytes)
	}
	if want := int64(5 * 8); tenant.PlainEstimate != want {
		t.Errorf("col[0].PlainEstimate = %d, want %d", tenant.PlainEstimate, want)
	}
	if !tenant.HasMinMax {
		t.Errorf("col[0].HasMinMax = false, want true (int64 columns carry min/max)")
	}

	eventType := stats.Columns[1]
	if eventType.Name != "event_type" {
		t.Errorf("col[1].Name = %q, want event_type", eventType.Name)
	}
	if eventType.Type.Kind != sqltype.KindText {
		t.Errorf("col[1].Type = %s, want text", eventType.Type)
	}
	if eventType.StoredBytes <= 0 {
		t.Errorf("col[1].StoredBytes = %d, want positive", eventType.StoredBytes)
	}
}

func TestTableStatsEmptyTableReportsZeros(t *testing.T) {
	store, table := newTestStore(t)
	stats, err := store.TableStats(context.Background(), table)
	if err != nil {
		t.Fatalf("TableStats: %v", err)
	}
	if stats.Segments != 0 || stats.Rows != 0 || stats.TableBytes != 0 || stats.ColumnPayloadBytes != 0 {
		t.Errorf("empty table = %+v, want zero counts", stats)
	}
	if len(stats.Columns) != 2 {
		t.Errorf("Columns = %d, want 2 (one per defined column even when empty)", len(stats.Columns))
	}
	for _, col := range stats.Columns {
		if col.PlainEstimate != 0 {
			t.Errorf("col %q PlainEstimate = %d, want 0 for empty rows", col.Name, col.PlainEstimate)
		}
	}
}

func TestPlainEstimateMatchesTypeWidth(t *testing.T) {
	cases := []struct {
		typ  sqltype.Type
		rows uint64
		want int64
	}{
		{sqltype.Bool, 100, 100},
		{sqltype.Int16, 100, 200},
		{sqltype.Int32, 100, 400},
		{sqltype.Date, 100, 400},
		{sqltype.Int64, 100, 800},
		{sqltype.Timestamp, 100, 800},
		{sqltype.Float32, 100, 400},
		{sqltype.Float64, 100, 800},
		{sqltype.UUID, 100, 1600},
		{sqltype.Text, 100, 2000},
	}
	for _, tc := range cases {
		got := plainEstimate(tc.typ, tc.rows)
		if got != tc.want {
			t.Errorf("plainEstimate(%s, %d) = %d, want %d", tc.typ, tc.rows, got, tc.want)
		}
	}
}
