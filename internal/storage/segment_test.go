package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestWriteReadSegmentFooterRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	batches := []types.Batch{segmentBatch(t, []int64{42, 7, 99}, []string{"checkout", "login", "signup"}, 1)}

	meta, err := WriteSegment(path, 77, batches)
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	got, _, err := ReadSegmentFooter(path)
	if err != nil {
		t.Fatalf("ReadSegmentFooter: %v", err)
	}
	if err := got.LoadAllColumns(); err != nil {
		t.Fatalf("LoadAllColumns: %v", err)
	}
	if !reflect.DeepEqual(got, meta) {
		t.Fatalf("meta = %#v, want %#v", got, meta)
	}
}

func TestReadColumnPageRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	batches := []types.Batch{segmentBatch(t, []int64{42, 7, 99}, []string{"checkout", "login", "signup"}, 1)}
	meta, err := WriteSegment(path, 11, batches)
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}

	col, err := ReadColumnPage(path, meta, 0, 0)
	if err != nil {
		t.Fatalf("ReadColumnPage: %v", err)
	}
	if col.Name != "tenant_id" || col.Type != types.Int64 || col.V.Encoding != types.EncodingFlat {
		t.Fatalf("column = %#v", col)
	}
	got := col.V.I64
	if len(got) != 3 || got[0] != 42 || got[2] != 99 {
		t.Fatalf("I64 valid rows = %v", got)
	}
	if types.IsValid(col.V.Valid, 1) {
		t.Fatal("row 1 should be invalid")
	}
}

func TestWriteSegmentCompressesHighCardinalityText(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	batch := compressibleTextBatch(t, types.StandardBatchRows)
	meta, err := WriteSegment(path, 12, []types.Batch{batch})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	page := meta.Columns[0].Pages[0]
	if page.Encoding != types.EncodingFlate && page.Encoding != types.EncodingZstd {
		t.Fatalf("encoding = %s, want flate or zstd", page.Encoding)
	}
	plainBytes := int64(page.Text.DataBytes) + int64(page.Rows)*4 + 4
	if int64(page.Length) >= plainBytes {
		t.Fatalf("stored length = %d, plain = %d", page.Length, plainBytes)
	}

	col, err := ReadColumnPage(path, meta, 0, 0)
	if err != nil {
		t.Fatalf("ReadColumnPage: %v", err)
	}
	if got, want := col.V.Var.String(77), batch.Columns[0].V.Var.String(77); got != want {
		t.Fatalf("row 77 = %q, want %q", got, want)
	}
}

func TestReadSegmentBatchRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	batches := []types.Batch{segmentBatch(t, []int64{42, 7, 99}, []string{"checkout", "login", "signup"}, 1)}
	meta, err := WriteSegment(path, 16, batches)
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}

	batch, payloadBytes, err := ReadSegmentBatch(path, meta, 0)
	if err != nil {
		t.Fatalf("ReadSegmentBatch: %v", err)
	}
	if batch.Len != 3 || len(batch.Columns) != 2 {
		t.Fatalf("batch = %#v", batch)
	}
	wantPayloadBytes := int(meta.Columns[0].Pages[0].Length + meta.Columns[1].Pages[0].Length)
	if payloadBytes != wantPayloadBytes {
		t.Fatalf("payloadBytes = %d, want %d", payloadBytes, wantPayloadBytes)
	}
	got := batch.Columns[0].V.I64
	if len(got) != 3 || got[0] != 42 || got[2] != 99 {
		t.Fatalf("I64 valid rows = %v", got)
	}
	if types.IsValid(batch.Columns[0].V.Valid, 1) {
		t.Fatal("row 1 should be invalid")
	}
	if got := textValue(t, batch.Columns[1].V, 2); got != "signup" {
		t.Fatalf("event row 2 = %q", got)
	}
}

func TestReadSegmentBatchColumns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	meta, err := WriteSegment(path, 11, []types.Batch{segmentBatch(t, []int64{42, 7, 99}, []string{"checkout", "login", "signup"})})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	batch, payloadBytes, err := ReadSegmentBatchColumns(path, meta, 0, []string{"event_type"})
	if err != nil {
		t.Fatalf("ReadSegmentBatchColumns: %v", err)
	}
	if len(batch.Columns) != 1 || batch.Columns[0].Name != "event_type" || batch.Len != 3 {
		t.Fatalf("batch = %#v", batch)
	}
	if got := textValue(t, batch.Columns[0].V, 2); got != "signup" {
		t.Fatalf("event_type[2] = %q", got)
	}
	if want := int(meta.Columns[1].Pages[0].Length); payloadBytes != want {
		t.Fatalf("payloadBytes = %d, want %d", payloadBytes, want)
	}
}

func TestReadSegmentBatchColumnsDeduplicatesProjection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	meta, err := WriteSegment(path, 110, []types.Batch{segmentBatch(t, []int64{42, 7, 99}, []string{"checkout", "login", "signup"})})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}

	batch, payloadBytes, err := ReadSegmentBatchColumns(path, meta, 0, []string{"event_type", "event_type"})
	if err != nil {
		t.Fatalf("ReadSegmentBatchColumns: %v", err)
	}
	if len(batch.Columns) != 1 || batch.Columns[0].Name != "event_type" {
		t.Fatalf("columns = %#v", batch.Columns)
	}
	if want := int(meta.Columns[1].Pages[0].Length); payloadBytes != want {
		t.Fatalf("payloadBytes = %d, want %d", payloadBytes, want)
	}
}

func TestReadSegmentBatchColumnsRejectsMissingColumn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	meta, err := WriteSegment(path, 111, []types.Batch{segmentBatch(t, []int64{42}, []string{"checkout"})})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	if _, _, err := ReadSegmentBatchColumns(path, meta, 0, []string{"missing"}); err == nil {
		t.Fatal("expected missing projection column error")
	}
}

func TestSegmentReadPlanBuildsProjectedPageInfos(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	batches := []types.Batch{
		segmentBatch(t, []int64{1, 2}, []string{"signup", "checkout"}),
		segmentBatch(t, []int64{3, 4, 5}, []string{"login", "signup", "checkout"}),
	}
	meta, err := WriteSegment(path, 112, batches)
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}

	plan, err := NewSegmentReadPlan(path, &meta, []string{"event_type"}, nil)
	if err != nil {
		t.Fatalf("NewSegmentReadPlan: %v", err)
	}
	if plan.Size <= 0 || plan.AllColumns || len(plan.Indexes) != 1 || plan.Indexes[0] != 1 {
		t.Fatalf("plan = %#v", plan)
	}
	if len(plan.PageInfos) != 2 {
		t.Fatalf("PageInfos len = %d, want 2", len(plan.PageInfos))
	}
	for pageIndex, info := range plan.PageInfos {
		want := int(meta.Columns[1].Pages[pageIndex].Length)
		if info.PayloadBytes != want {
			t.Fatalf("PageInfos[%d].PayloadBytes = %d, want %d", pageIndex, info.PayloadBytes, want)
		}
	}
	if plan.PageInfos[1].RowStart != 2 || plan.PageInfos[1].Rows != 3 {
		t.Fatalf("PageInfos[1] = %#v", plan.PageInfos[1])
	}

	batch, payloadBytes, err := plan.ReadBatch(1)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if len(batch.Columns) != 1 || batch.Columns[0].Name != "event_type" || batch.Len != 3 {
		t.Fatalf("batch = %#v", batch)
	}
	if want := int(meta.Columns[1].Pages[1].Length); payloadBytes != want {
		t.Fatalf("payloadBytes = %d, want %d", payloadBytes, want)
	}
}

func TestSegmentReadPlanReadBatchKeepsPreviousBatchStable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	batches := []types.Batch{
		segmentBatch(t, []int64{1, 2}, []string{"signup", "checkout"}),
		segmentBatch(t, []int64{3, 4}, []string{"login", "purchase"}),
	}
	meta, err := WriteSegment(path, 113, batches)
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	plan, err := NewSegmentReadPlan(path, &meta, []string{"tenant_id", "event_type"}, nil)
	if err != nil {
		t.Fatalf("NewSegmentReadPlan: %v", err)
	}
	first, _, err := plan.ReadBatch(0)
	if err != nil {
		t.Fatalf("ReadBatch first: %v", err)
	}
	if _, _, err := plan.ReadBatch(1); err != nil {
		t.Fatalf("ReadBatch second: %v", err)
	}
	if first.Columns[0].V.I64[0] != 1 || first.Columns[1].V.Var.String(0) != "signup" {
		t.Fatalf("first batch mutated after second read: %#v", first)
	}
}

func TestReadSegmentBatchMultiBatchSecondPage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	batches := []types.Batch{
		segmentBatch(t, []int64{1, 2}, []string{"signup", "checkout"}),
		segmentBatch(t, []int64{3, 4, 5}, []string{"login", "signup", "checkout"}),
	}
	meta, err := WriteSegment(path, 17, batches)
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}

	batch, _, err := ReadSegmentBatch(path, meta, 1)
	if err != nil {
		t.Fatalf("ReadSegmentBatch: %v", err)
	}
	if batch.Len != 3 {
		t.Fatalf("batch Len = %d, want 3", batch.Len)
	}
	if got := batch.Columns[0].V.I64; !reflect.DeepEqual(got, []int64{3, 4, 5}) {
		t.Fatalf("page I64 = %v", got)
	}
	if got := textValue(t, batch.Columns[1].V, 1); got != "signup" {
		t.Fatalf("event row 1 = %q", got)
	}
}

func TestReadSegmentBatchRejectsPageMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	meta, err := WriteSegment(path, 18, []types.Batch{segmentBatch(t, []int64{1, 2}, []string{"checkout", "login"})})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	meta.Columns[1].Pages[0].RowStart = 1

	if _, _, err := ReadSegmentBatch(path, meta, 0); err == nil {
		t.Fatal("expected page row range mismatch")
	}
}

func TestReadSegmentBatchRejectsPageOutOfRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	meta, err := WriteSegment(path, 19, []types.Batch{segmentBatch(t, []int64{1}, []string{"checkout"})})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	if _, _, err := ReadSegmentBatch(path, meta, 1); err == nil {
		t.Fatal("expected page out-of-range error")
	}
}

func TestWriteSegmentMultiBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	batches := []types.Batch{
		segmentBatch(t, []int64{1, 2}, []string{"signup", "checkout"}),
		segmentBatch(t, []int64{3, 4, 5}, []string{"login", "signup", "checkout"}),
	}
	meta, err := WriteSegment(path, 12, batches)
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	if meta.Rows != 5 || len(meta.Columns) != 2 || len(meta.Columns[0].Pages) != 2 {
		t.Fatalf("meta = %#v", meta)
	}
	if meta.Columns[0].Pages[0].RowStart != 0 || meta.Columns[0].Pages[1].RowStart != 2 {
		t.Fatalf("pages = %#v", meta.Columns[0].Pages)
	}

	col, err := ReadColumnPage(path, meta, 0, 1)
	if err != nil {
		t.Fatalf("ReadColumnPage: %v", err)
	}
	if got := col.V.I64; !reflect.DeepEqual(got, []int64{3, 4, 5}) {
		t.Fatalf("page I64 = %v", got)
	}
}

func TestWriteSegmentUsesPageMajorLayout(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	meta, err := WriteSegment(path, 120, []types.Batch{
		segmentBatch(t, []int64{1, 2}, []string{"signup", "checkout"}),
		segmentBatch(t, []int64{3, 4}, []string{"login", "signup"}),
	})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	tenantPage0 := meta.Columns[0].Pages[0].Offset
	eventPage0 := meta.Columns[1].Pages[0].Offset
	tenantPage1 := meta.Columns[0].Pages[1].Offset
	eventPage1 := meta.Columns[1].Pages[1].Offset
	if !(tenantPage0 < eventPage0 && eventPage0 < tenantPage1 && tenantPage1 < eventPage1) {
		t.Fatalf("offsets are not page-major: tenant0=%d event0=%d tenant1=%d event1=%d", tenantPage0, eventPage0, tenantPage1, eventPage1)
	}
}

func TestWriteSegmentStoresSummaryStats(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	meta, err := WriteSegment(path, 20, []types.Batch{segmentBatch(t, []int64{10, 2, 30, 8}, []string{"red", "blue", "red", "green"}, 3)})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	got, _, err := ReadSegmentFooter(path)
	if err != nil {
		t.Fatalf("ReadSegmentFooter: %v", err)
	}
	if err := got.LoadAllColumns(); err != nil {
		t.Fatalf("LoadAllColumns: %v", err)
	}
	if got.Columns[0].Stats.Int64 == nil || got.Columns[0].Stats.Int64.Min != 2 || got.Columns[0].Stats.Int64.Max != 30 || got.Columns[0].Stats.Int64.Sum != 42 || !got.Columns[0].Stats.Int64.SumValid {
		t.Fatalf("int64 column stats = %#v", got.Columns[0].Stats.Int64)
	}
	valueStats := got.Columns[0].Pages[0].Int64Values
	if valueStats == nil || valueStats.Truncated || !int64SetContains(valueStats.Values, 10) || !int64SetContains(valueStats.Values, 2) || !int64SetContains(valueStats.Values, 30) || int64SetContains(valueStats.Values, 8) {
		t.Fatalf("int64 value stats = %#v", valueStats)
	}
	if got.Columns[0].AllValid || got.Columns[0].AllNull || got.Columns[0].Pages[0].NullCount != 1 {
		t.Fatalf("int64 validity stats = %#v", got.Columns[0])
	}
	if got.Columns[1].Stats.Text == nil || !stringSetContains(got.Columns[1].Stats.Text.Values, "red") || !stringSetContains(got.Columns[1].Stats.Text.Values, "blue") || !stringSetContains(got.Columns[1].Stats.Text.Values, "green") {
		t.Fatalf("text column stats = %#v", got.Columns[1].Stats.Text)
	}
	if !reflect.DeepEqual(got, meta) {
		t.Fatalf("footer meta = %#v, want %#v", got, meta)
	}
}

func TestWriteSegmentDropsTruncatedValueStatsPayload(t *testing.T) {
	ids := make([]int64, ValueStatsMaxValues+1)
	events := make([]string, len(ids))
	for i := range ids {
		ids[i] = int64(i)
		events[i] = "checkout"
	}
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	meta, err := WriteSegment(path, 21, []types.Batch{segmentBatch(t, ids, events)})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	stats := meta.Columns[0].Pages[0].Int64Values
	if stats == nil || !stats.Truncated || len(stats.Values) != 0 {
		t.Fatalf("value stats = %#v, want truncated with no values", stats)
	}
}

func TestWriteSegmentPicksConstantTextPage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	batches := []types.Batch{segmentBatch(t, []int64{1, 2, 3, 4}, []string{"checkout", "checkout", "checkout", "checkout"})}
	meta, err := WriteSegment(path, 13, batches)
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	textPage := meta.Columns[1].Pages[0]
	if textPage.Encoding != types.EncodingConstant {
		t.Fatalf("text encoding = %s, want constant", textPage.Encoding)
	}
	if textPage.Text == nil || !stringSetContains(textPage.Text.Values, "checkout") || textPage.Text.Truncated {
		t.Fatalf("text stats = %#v", textPage.Text)
	}

	col, err := ReadColumnPage(path, meta, 1, 0)
	if err != nil {
		t.Fatalf("ReadColumnPage: %v", err)
	}
	if col.V.Encoding != types.EncodingFlat || col.V.Var.String(0) != "checkout" || col.V.Var.String(3) != "checkout" {
		t.Fatalf("text vec = %#v", col.V)
	}
}

func TestWriteSegmentStoresBoolAndInt16Stats(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	meta, err := WriteSegment(path, 121, []types.Batch{boolInt16Batch(t, []bool{true, false, true}, []int16{-2, 7, 3})})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	got, _, err := ReadSegmentFooter(path)
	if err != nil {
		t.Fatalf("ReadSegmentFooter: %v", err)
	}
	if err := got.LoadAllColumns(); err != nil {
		t.Fatalf("LoadAllColumns: %v", err)
	}
	if got.Columns[0].Stats.Bool == nil || !got.Columns[0].Stats.Bool.HasTrue || !got.Columns[0].Stats.Bool.HasFalse {
		t.Fatalf("bool column stats = %#v", got.Columns[0].Stats.Bool)
	}
	if got.Columns[0].Pages[0].Bool == nil || !got.Columns[0].Pages[0].Bool.HasTrue || !got.Columns[0].Pages[0].Bool.HasFalse {
		t.Fatalf("bool page stats = %#v", got.Columns[0].Pages[0].Bool)
	}
	if got.Columns[1].Stats.Int32 == nil || got.Columns[1].Stats.Int32.Min != -2 || got.Columns[1].Stats.Int32.Max != 7 || got.Columns[1].Stats.Int32.Sum != 8 || !got.Columns[1].Stats.Int32.SumValid {
		t.Fatalf("int16 column stats = %#v", got.Columns[1].Stats.Int32)
	}
	if got.Columns[1].Pages[0].Int32 == nil || got.Columns[1].Pages[0].Int32.Min != -2 || got.Columns[1].Pages[0].Int32.Max != 7 {
		t.Fatalf("int16 page stats = %#v", got.Columns[1].Pages[0].Int32)
	}
	if got.Columns[1].Pages[0].Int32Values == nil || got.Columns[1].Pages[0].Int32Values.Truncated || !int32SetContains(got.Columns[1].Pages[0].Int32Values.Values, -2) || !int32SetContains(got.Columns[1].Pages[0].Int32Values.Values, 7) || !int32SetContains(got.Columns[1].Pages[0].Int32Values.Values, 3) {
		t.Fatalf("int16 value stats = %#v", got.Columns[1].Pages[0].Int32Values)
	}
	if !reflect.DeepEqual(got, meta) {
		t.Fatalf("footer meta = %#v, want %#v", got, meta)
	}
}

func TestReadSegmentFooterRejectsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	meta, err := WriteSegment(path, 14, []types.Batch{segmentBatch(t, []int64{1}, []string{"checkout"})})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if meta.Rows != 1 {
		t.Fatalf("unexpected meta = %#v", meta)
	}

	tests := []struct {
		name string
		data []byte
	}{
		{name: "bad header", data: corruptCopy(data, 0, 'X')},
		{name: "bad footer magic", data: corruptCopy(data, len(data)-1, 'X')},
		{name: "bad footer length", data: corruptFooterLength(data, uint64(len(data))*2)},
		{name: "truncated segment", data: append([]byte(nil), data[:len(data)-3]...)},
		{name: "truncated metadata", data: malformedFooterFile([]byte{1, 2, 3, 4})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			corruptPath := filepath.Join(t.TempDir(), "segment.dsv3")
			if err := os.WriteFile(corruptPath, tt.data, 0o644); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			if _, _, err := ReadSegmentFooter(corruptPath); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestReadColumnPageRejectsTruncatedPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	meta, err := WriteSegment(path, 15, []types.Batch{segmentBatch(t, []int64{1, 2}, []string{"checkout", "login"})})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	page := meta.Columns[0].Pages[0]
	if err := os.Truncate(path, int64(page.Offset+page.Length-1)); err != nil {
		t.Fatalf("Truncate: %v", err)
	}
	if _, err := ReadColumnPage(path, meta, 0, 0); err == nil {
		t.Fatal("expected truncated page error")
	}
}

func TestReadSegmentConcurrentReaders(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	meta, err := WriteSegment(path, 25, []types.Batch{segmentBatch(t, []int64{1, 2}, []string{"checkout", "login"})})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	const readers = 8
	var wg sync.WaitGroup
	errs := make(chan error, readers*2)
	for range readers {
		wg.Go(func() {
			if _, _, err := ReadSegmentFooter(path); err != nil {
				errs <- err
				return
			}
			if _, _, err := ReadSegmentBatch(path, meta, 0); err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent read: %v", err)
		}
	}
}

func segmentBatch(t *testing.T, ids []int64, events []string, invalidRows ...int) types.Batch {
	t.Helper()
	if len(ids) != len(events) {
		t.Fatalf("ids/events length mismatch: %d/%d", len(ids), len(events))
	}
	var valid types.Validity
	if len(invalidRows) != 0 {
		valid = types.NewValidity(len(ids))
		for _, row := range invalidRows {
			types.SetInvalid(valid, row)
		}
	}
	varText := types.NewVarBytes(len(events), 0)
	for i, value := range events {
		varText.AppendString(i, value)
	}
	batch, err := types.NewBatch([]types.Column{
		{Name: "tenant_id", Type: types.Int64, V: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: len(ids), Valid: valid, I64: append([]int64(nil), ids...)}},
		{Name: "event_type", Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: len(events), Var: varText}},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func compressibleTextBatch(t *testing.T, rows int) types.Batch {
	t.Helper()
	varText := types.NewVarBytes(rows, rows*128)
	for row := range rows {
		value := fmt.Sprintf("%s/%08d/%s", strings.Repeat("/users/events", 8), row, strings.Repeat("x", 32))
		varText.AppendString(row, value)
	}
	batch, err := types.NewBatch([]types.Column{{Name: "url", Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: rows, Var: varText}}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func boolBatch(t *testing.T, values []bool) types.Batch {
	t.Helper()
	bits := make([]uint64, types.ValidityWords(len(values)))
	for row, value := range values {
		if value {
			bits[row>>6] |= uint64(1) << uint(row&63)
		}
	}
	batch, err := types.NewBatch([]types.Column{{Name: "flag", Type: types.Bool, V: types.Vec{Kind: types.VecBool, Encoding: types.EncodingFlat, Len: len(values), BoolBits: bits}}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func int16Batch(t *testing.T, values []int16) types.Batch {
	t.Helper()
	batch, err := types.NewBatch([]types.Column{{Name: "small_id", Type: types.Int16, V: types.Vec{Kind: types.VecInt16, Encoding: types.EncodingFlat, Len: len(values), I16: append([]int16(nil), values...)}}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func boolInt16Batch(t *testing.T, flags []bool, values []int16) types.Batch {
	t.Helper()
	if len(flags) != len(values) {
		t.Fatalf("flags/values length mismatch: %d/%d", len(flags), len(values))
	}
	bits := make([]uint64, types.ValidityWords(len(flags)))
	for row, value := range flags {
		if value {
			bits[row>>6] |= uint64(1) << uint(row&63)
		}
	}
	batch, err := types.NewBatch([]types.Column{
		{Name: "flag", Type: types.Bool, V: types.Vec{Kind: types.VecBool, Encoding: types.EncodingFlat, Len: len(flags), BoolBits: bits}},
		{Name: "small_id", Type: types.Int16, V: types.Vec{Kind: types.VecInt16, Encoding: types.EncodingFlat, Len: len(values), I16: append([]int16(nil), values...)}},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func uniqueEvents(n int) []string {
	events := make([]string, n)
	for i := range events {
		events[i] = "event_" + string([]byte{'a' + byte(i/26), 'a' + byte(i%26)})
	}
	return events
}

func corruptCopy(data []byte, offset int, value byte) []byte {
	copyData := append([]byte(nil), data...)
	copyData[offset] = value
	return copyData
}

func corruptFooterLength(data []byte, footerLen uint64) []byte {
	copyData := append([]byte(nil), data...)
	tailStart := len(copyData) - len(segmentMagic) - 8
	binary.LittleEndian.PutUint64(copyData[tailStart:tailStart+8], footerLen)
	return copyData
}

func malformedFooterFile(footer []byte) []byte {
	data := make([]byte, 0, len(segmentMagic)+len(footer)+8+len(segmentMagic))
	data = append(data, segmentMagic...)
	data = append(data, footer...)
	var footerLen [8]byte
	binary.LittleEndian.PutUint64(footerLen[:], uint64(len(footer)))
	data = append(data, footerLen[:]...)
	data = append(data, segmentMagic...)
	return data
}

func textValue(t *testing.T, v types.Vec, row int) string {
	t.Helper()
	switch v.Encoding {
	case types.EncodingFlat:
		return v.Var.String(row)
	case types.EncodingDictionary:
		return v.Encoded.DictValues.String(int(v.Encoded.DictIDs[row]))
	default:
		t.Fatalf("unsupported text encoding %s", v.Encoding)
		return ""
	}
}

func stringSetContains(values []string, want string) bool {
	return slices.Contains(values, want)
}

func int32SetContains(values []int32, want int32) bool {
	return slices.Contains(values, want)
}

func int64SetContains(values []int64, want int64) bool {
	return slices.Contains(values, want)
}
