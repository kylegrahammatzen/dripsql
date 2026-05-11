package storage

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestSegmentScanIteratorWalksPagesAndStats(t *testing.T) {
	batch := predicateBatch(t)
	stats := &ExecStats{}
	it := SegmentScanIterator{
		Segments: []ScanSegment{{Pages: []ScanPage{{Batch: batch, PayloadBytes: 64}, {Batch: batch, PayloadBytes: 32}}}},
		Stats:    stats,
	}
	visits := 0
	if err := it.ForEach(func(_ types.Batch, sel types.SelectionMask) error {
		visits++
		if sel.PopCount() != batch.Len {
			t.Fatalf("popcount = %d, want %d", sel.PopCount(), batch.Len)
		}
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visits != 2 {
		t.Fatalf("visits = %d, want 2", visits)
	}
	if stats.SegmentsTotal != 1 || stats.PagesTotal != 2 || stats.RowsMatched != int64(batch.Len*2) || stats.PayloadBytesRead != 96 {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestSegmentScanIteratorReusesSelectionMaskAcrossPages(t *testing.T) {
	batch := predicateBatch(t)
	it := SegmentScanIterator{
		Segments: []ScanSegment{{Pages: []ScanPage{{Batch: batch}, {Batch: batch}}}},
	}
	visits := 0
	var firstWord *uint64
	if err := it.ForEach(func(_ types.Batch, sel types.SelectionMask) error {
		visits++
		if len(sel.Words) == 0 {
			t.Fatalf("selection mask has no words")
		}
		if firstWord == nil {
			firstWord = &sel.Words[0]
			return nil
		}
		if &sel.Words[0] != firstWord {
			t.Fatalf("selection mask was not reused")
		}
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visits != 2 {
		t.Fatalf("visits = %d, want 2", visits)
	}
}

func TestSegmentScanIteratorPredicateSkipsEmptyPage(t *testing.T) {
	batch := predicateBatch(t)
	stats := &ExecStats{}
	it := SegmentScanIterator{
		Segments:  []ScanSegment{{Pages: []ScanPage{{Batch: batch, PayloadBytes: 64}}}},
		Predicate: NewPredicateEvaluator(Predicate{Column: "tenant_id", Op: PredicateOpEq, Int64: 999}),
		Stats:     stats,
	}
	visits := 0
	if err := it.ForEach(func(types.Batch, types.SelectionMask) error {
		visits++
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visits != 0 || stats.RowsMatched != 0 || stats.PagesCandidate != 1 {
		t.Fatalf("visits = %d stats = %#v", visits, stats)
	}
}

func TestSegmentScanIteratorCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	it := SegmentScanIterator{Context: ctx, Segments: []ScanSegment{{Pages: []ScanPage{{Batch: predicateBatch(t)}}}}}
	if err := it.ForEach(func(types.Batch, types.SelectionMask) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestSegmentScanIteratorReadsPersistedSegment(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	batches := []types.Batch{
		segmentBatch(t, []int64{1, 2}, []string{"signup", "checkout"}),
		segmentBatch(t, []int64{3, 4, 5}, []string{"login", "signup", "checkout"}),
	}
	meta, err := WriteSegment(path, 21, batches)
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	stats := &ExecStats{}
	it := SegmentScanIterator{Segments: []ScanSegment{{Path: path, Meta: meta}}, Stats: stats}

	visits := 0
	rows := 0
	if err := it.ForEach(func(batch types.Batch, sel types.SelectionMask) error {
		visits++
		rows += sel.PopCount()
		if batch.Len != sel.PopCount() {
			t.Fatalf("batch Len = %d popcount = %d", batch.Len, sel.PopCount())
		}
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visits != 2 || rows != 5 {
		t.Fatalf("visits = %d rows = %d, want 2/5", visits, rows)
	}
	if stats.SegmentsTotal != 1 || stats.PagesTotal != 2 || stats.RowsMatched != 5 || stats.PayloadBytesRead != int64(segmentPayloadBytes(meta)) {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestSegmentScanIteratorReusesPersistedDecodeBuffers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	batches := []types.Batch{
		segmentBatch(t, []int64{1, 2}, []string{"signup", "checkout"}),
		segmentBatch(t, []int64{3, 4}, []string{"login", "purchase"}),
	}
	meta, err := WriteSegment(path, 211, batches)
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	it := SegmentScanIterator{Segments: []ScanSegment{{Path: path, Meta: meta}}}

	visits := 0
	var firstValue *int64
	if err := it.ForEach(func(batch types.Batch, sel types.SelectionMask) error {
		visits++
		if sel.PopCount() != batch.Len {
			t.Fatalf("selection = %#v batch = %#v", sel, batch)
		}
		if len(batch.Columns[0].V.I64) == 0 {
			t.Fatalf("missing tenant_id values: %#v", batch)
		}
		value := &batch.Columns[0].V.I64[0]
		if firstValue == nil {
			firstValue = value
			return nil
		}
		if value != firstValue {
			t.Fatalf("persisted scan did not reuse decoded int64 buffer")
		}
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visits != 2 {
		t.Fatalf("visits = %d, want 2", visits)
	}
}

func TestSegmentScanIteratorPersistedPredicateSkipsEmptyPage(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	batches := []types.Batch{
		segmentBatch(t, []int64{42, 7}, []string{"checkout", "login"}),
		segmentBatch(t, []int64{1, 2}, []string{"signup", "checkout"}),
	}
	meta, err := WriteSegment(path, 22, batches)
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	stats := &ExecStats{}
	it := SegmentScanIterator{
		Segments:  []ScanSegment{{Path: path, Meta: meta}},
		Predicate: NewPredicateEvaluator(Predicate{Column: "tenant_id", Op: PredicateOpEq, Int64: 42}),
		Stats:     stats,
	}

	visits := 0
	if err := it.ForEach(func(batch types.Batch, sel types.SelectionMask) error {
		visits++
		if batch.Columns[0].V.I64[0] != 42 || sel.PopCount() != 1 {
			t.Fatalf("batch = %#v sel = %#v", batch, sel)
		}
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visits != 1 || stats.PagesCandidate != 2 || stats.RowsMatched != 1 {
		t.Fatalf("visits = %d stats = %#v", visits, stats)
	}
}

func TestSegmentScanIteratorReadsOutputColumns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	meta, err := WriteSegment(path, 221, []types.Batch{segmentBatch(t, []int64{42, 7}, []string{"checkout", "login"})})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	infos, err := buildSegmentPageInfos(meta, nil)
	if err != nil {
		t.Fatalf("buildSegmentPageInfos: %v", err)
	}
	pred := Predicate{Column: "event_type", Op: PredicateOpEq, Text: "checkout"}
	stats := &ExecStats{}
	it := SegmentScanIterator{
		Segments:      []ScanSegment{{Path: path, Meta: meta, PageInfos: infos}},
		Predicate:     NewPredicateEvaluator(pred),
		Prune:         pred,
		OutputColumns: []string{"event_type"},
		Stats:         stats,
	}
	visits := 0
	if err := it.ForEach(func(batch types.Batch, sel types.SelectionMask) error {
		visits++
		if len(batch.Columns) != 1 || batch.Columns[0].Name != "event_type" {
			t.Fatalf("columns = %#v", batch.Columns)
		}
		if sel.PopCount() != 1 || !sel.IsSet(0) {
			t.Fatalf("selection = %#v", sel)
		}
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visits != 1 {
		t.Fatalf("visits = %d, want 1", visits)
	}
	if want := int64(meta.Columns[1].Pages[0].Length); stats.PayloadBytesRead != want {
		t.Fatalf("PayloadBytesRead = %d, want %d", stats.PayloadBytesRead, want)
	}
}

func TestSegmentScanIteratorReadsPredicateOnlyFORColumnEncoded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	meta, err := WriteSegment(path, 222, []types.Batch{segmentBatch(t, []int64{42, 7}, []string{"checkout", "login"})})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	if meta.Columns[0].Pages[0].Encoding != types.EncodingFORBitPack {
		t.Fatalf("encoding = %s, want for+bitpack", meta.Columns[0].Pages[0].Encoding)
	}
	pred := Predicate{Column: "tenant_id", Op: PredicateOpEq, Int64: 42}
	it := SegmentScanIterator{
		Segments:      []ScanSegment{{Path: path, Meta: meta}},
		Predicate:     NewPredicateEvaluator(pred),
		OutputColumns: []string{"event_type"},
	}
	visits := 0
	if err := it.ForEach(func(batch types.Batch, sel types.SelectionMask) error {
		visits++
		col, ok := columnByName(batch, "tenant_id")
		if !ok {
			t.Fatalf("missing tenant_id in batch %#v", batch.Columns)
		}
		if col.V.Encoding != types.EncodingFORBitPack || len(col.V.I64) != 0 {
			t.Fatalf("tenant_id vector = %#v", col.V)
		}
		assertMaskRows(t, sel, []int{0})
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visits != 1 {
		t.Fatalf("visits = %d, want 1", visits)
	}
}

func TestSegmentScanIteratorLateMaterializesSelectedFOROutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	meta, err := WriteSegment(path, 223, []types.Batch{segmentBatch(t, []int64{42, 7}, []string{"checkout", "login"})})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	if meta.Columns[0].Pages[0].Encoding != types.EncodingFORBitPack {
		t.Fatalf("encoding = %s, want for+bitpack", meta.Columns[0].Pages[0].Encoding)
	}
	stats := &ExecStats{}
	pred := Predicate{Column: "event_type", Op: PredicateOpEq, Text: "checkout"}
	it := SegmentScanIterator{
		Segments:      []ScanSegment{{Path: path, Meta: meta}},
		Predicate:     NewPredicateEvaluator(pred),
		OutputColumns: []string{"tenant_id"},
		Stats:         stats,
	}
	visits := 0
	if err := it.ForEach(func(batch types.Batch, sel types.SelectionMask) error {
		visits++
		col, ok := columnByName(batch, "tenant_id")
		if !ok {
			t.Fatalf("missing tenant_id in batch %#v", batch.Columns)
		}
		if col.V.Encoding != types.EncodingFlat || col.V.I64[0] != 42 {
			t.Fatalf("tenant_id vector = %#v", col.V)
		}
		assertMaskRows(t, sel, []int{0})
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visits != 1 {
		t.Fatalf("visits = %d, want 1", visits)
	}
	wantPayload := int64(meta.Columns[0].Pages[0].Length + meta.Columns[1].Pages[0].Length)
	if stats.PayloadBytesRead != wantPayload {
		t.Fatalf("PayloadBytesRead = %d, want %d", stats.PayloadBytesRead, wantPayload)
	}
}

func TestSegmentScanIteratorLateMaterializesEncodedFOROutputForRequester(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	meta, err := WriteSegment(path, 228, []types.Batch{segmentBatch(t, []int64{42, 7}, []string{"checkout", "login"})})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	if meta.Columns[0].Pages[0].Encoding != types.EncodingFORBitPack {
		t.Fatalf("encoding = %s, want for+bitpack", meta.Columns[0].Pages[0].Encoding)
	}
	pred := Predicate{Column: "event_type", Op: PredicateOpEq, Text: "checkout"}
	it := SegmentScanIterator{
		Segments:             []ScanSegment{{Path: path, Meta: meta}},
		Predicate:            NewPredicateEvaluator(pred),
		OutputColumns:        []string{"tenant_id"},
		EncodedOutputColumns: map[string]struct{}{"tenant_id": {}},
	}
	visits := 0
	if err := it.ForEach(func(batch types.Batch, sel types.SelectionMask) error {
		visits++
		col, ok := columnByName(batch, "tenant_id")
		if !ok {
			t.Fatalf("missing tenant_id in batch %#v", batch.Columns)
		}
		if col.V.Encoding != types.EncodingFORBitPack || len(col.V.I64) != 0 {
			t.Fatalf("tenant_id vector = %#v", col.V)
		}
		assertMaskRows(t, sel, []int{0})
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visits != 1 {
		t.Fatalf("visits = %d, want 1", visits)
	}
}

func TestSegmentScanIteratorLateMaterializesSelectedPlainOutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	meta, err := WriteSegment(path, 225, []types.Batch{floatOutputBatch(t, []int64{1, 42, 3}, []float64{1.25, 2.5, 9.75})})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	if meta.Columns[1].Pages[0].Encoding != types.EncodingFlat {
		t.Fatalf("score encoding = %s, want flat", meta.Columns[1].Pages[0].Encoding)
	}
	pred := Predicate{Column: "tenant_id", Op: PredicateOpEq, Int64: 42}
	it := SegmentScanIterator{
		Segments:      []ScanSegment{{Path: path, Meta: meta}},
		Predicate:     NewPredicateEvaluator(pred),
		OutputColumns: []string{"score"},
	}
	visits := 0
	if err := it.ForEach(func(batch types.Batch, sel types.SelectionMask) error {
		visits++
		col, ok := columnByName(batch, "score")
		if !ok {
			t.Fatalf("missing score in batch %#v", batch.Columns)
		}
		if col.V.Encoding != types.EncodingFlat || col.V.F64[1] != 2.5 {
			t.Fatalf("score vector = %#v", col.V)
		}
		if col.V.F64[0] != 0 || col.V.F64[2] != 0 {
			t.Fatalf("unselected plain values were materialized: %#v", col.V.F64)
		}
		assertMaskRows(t, sel, []int{1})
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visits != 1 {
		t.Fatalf("visits = %d, want 1", visits)
	}
}

func TestSegmentScanIteratorLateMaterializesSelectedDictionaryOutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	meta, err := WriteSegment(path, 226, []types.Batch{segmentBatch(t, []int64{1, 2, 42, 3}, []string{"checkout", "signup", "login", "checkout"})})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	if meta.Columns[1].Pages[0].Encoding != types.EncodingDictionary {
		t.Fatalf("event_type encoding = %s, want dictionary", meta.Columns[1].Pages[0].Encoding)
	}
	pred := Predicate{Column: "tenant_id", Op: PredicateOpEq, Int64: 42}
	it := SegmentScanIterator{
		Segments:      []ScanSegment{{Path: path, Meta: meta}},
		Predicate:     NewPredicateEvaluator(pred),
		OutputColumns: []string{"event_type"},
	}
	visits := 0
	if err := it.ForEach(func(batch types.Batch, sel types.SelectionMask) error {
		visits++
		col, ok := columnByName(batch, "event_type")
		if !ok {
			t.Fatalf("missing event_type in batch %#v", batch.Columns)
		}
		if col.V.Encoding != types.EncodingDictionary || textValue(t, col.V, 2) != "login" {
			t.Fatalf("event_type vector = %#v", col.V)
		}
		if textValue(t, col.V, 1) == "signup" {
			t.Fatalf("unselected dictionary value was materialized: %#v", col.V.DictIDs)
		}
		assertMaskRows(t, sel, []int{2})
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visits != 1 {
		t.Fatalf("visits = %d, want 1", visits)
	}
}

func TestSegmentScanIteratorLateMaterializesSelectedConstantOutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	meta, err := WriteSegment(path, 227, []types.Batch{segmentBatch(t, []int64{1, 42, 3}, []string{"checkout", "checkout", "checkout"})})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	if meta.Columns[1].Pages[0].Encoding != types.EncodingConstant {
		t.Fatalf("event_type encoding = %s, want constant", meta.Columns[1].Pages[0].Encoding)
	}
	pred := Predicate{Column: "tenant_id", Op: PredicateOpEq, Int64: 42}
	it := SegmentScanIterator{
		Segments:      []ScanSegment{{Path: path, Meta: meta}},
		Predicate:     NewPredicateEvaluator(pred),
		OutputColumns: []string{"event_type"},
	}
	visits := 0
	if err := it.ForEach(func(batch types.Batch, sel types.SelectionMask) error {
		visits++
		col, ok := columnByName(batch, "event_type")
		if !ok {
			t.Fatalf("missing event_type in batch %#v", batch.Columns)
		}
		if col.V.Encoding != types.EncodingFlat || col.V.Var.String(1) != "checkout" {
			t.Fatalf("event_type vector = %#v", col.V)
		}
		if col.V.Var.String(0) != "" || col.V.Var.String(2) != "" {
			t.Fatalf("unselected constant values were materialized: %#v", col.V.Var)
		}
		assertMaskRows(t, sel, []int{1})
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visits != 1 {
		t.Fatalf("visits = %d, want 1", visits)
	}
}

func TestSegmentScanIteratorSkipsLateOutputWhenPredicateMatchesNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	meta, err := WriteSegment(path, 224, []types.Batch{segmentBatch(t, []int64{42, 7}, []string{"checkout", "checkout"})})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	stats := &ExecStats{}
	pred := Predicate{Column: "event_type", Op: PredicateOpNotEq, Text: "checkout"}
	it := SegmentScanIterator{
		Segments:      []ScanSegment{{Path: path, Meta: meta}},
		Predicate:     NewPredicateEvaluator(pred),
		OutputColumns: []string{"tenant_id"},
		Stats:         stats,
	}
	visits := 0
	if err := it.ForEach(func(types.Batch, types.SelectionMask) error {
		visits++
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visits != 0 || stats.RowsMatched != 0 {
		t.Fatalf("visits = %d stats = %#v", visits, stats)
	}
	wantPayload := int64(meta.Columns[1].Pages[0].Length)
	if stats.PayloadBytesRead != wantPayload {
		t.Fatalf("PayloadBytesRead = %d, want %d", stats.PayloadBytesRead, wantPayload)
	}
}

func TestSegmentScanIteratorPrunesPersistedPages(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	batches := []types.Batch{
		segmentBatch(t, []int64{42, 7}, []string{"checkout", "login"}),
		segmentBatch(t, []int64{1, 2}, []string{"signup", "checkout"}),
	}
	meta, err := WriteSegment(path, 23, batches)
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	stats := &ExecStats{}
	pred := Predicate{Column: "tenant_id", Op: PredicateOpEq, Int64: 42}
	it := SegmentScanIterator{
		Segments:  []ScanSegment{{Path: path, Meta: meta}},
		Predicate: NewPredicateEvaluator(pred),
		Prune:     pred,
		Stats:     stats,
	}

	visits := 0
	if err := it.ForEach(func(types.Batch, types.SelectionMask) error {
		visits++
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visits != 1 {
		t.Fatalf("visits = %d, want 1", visits)
	}
	if stats.PagesTotal != 2 || stats.PagesCandidate != 1 || stats.RowsTotal != 4 || stats.RowsCandidate != 2 || stats.RowsMatched != 1 {
		t.Fatalf("stats = %#v", stats)
	}
	wantPayload := int64(meta.Columns[0].Pages[0].Length + meta.Columns[1].Pages[0].Length)
	if stats.PayloadBytesRead != wantPayload {
		t.Fatalf("PayloadBytesRead = %d, want %d", stats.PayloadBytesRead, wantPayload)
	}
}

func TestSegmentScanIteratorPrunesPersistedSegments(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	batches := []types.Batch{
		segmentBatch(t, []int64{1, 2}, []string{"checkout", "login"}),
		segmentBatch(t, []int64{3, 4}, []string{"signup", "checkout"}),
	}
	meta, err := WriteSegment(path, 231, batches)
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	stats := &ExecStats{}
	pred := Predicate{Column: "tenant_id", Op: PredicateOpEq, Int64: 999999}
	it := SegmentScanIterator{Segments: []ScanSegment{{Path: path, Meta: meta}}, Predicate: NewPredicateEvaluator(pred), Prune: pred, Stats: stats}

	visits := 0
	if err := it.ForEach(func(types.Batch, types.SelectionMask) error {
		visits++
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visits != 0 {
		t.Fatalf("visits = %d, want 0", visits)
	}
	if stats.SegmentsTotal != 1 || stats.SegmentsCandidate != 0 || stats.PagesTotal != 2 || stats.PagesCandidate != 0 || stats.RowsTotal != 4 || stats.RowsCandidate != 0 || stats.RowsMatched != 0 || stats.PayloadBytesRead != 0 {
		t.Fatalf("stats = %#v", stats)
	}
}

func TestSegmentScanIteratorPrunesPersistedTextPages(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	batches := []types.Batch{
		segmentBatch(t, []int64{1, 2}, []string{"checkout", "login"}),
		segmentBatch(t, []int64{3, 4}, []string{"signup", "logout"}),
	}
	meta, err := WriteSegment(path, 24, batches)
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	stats := &ExecStats{}
	pred := Predicate{Column: "event_type", Op: PredicateOpEq, Text: "checkout"}
	it := SegmentScanIterator{Segments: []ScanSegment{{Path: path, Meta: meta}}, Predicate: NewPredicateEvaluator(pred), Prune: pred, Stats: stats}

	visits := 0
	if err := it.ForEach(func(types.Batch, types.SelectionMask) error {
		visits++
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visits != 1 || stats.PagesCandidate != 1 || stats.RowsMatched != 1 {
		t.Fatalf("visits = %d stats = %#v", visits, stats)
	}
}

func TestSegmentScanIteratorPrunesPersistedBoolPages(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	batches := []types.Batch{
		boolBatch(t, []bool{true, true}),
		boolBatch(t, []bool{false, false}),
	}
	meta, err := WriteSegment(path, 25, batches)
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	stats := &ExecStats{}
	pred := Predicate{Column: "flag", Op: PredicateOpEq, Bool: true}
	it := SegmentScanIterator{Segments: []ScanSegment{{Path: path, Meta: meta}}, Predicate: NewPredicateEvaluator(pred), Prune: pred, Stats: stats}

	visits := 0
	if err := it.ForEach(func(_ types.Batch, sel types.SelectionMask) error {
		visits++
		if sel.PopCount() != 2 {
			t.Fatalf("selection = %#v", sel)
		}
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visits != 1 || stats.PagesCandidate != 1 || stats.RowsCandidate != 2 || stats.RowsMatched != 2 {
		t.Fatalf("visits = %d stats = %#v", visits, stats)
	}
}

func TestSegmentScanIteratorPrunesPersistedInt16Pages(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	batches := []types.Batch{
		int16Batch(t, []int16{1, 2}),
		int16Batch(t, []int16{10, 11}),
	}
	meta, err := WriteSegment(path, 26, batches)
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	stats := &ExecStats{}
	pred := Predicate{Column: "small_id", Op: PredicateOpEq, Int64: 1}
	it := SegmentScanIterator{Segments: []ScanSegment{{Path: path, Meta: meta}}, Predicate: NewPredicateEvaluator(pred), Prune: pred, Stats: stats}

	visits := 0
	if err := it.ForEach(func(_ types.Batch, sel types.SelectionMask) error {
		visits++
		if sel.PopCount() != 1 || !sel.IsSet(0) {
			t.Fatalf("selection = %#v", sel)
		}
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visits != 1 || stats.PagesCandidate != 1 || stats.RowsCandidate != 2 || stats.RowsMatched != 1 {
		t.Fatalf("visits = %d stats = %#v", visits, stats)
	}
}

func TestSegmentScanIteratorPrunesPersistedInt16ValuePages(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	batches := []types.Batch{
		int16Batch(t, []int16{1, 3}),
		int16Batch(t, []int16{5, 7}),
	}
	meta, err := WriteSegment(path, 262, batches)
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	if meta.Columns[0].Pages[0].Int32Values == nil || meta.Columns[0].Pages[0].Int32Values.Truncated {
		t.Fatalf("int16 value stats = %#v", meta.Columns[0].Pages[0].Int32Values)
	}
	stats := &ExecStats{}
	pred := Predicate{Column: "small_id", Op: PredicateOpEq, Int64: 2}
	it := SegmentScanIterator{Segments: []ScanSegment{{Path: path, Meta: meta}}, Predicate: NewPredicateEvaluator(pred), Prune: pred, Stats: stats}

	visits := 0
	if err := it.ForEach(func(types.Batch, types.SelectionMask) error {
		visits++
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visits != 0 || stats.PagesTotal != 2 || stats.PagesCandidate != 0 || stats.RowsCandidate != 0 || stats.RowsMatched != 0 {
		t.Fatalf("visits = %d stats = %#v", visits, stats)
	}
}

func TestSegmentScanIteratorPrunesPersistedInt64ValuePages(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	batches := []types.Batch{
		segmentBatch(t, []int64{1, 3}, []string{"checkout", "login"}),
		segmentBatch(t, []int64{5, 7}, []string{"signup", "logout"}),
	}
	meta, err := WriteSegment(path, 261, batches)
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	if meta.Columns[0].Pages[0].Int64Values == nil || meta.Columns[0].Pages[0].Int64Values.Truncated {
		t.Fatalf("int64 value stats = %#v", meta.Columns[0].Pages[0].Int64Values)
	}

	stats := &ExecStats{}
	pred := Predicate{Column: "tenant_id", Op: PredicateOpEq, Int64: 2}
	it := SegmentScanIterator{Segments: []ScanSegment{{Path: path, Meta: meta}}, Predicate: NewPredicateEvaluator(pred), Prune: pred, Stats: stats}

	visits := 0
	if err := it.ForEach(func(types.Batch, types.SelectionMask) error {
		visits++
		return nil
	}); err != nil {
		t.Fatalf("ForEach absent: %v", err)
	}
	if visits != 0 || stats.PagesTotal != 2 || stats.PagesCandidate != 0 || stats.RowsCandidate != 0 || stats.RowsMatched != 0 {
		t.Fatalf("absent visits = %d stats = %#v", visits, stats)
	}

	stats = &ExecStats{}
	pred = Predicate{Column: "tenant_id", Op: PredicateOpEq, Int64: 3}
	it = SegmentScanIterator{Segments: []ScanSegment{{Path: path, Meta: meta}}, Predicate: NewPredicateEvaluator(pred), Prune: pred, Stats: stats}
	visits = 0
	if err := it.ForEach(func(_ types.Batch, sel types.SelectionMask) error {
		visits++
		if sel.PopCount() != 1 || !sel.IsSet(1) {
			t.Fatalf("selection = %#v", sel)
		}
		return nil
	}); err != nil {
		t.Fatalf("ForEach present: %v", err)
	}
	if visits != 1 || stats.PagesCandidate != 1 || stats.RowsCandidate != 2 || stats.RowsMatched != 1 {
		t.Fatalf("present visits = %d stats = %#v", visits, stats)
	}
}

func TestSegmentScanIteratorPrunesTruncatedTextStatsWithBloom(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	ids := make([]int64, TextStatsMaxValues+1)
	for i := range ids {
		ids[i] = int64(i)
	}
	events := uniqueEvents(len(ids))
	meta, err := WriteSegment(path, 27, []types.Batch{segmentBatch(t, ids, events)})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	textStats := meta.Columns[1].Pages[0].Text
	if textStats == nil || !textStats.Truncated || len(textStats.hashes) != 0 || len(textStats.HashBloom) == 0 {
		t.Fatalf("text stats = %#v", textStats)
	}

	stats := &ExecStats{}
	pred := Predicate{Column: "event_type", Op: PredicateOpEq, Text: missingTextBloomValueAcross(textPageBloomProbes, textStats.HashBloom)}
	it := SegmentScanIterator{Segments: []ScanSegment{{Path: path, Meta: meta}}, Predicate: NewPredicateEvaluator(pred), Prune: pred, Stats: stats}

	visits := 0
	if err := it.ForEach(func(types.Batch, types.SelectionMask) error {
		visits++
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visits != 0 || stats.PagesTotal != 1 || stats.PagesCandidate != 0 || stats.RowsCandidate != 0 || stats.RowsMatched != 0 || stats.PayloadBytesRead != 0 {
		t.Fatalf("visits = %d stats = %#v", visits, stats)
	}

	stats = &ExecStats{}
	pred = Predicate{Column: "event_type", Op: PredicateOpEq, Text: events[len(events)-1]}
	it = SegmentScanIterator{Segments: []ScanSegment{{Path: path, Meta: meta}}, Predicate: NewPredicateEvaluator(pred), Prune: pred, Stats: stats}
	visits = 0
	if err := it.ForEach(func(_ types.Batch, sel types.SelectionMask) error {
		visits++
		if sel.PopCount() != 1 || !sel.IsSet(len(events)-1) {
			t.Fatalf("selection = %#v", sel)
		}
		return nil
	}); err != nil {
		t.Fatalf("ForEach present: %v", err)
	}
	if visits != 1 || stats.PagesCandidate != 1 || stats.RowsCandidate != int64(len(events)) || stats.RowsMatched != 1 || stats.PayloadBytesRead == 0 {
		t.Fatalf("present visits = %d stats = %#v", visits, stats)
	}
}

func TestPredicatePruneTextStats(t *testing.T) {
	assertPruneCandidate := func(t *testing.T, meta SegmentMeta, pred Predicate, want bool) {
		t.Helper()
		if got := BindPrunePredicate(pred, meta).PageCandidate(0); got != want {
			t.Fatalf("PageCandidate(%#v) = %v, want %v", pred, got, want)
		}
	}

	exactMeta := SegmentMeta{Columns: []ColumnMeta{{Name: "event_type", Pages: []PageMeta{{Rows: 2, Text: &TextStats{Values: []string{"checkout", "login"}, Counts: []uint32{1, 1}}}}}}}
	assertPruneCandidate(t, exactMeta, Predicate{Column: "event_type", Op: PredicateOpEq, Text: "missing"}, false)
	assertPruneCandidate(t, exactMeta, Predicate{Column: "event_type", Op: PredicateOpEq, Text: "checkout"}, true)
	assertPruneCandidate(t, exactMeta, Predicate{Column: "event_type", Op: PredicateOpIn, Texts: []string{"missing", "absent"}}, false)
	assertPruneCandidate(t, exactMeta, Predicate{Column: "event_type", Op: PredicateOpIn, Texts: []string{"missing", "login"}}, true)

	bloom := newTextPageHashBloom(2)
	hashBloomAdd(bloom, textHash32String("checkout"), textPageBloomProbes)
	hashBloomAdd(bloom, textHash32String("login"), textPageBloomProbes)
	truncatedMeta := SegmentMeta{Columns: []ColumnMeta{{Name: "event_type", Pages: []PageMeta{{Rows: 3, Text: &TextStats{HashBloom: bloom, Truncated: true}}}}}}
	absent := missingTextBloomValueAcross(textPageBloomProbes, truncatedMeta.Columns[0].Pages[0].Text.HashBloom)
	assertPruneCandidate(t, truncatedMeta, Predicate{Column: "event_type", Op: PredicateOpEq, Text: absent}, false)
	assertPruneCandidate(t, truncatedMeta, Predicate{Column: "event_type", Op: PredicateOpEq, Text: "checkout"}, true)
	assertPruneCandidate(t, truncatedMeta, Predicate{Column: "event_type", Op: PredicateOpIn, Texts: []string{absent, absent}}, false)
	assertPruneCandidate(t, truncatedMeta, Predicate{Column: "event_type", Op: PredicateOpIn, Texts: []string{absent, "login"}}, true)
}

func TestPredicatePruneSegmentTextHashStats(t *testing.T) {
	segmentAPath := filepath.Join(t.TempDir(), "segment-a.dsv3")
	segmentBPath := filepath.Join(t.TempDir(), "segment-b.dsv3")
	ids := make([]int64, TextStatsMaxValues+1)
	for i := range ids {
		ids[i] = int64(i)
	}
	eventsA := uniqueEventsWithPrefix("segment_a_", len(ids))
	eventsB := uniqueEventsWithPrefix("segment_b_", len(ids))
	metaA, err := WriteSegment(segmentAPath, 281, []types.Batch{segmentBatch(t, ids, eventsA)})
	if err != nil {
		t.Fatalf("WriteSegment A: %v", err)
	}
	metaB, err := WriteSegment(segmentBPath, 282, []types.Batch{segmentBatch(t, ids, eventsB)})
	if err != nil {
		t.Fatalf("WriteSegment B: %v", err)
	}
	if metaA.Columns[1].Text == nil || !metaA.Columns[1].Text.Truncated || len(metaA.Columns[1].Text.hashes) != 0 || len(metaA.Columns[1].Text.HashBloom) == 0 {
		t.Fatalf("segment A text stats = %#v", metaA.Columns[1].Text)
	}
	if metaB.Columns[1].Text == nil || !metaB.Columns[1].Text.Truncated || len(metaB.Columns[1].Text.hashes) != 0 || len(metaB.Columns[1].Text.HashBloom) == 0 {
		t.Fatalf("segment B text stats = %#v", metaB.Columns[1].Text)
	}

	pred := Predicate{Column: "event_type", Op: PredicateOpEq, Text: eventsB[len(eventsB)/2]}
	if got := BindPrunePredicate(pred, metaA).SegmentCandidate(); got {
		t.Fatalf("segment A candidate for segment B value = true")
	}
	if got := BindPrunePredicate(pred, metaB).SegmentCandidate(); !got {
		t.Fatalf("segment B candidate for segment B value = false")
	}

	absent := missingTextBloomValueAcross(textSegmentBloomProbes, metaA.Columns[1].Text.HashBloom, metaB.Columns[1].Text.HashBloom)
	pred = Predicate{Column: "event_type", Op: PredicateOpEq, Text: absent}
	if got := BindPrunePredicate(pred, metaA).SegmentCandidate(); got {
		t.Fatalf("segment A candidate for absent value %q = true", absent)
	}
	if got := BindPrunePredicate(pred, metaB).SegmentCandidate(); got {
		t.Fatalf("segment B candidate for absent value %q = true", absent)
	}
}

func TestSegmentScanIteratorPrunesUUIDStatsWithBloom(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	values := make([]types.UUID16, TextStatsMaxValues+1)
	for i := range values {
		values[i] = testUUID16(i + 1)
	}
	meta, err := WriteSegment(path, 391, []types.Batch{uuidBatch(t, values)})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	uuidStats := meta.Columns[0].Pages[0].UUID
	if uuidStats == nil || len(uuidStats.hashes) != 0 || len(uuidStats.HashBloom) == 0 {
		t.Fatalf("uuid stats = %#v", uuidStats)
	}

	stats := &ExecStats{}
	absent := missingUUIDBloomValueAcross(textPageBloomProbes, uuidStats.HashBloom)
	pred := Predicate{Column: "event_uuid", Op: PredicateOpEq, UUID: absent}
	it := SegmentScanIterator{Segments: []ScanSegment{{Path: path, Meta: meta}}, Predicate: NewPredicateEvaluator(pred), Prune: pred, Stats: stats}
	visits := 0
	if err := it.ForEach(func(types.Batch, types.SelectionMask) error {
		visits++
		return nil
	}); err != nil {
		t.Fatalf("ForEach absent: %v", err)
	}
	if visits != 0 || stats.PagesTotal != 1 || stats.PagesCandidate != 0 || stats.PayloadBytesRead != 0 {
		t.Fatalf("absent visits = %d stats = %#v", visits, stats)
	}

	stats = &ExecStats{}
	pred = Predicate{Column: "event_uuid", Op: PredicateOpEq, UUID: values[len(values)-1]}
	it = SegmentScanIterator{Segments: []ScanSegment{{Path: path, Meta: meta}}, Predicate: NewPredicateEvaluator(pred), Prune: pred, Stats: stats}
	visits = 0
	if err := it.ForEach(func(_ types.Batch, sel types.SelectionMask) error {
		visits++
		if sel.PopCount() != 1 || !sel.IsSet(len(values)-1) {
			t.Fatalf("selection = %#v", sel)
		}
		return nil
	}); err != nil {
		t.Fatalf("ForEach present: %v", err)
	}
	if visits != 1 || stats.PagesCandidate != 1 || stats.RowsMatched != 1 || stats.PayloadBytesRead == 0 {
		t.Fatalf("present visits = %d stats = %#v", visits, stats)
	}
}

func TestTextSegmentBloomSizing(t *testing.T) {
	if got := textPageBloomWordsFor(types.StandardBatchRows); got != 512 {
		t.Fatalf("page bloom words = %d, want %d", got, 512)
	}
	if got := textSegmentBloomWordsFor(TextStatsMaxValues + 1); got != textSegmentBloomMinWords {
		t.Fatalf("small bloom words = %d, want %d", got, textSegmentBloomMinWords)
	}
	if got := textSegmentBloomWordsFor(DefaultSegmentRows); got != 32*1024 {
		t.Fatalf("default segment bloom words = %d, want %d", got, 32*1024)
	}
	if got := textSegmentBloomWordsFor(2_097_152); got != textSegmentBloomMaxWords {
		t.Fatalf("large segment bloom words = %d, want %d", got, textSegmentBloomMaxWords)
	}
}

func TestSegmentScanIteratorDoesNotPruneTruncatedInt64ValueStats(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	ids := make([]int64, ValueStatsMaxValues+1)
	for i := range ids {
		ids[i] = int64(i * 2)
	}
	meta, err := WriteSegment(path, 271, []types.Batch{segmentBatch(t, ids, uniqueEvents(len(ids)))})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	if meta.Columns[0].Pages[0].Int64Values == nil || !meta.Columns[0].Pages[0].Int64Values.Truncated {
		t.Fatalf("int64 value stats = %#v", meta.Columns[0].Pages[0].Int64Values)
	}
	stats := &ExecStats{}
	pred := Predicate{Column: "tenant_id", Op: PredicateOpEq, Int64: 1}
	it := SegmentScanIterator{Segments: []ScanSegment{{Path: path, Meta: meta}}, Predicate: NewPredicateEvaluator(pred), Prune: pred, Stats: stats}

	visits := 0
	if err := it.ForEach(func(types.Batch, types.SelectionMask) error {
		visits++
		return nil
	}); err != nil {
		t.Fatalf("ForEach: %v", err)
	}
	if visits != 0 || stats.PagesCandidate != 1 || stats.RowsMatched != 0 {
		t.Fatalf("visits = %d stats = %#v", visits, stats)
	}
}

func TestPredicatePruneValueStatsCompoundSafety(t *testing.T) {
	meta := SegmentMeta{Columns: []ColumnMeta{{Name: "tenant_id", Pages: []PageMeta{{Rows: 2, Int64: &Int64Stats{Min: 1, Max: 3}, Int64Values: &Int64ValueStats{Values: []int64{1, 3}}}}}}}
	eq2 := Predicate{Column: "tenant_id", Op: PredicateOpEq, Int64: 2}
	eq3 := Predicate{Column: "tenant_id", Op: PredicateOpEq, Int64: 3}
	assertPruneCandidate := func(pred Predicate, want bool) {
		t.Helper()
		if got := BindPrunePredicate(pred, meta).PageCandidate(0); got != want {
			t.Fatalf("PageCandidate(%#v) = %v, want %v", pred, got, want)
		}
	}

	assertPruneCandidate(eq2, false)
	assertPruneCandidate(Predicate{Op: PredicateAnd, Children: []Predicate{eq2, eq3}}, false)
	assertPruneCandidate(Predicate{Op: PredicateOr, Children: []Predicate{eq2, eq3}}, true)

	meta.Columns[0].Pages[0].Int64Values.Truncated = true
	assertPruneCandidate(eq2, true)
}

func floatOutputBatch(t *testing.T, ids []int64, scores []float64) types.Batch {
	t.Helper()
	if len(ids) != len(scores) {
		t.Fatalf("ids/scores length mismatch: %d/%d", len(ids), len(scores))
	}
	batch, err := types.NewBatch([]types.Column{
		{Name: "tenant_id", Type: types.Int64, V: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: len(ids), I64: append([]int64(nil), ids...)}},
		{Name: "score", Type: types.Float64, V: types.Vec{Kind: types.VecFloat64, Encoding: types.EncodingFlat, Len: len(scores), F64: append([]float64(nil), scores...)}},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func uuidBatch(t *testing.T, values []types.UUID16) types.Batch {
	t.Helper()
	batch, err := types.NewBatch([]types.Column{{Name: "event_uuid", Type: types.UUID, V: types.Vec{Kind: types.VecUUID, Encoding: types.EncodingFlat, Len: len(values), UUID: append([]types.UUID16(nil), values...)}}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func segmentPayloadBytes(meta SegmentMeta) int {
	total := 0
	for _, col := range meta.Columns {
		for _, page := range col.Pages {
			total += int(page.Length)
		}
	}
	return total
}

func missingTextBloomValueAcross(probes uint64, blooms ...[]uint64) string {
	for i := 0; i < 1_000_000; i++ {
		value := fmt.Sprintf("missing_%d", i)
		found := false
		for _, bloom := range blooms {
			if hashBloomHas(bloom, textHash32String(value), probes) {
				found = true
				break
			}
		}
		if !found {
			return value
		}
	}
	panic("could not find missing text bloom value")
}

func missingUUIDBloomValueAcross(probes uint64, blooms ...[]uint64) types.UUID16 {
	for i := 0; i < 1_000_000; i++ {
		value := testUUID16(i + 1)
		found := false
		for _, bloom := range blooms {
			if hashBloomHas(bloom, uuidHash32(value), probes) {
				found = true
				break
			}
		}
		if !found {
			return value
		}
	}
	panic("could not find missing uuid bloom value")
}

func testUUID16(value int) types.UUID16 {
	var out types.UUID16
	out[12] = byte(value >> 24)
	out[13] = byte(value >> 16)
	out[14] = byte(value >> 8)
	out[15] = byte(value)
	return out
}

func uniqueEventsWithPrefix(prefix string, n int) []string {
	events := make([]string, n)
	for i := range events {
		events[i] = fmt.Sprintf("%s%04d", prefix, i)
	}
	return events
}
