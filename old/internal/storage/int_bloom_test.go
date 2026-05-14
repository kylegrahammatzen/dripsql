package storage

import (
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestInt64BloomBuiltOnPageTruncation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	ids := make([]int64, ValueStatsMaxValues+1)
	for i := range ids {
		ids[i] = int64(i * 2)
	}
	meta, err := WriteSegment(path, 1, []types.Batch{segmentBatch(t, ids, uniqueEvents(len(ids)))})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	page := meta.Columns[0].Pages[0]
	if !page.Int64Values.Truncated || len(page.Int64Values.HashBloom) == 0 {
		t.Fatalf("page bloom not built: %#v", page.Int64Values)
	}
	if meta.Columns[0].Stats.Int64Values == nil || len(meta.Columns[0].Stats.Int64Values.HashBloom) == 0 {
		t.Fatalf("segment bloom not built: %#v", meta.Columns[0].Stats.Int64Values)
	}
}

func TestInt64BloomNotBuiltWhenAllPagesFitInValues(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	ids := []int64{1, 2, 3, 4, 5}
	meta, err := WriteSegment(path, 2, []types.Batch{segmentBatch(t, ids, uniqueEvents(len(ids)))})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	page := meta.Columns[0].Pages[0]
	if page.Int64Values.Truncated || len(page.Int64Values.HashBloom) != 0 {
		t.Fatalf("expected no bloom on small page: %#v", page.Int64Values)
	}
	if meta.Columns[0].Stats.Int64Values != nil {
		t.Fatalf("expected no segment-level int64 bloom: %#v", meta.Columns[0].Stats.Int64Values)
	}
}

func TestInt64BloomRoundTripsThroughFooter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "segment.dsv3")
	ids := make([]int64, ValueStatsMaxValues+1)
	for i := range ids {
		ids[i] = int64(i*2 + 1)
	}
	meta, err := WriteSegment(path, 3, []types.Batch{segmentBatch(t, ids, uniqueEvents(len(ids)))})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	reopened, _, err := ReadSegmentFooter(path)
	if err != nil {
		t.Fatalf("ReadSegmentFooter: %v", err)
	}
	if err := reopened.LoadAllColumns(); err != nil {
		t.Fatalf("LoadAllColumns: %v", err)
	}
	originalSeg := meta.Columns[0].Stats.Int64Values
	roundtripSeg := reopened.Columns[0].Stats.Int64Values
	if originalSeg == nil || roundtripSeg == nil {
		t.Fatalf("missing segment bloom: original=%v reopened=%v", originalSeg, roundtripSeg)
	}
	if len(originalSeg.HashBloom) != len(roundtripSeg.HashBloom) {
		t.Fatalf("segment bloom size differs: %d vs %d", len(originalSeg.HashBloom), len(roundtripSeg.HashBloom))
	}
	for i, word := range originalSeg.HashBloom {
		if word != roundtripSeg.HashBloom[i] {
			t.Fatalf("segment bloom word %d differs: %x vs %x", i, word, roundtripSeg.HashBloom[i])
		}
	}
	originalPage := meta.Columns[0].Pages[0].Int64Values
	roundtripPage := reopened.Columns[0].Pages[0].Int64Values
	if len(originalPage.HashBloom) != len(roundtripPage.HashBloom) {
		t.Fatalf("page bloom size differs: %d vs %d", len(originalPage.HashBloom), len(roundtripPage.HashBloom))
	}
}

func TestInt64BloomPruneAcceptsKnownAbsentValue(t *testing.T) {
	stats := Int64ValueStats{Truncated: true, HashBloom: newTextHashBloom(64)}
	for i := int64(0); i < 100; i += 2 {
		hashBloomAdd(stats.HashBloom, intHash32(i), textPageBloomProbes)
	}
	pred := BoundPredicate{op: PredicateOpEq, int64Value: 7}
	if pruneInt64ValuePageCandidate(stats, pred) {
		t.Fatal("expected bloom to prune absent value 7")
	}
	predHit := BoundPredicate{op: PredicateOpEq, int64Value: 4}
	if !pruneInt64ValuePageCandidate(stats, predHit) {
		t.Fatal("expected bloom to keep candidate value 4")
	}
}

func TestInt64BloomPruneFallsThroughOnGtLt(t *testing.T) {
	stats := Int64ValueStats{Truncated: true, HashBloom: newTextHashBloom(64)}
	for i := int64(0); i < 100; i += 2 {
		hashBloomAdd(stats.HashBloom, intHash32(i), textPageBloomProbes)
	}
	pred := BoundPredicate{op: PredicateOpGreater, int64Value: 7}
	if !pruneInt64ValuePageCandidate(stats, pred) {
		t.Fatal("range predicates must not be pruned by hash bloom")
	}
}

