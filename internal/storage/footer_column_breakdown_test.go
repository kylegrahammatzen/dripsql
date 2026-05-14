package storage

import (
	"os"
	"testing"
	"time"
)

// TestFooterColumnBreakdown measures the cost of fully decoding a segment
// footer vs. only decoding a single column's worth of metadata. Run with:
//
//	go test ./internal/storage -run TestFooterColumnBreakdown -v
//
// It reads a real segment file from the structured bench DB so the numbers
// reflect production-shaped metadata. Skipped when the file is missing or
// written in a pre-v4 format (regenerate by running cmd/bench).
func TestFooterColumnBreakdown(t *testing.T) {
	const path = `..\..\db\bench\structured\seg-default\tables\events\segments\0000000000000001.dsv3`
	if _, err := os.Stat(path); err != nil {
		t.Skipf("bench segment not available at %s: %v", path, err)
	}

	meta, segSize, err := ReadSegmentFooter(path)
	if err != nil {
		t.Skipf("bench segment at %s not readable as v4 (regenerate with cmd/bench): %v", path, err)
	}
	if err := meta.LoadAllColumns(); err != nil {
		t.Fatalf("LoadAllColumns: %v", err)
	}
	t.Logf("Segment file: %d bytes, %d rows, %d pages, %d columns",
		segSize, meta.Rows, len(meta.PageRowCounts), len(meta.Columns))

	// Total marshaled bytes for the whole footer (post-marshal, pre-compress).
	fullRaw, err := marshalSegmentMetaRaw(meta)
	if err != nil {
		t.Fatalf("marshal full meta: %v", err)
	}
	fullCompressed, err := compressSegmentFooter(fullRaw)
	if err != nil {
		t.Fatalf("compress full footer: %v", err)
	}

	// Per-column raw bytes: build a SegmentMeta with just that column.
	type colStat struct {
		name      string
		rawBytes  int
		compBytes int
		decompNs  int64
	}
	stats := make([]colStat, len(meta.Columns))
	const iters = 20
	for i, col := range meta.Columns {
		oneCol := SegmentMeta{
			ID:       meta.ID,
			Rows:     meta.Rows,
			PageRows: meta.PageRows,
			Columns:  []ColumnMeta{col},
		}
		raw, err := marshalSegmentMetaRaw(oneCol)
		if err != nil {
			t.Fatalf("marshal one-col meta %q: %v", col.Name, err)
		}
		comp, err := compressSegmentFooter(raw)
		if err != nil {
			t.Fatalf("compress one-col footer %q: %v", col.Name, err)
		}
		stats[i] = colStat{name: col.Name, rawBytes: len(raw), compBytes: len(comp)}

		// Decompression time for this one-column blob, averaged over iters.
		start := time.Now()
		for range iters {
			out, err := decompressSegmentFooter(comp, len(raw))
			if err != nil || len(out) != len(raw) {
				t.Fatalf("decompress one-col %q: %v", col.Name, err)
			}
		}
		stats[i].decompNs = time.Since(start).Nanoseconds() / int64(iters)
	}

	// Full-footer decompression time, same number of iters for comparison.
	startFull := time.Now()
	for range iters {
		out, err := decompressSegmentFooter(fullCompressed, len(fullRaw))
		if err != nil || len(out) != len(fullRaw) {
			t.Fatalf("decompress full: %v", err)
		}
	}
	fullDecompNs := time.Since(startFull).Nanoseconds() / int64(iters)

	t.Logf("Full footer: raw=%d B, compressed=%d B, decompress=%s",
		len(fullRaw), len(fullCompressed), time.Duration(fullDecompNs))
	t.Logf("Per-column breakdown:")
	var sumRaw, sumComp, sumDecomp int64
	for _, s := range stats {
		t.Logf("  %-12s raw=%7d B  comp=%7d B  decompress=%8s  (%.1f%% of full raw, %.1f%% of full compressed, %.1f%% of full decompress)",
			s.name, s.rawBytes, s.compBytes, time.Duration(s.decompNs),
			100*float64(s.rawBytes)/float64(len(fullRaw)),
			100*float64(s.compBytes)/float64(len(fullCompressed)),
			100*float64(s.decompNs)/float64(fullDecompNs),
		)
		sumRaw += int64(s.rawBytes)
		sumComp += int64(s.compBytes)
		sumDecomp += s.decompNs
	}
	t.Logf("Sum of one-column footers: raw=%d B (%.1fx full), comp=%d B (%.1fx full), decompress=%s (%.1fx full)",
		sumRaw, float64(sumRaw)/float64(len(fullRaw)),
		sumComp, float64(sumComp)/float64(len(fullCompressed)),
		time.Duration(sumDecomp), float64(sumDecomp)/float64(fullDecompNs),
	)
	t.Logf("Hypothesis check: decoding only the predicate column (e.g. user_id) instead of all %d columns",
		len(meta.Columns))
}
