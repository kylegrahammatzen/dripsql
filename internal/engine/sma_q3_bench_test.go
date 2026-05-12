package engine

import (
	"testing"
)

// TestSMA_CrossCountFootprintQ3 documents what a per-page (event_type × country)
// cross-count projection would cost and how fast Q3 (`SELECT country, count(*)
// WHERE event_type='checkout' GROUP BY country`) reads from it.
//
// Q3 today scans 19.56 MiB of payload across 5 segments × 4883 pages to
// evaluate the event_type predicate, then aggregates the surviving country
// values. With per-page cross-counts the query becomes a metadata-only merge
// over (event_type=checkout, country=*) tuples — exactly the same shape as
// the SMA bench that was already proven sub-ms.

const (
	q3PagesPer    = 4883
	q3Segments    = 5
	q3EventTypes  = 4
	q3Countries   = 8
	q3TotalPages  = q3PagesPer * q3Segments
)

// pageCrossCount[i][j] = count of rows where event_type==Values[i] AND country==Values[j].
// Per page: 4 × 8 = 32 entries × 8 bytes = 256 bytes.
type pageCrossCount [q3EventTypes][q3Countries]int64

func buildQ3CrossCounts() [][]pageCrossCount {
	all := make([][]pageCrossCount, q3Segments)
	for s := range all {
		pages := make([]pageCrossCount, q3PagesPer)
		rowsPerPage := int64(2_097_152 / q3PagesPer)
		// Even split across event_type × country buckets.
		per := rowsPerPage / int64(q3EventTypes*q3Countries)
		for p := range pages {
			for et := 0; et < q3EventTypes; et++ {
				for c := 0; c < q3Countries; c++ {
					pages[p][et][c] = per
				}
			}
		}
		all[s] = pages
	}
	return all
}

func BenchmarkSMA_Q3CrossCountMergeOnly(b *testing.B) {
	cross := buildQ3CrossCounts()
	const checkoutEventID = 1
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var counts [q3Countries]int64
		for _, segment := range cross {
			for p := range segment {
				page := &segment[p]
				for c := 0; c < q3Countries; c++ {
					counts[c] += page[checkoutEventID][c]
				}
			}
		}
		_ = counts
	}
}

func TestSMA_Q3CrossCountFootprint(t *testing.T) {
	bytesPerPage := q3EventTypes * q3Countries * 8
	totalBytes := bytesPerPage * q3TotalPages
	t.Logf("Q3 cross-count footprint @ 10M rows:")
	t.Logf("  pages:           %d (%d segments x %d pages)", q3TotalPages, q3Segments, q3PagesPer)
	t.Logf("  bytes per page:  %d (%dx%d cells x 8B)", bytesPerPage, q3EventTypes, q3Countries)
	t.Logf("  total metadata:  %d bytes (%.2f MiB)", totalBytes, float64(totalBytes)/(1024*1024))
	t.Logf("  base table:      %d MiB compressed", 439)
	t.Logf("  overhead:        %.2f%% of compressed table", float64(totalBytes)*100/(439*1024*1024))
}
