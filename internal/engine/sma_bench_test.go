package engine

import (
	"testing"
)

// TestSMA_AggregateProjectionCeiling answers "what would the country aggregate
// query cost if the writer pre-computed per-page (dict_id, count, sum)
// projections at seal time?"
//
// The bench mimics ClickHouse aggregate projections / Sybase IQ fast
// projection / Moerkotte 1998 SMA: for each (segment, page) pair, store
// per-dict-id count + sum tuples. Reading the query becomes a merge over
// metadata, never touching payload bytes.
//
// Shape matches the structured-profile country aggregate at 10M rows:
// 5 segments × 4883 pages × 8 country dict-ids.

const (
	smaSegments      = 5
	smaPagesPer      = 4883
	smaCountries     = 8
	smaSegmentRows   = 2_097_152
	smaPageRows      = smaSegmentRows / smaPagesPer
	smaTotalPages    = smaSegments * smaPagesPer
)

// pageSMA is the per-page projection: for each dict-id, the count and sum.
// 8 ids × 16 bytes = 128 bytes per page. 4883 × 5 × 128 = 3.05 MiB total.
type pageSMA struct {
	counts [smaCountries]int64
	sums   [smaCountries]int64
}

func buildSMA() [][]pageSMA {
	all := make([][]pageSMA, smaSegments)
	for s := range all {
		pages := make([]pageSMA, smaPagesPer)
		for p := range pages {
			rowsPer := int64(smaPageRows / smaCountries)
			for c := 0; c < smaCountries; c++ {
				pages[p].counts[c] = rowsPer
				pages[p].sums[c] = rowsPer * int64(c+1) * 100
			}
		}
		all[s] = pages
	}
	return all
}

func BenchmarkSMACountryAggregateMergeOnly(b *testing.B) {
	sma := buildSMA()
	dictKeys := []string{"US", "CA", "GB", "DE", "FR", "JP", "BR", "AU"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var counts [smaCountries]int64
		var sums [smaCountries]int64
		for _, segment := range sma {
			for p := range segment {
				page := &segment[p]
				for c := 0; c < smaCountries; c++ {
					counts[c] += page.counts[c]
					sums[c] += page.sums[c]
				}
			}
		}
		_ = dictKeys
		_ = counts
		_ = sums
	}
}

// TestSMA_FootprintSize documents the metadata cost so the design
// trade is explicit when we revisit this.
func TestSMA_FootprintSize(t *testing.T) {
	const bytesPerPage = smaCountries * 16
	totalBytes := bytesPerPage * smaTotalPages
	t.Logf("SMA footprint for country agg @ 10M rows:")
	t.Logf("  pages:           %d (5 segments x %d)", smaTotalPages, smaPagesPer)
	t.Logf("  bytes per page:  %d (8 ids x 16B)", bytesPerPage)
	t.Logf("  total metadata:  %d bytes (%.2f MiB)", totalBytes, float64(totalBytes)/(1024*1024))
	t.Logf("  base table:      %d MiB compressed", 439)
	t.Logf("  overhead:        %.2f%% of compressed table", float64(totalBytes)*100/(439*1024*1024))
}
