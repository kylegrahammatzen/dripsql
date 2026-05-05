package table

const (
	targetSegments     = 256
	minSegmentRows     = 100_000
	maxSegmentRows     = 1_000_000
	segmentRowsQuantum = 10_000
)

// RecommendedSegmentRows returns a row-group size for a known-size append.
func RecommendedSegmentRows(totalRows int64) int {
	if totalRows <= 0 {
		return minSegmentRows
	}

	rows := ceilDiv(totalRows, targetSegments)
	rows = roundUp(rows, segmentRowsQuantum)
	rows = min(max(rows, minSegmentRows), maxSegmentRows)
	return int(rows)
}

// AverageSegmentRows returns the manifest's average rows per segment, rounded up.
func AverageSegmentRows(rows int64, segments int) int {
	if rows <= 0 || segments <= 0 {
		return 0
	}
	return int(ceilDiv(rows, int64(segments)))
}

func ceilDiv(n int64, d int64) int64 {
	if n <= 0 {
		return 0
	}
	return (n + d - 1) / d
}

func roundUp(n int64, step int64) int64 {
	if step <= 0 || n == 0 {
		return n
	}
	return ((n + step - 1) / step) * step
}
