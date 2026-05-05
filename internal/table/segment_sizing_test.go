package table

import "testing"

func TestRecommendedSegmentRows(t *testing.T) {
	tests := []struct {
		name string
		rows int64
		want int
	}{
		{name: "empty", rows: 0, want: 100_000},
		{name: "small", rows: 1_000_000, want: 100_000},
		{name: "large", rows: 100_000_000, want: 400_000},
		{name: "capped", rows: 1_000_000_000, want: 1_000_000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RecommendedSegmentRows(tt.rows); got != tt.want {
				t.Fatalf("RecommendedSegmentRows(%d) = %d, want %d", tt.rows, got, tt.want)
			}
		})
	}
}

func TestAverageSegmentRows(t *testing.T) {
	if got := AverageSegmentRows(1_000_001, 10); got != 100_001 {
		t.Fatalf("AverageSegmentRows = %d, want 100001", got)
	}
	if got := AverageSegmentRows(0, 10); got != 0 {
		t.Fatalf("AverageSegmentRows empty = %d, want 0", got)
	}
}
