// Package explain holds the report types shared by the SQL EXPLAIN surface
// and the cmd/bench driver. Storage layers do not import this package; they
// populate execution counters through internal/storage.ExecStats and the
// engine layer folds the result into a QueryReport.
package explain

// QueryReport is the per-query report consumed by the EXPLAIN renderer and
// the bench driver. Sections that are not populated render as nothing, which
// is how plain EXPLAIN (without ANALYZE) hides Reduction/Read/Timing.
type QueryReport struct {
	Name      string         `json:"query_name,omitempty"`
	SQL       string         `json:"sql,omitempty"`
	Strategy  string         `json:"strategy,omitempty"`
	Plan      *PlanNode      `json:"plan,omitempty"`
	Predicate []string       `json:"predicate,omitempty"`
	Access    []AccessEntry  `json:"access,omitempty"`
	Reduction *Reduction     `json:"reduction,omitempty"`
	Read      []ReadEntry    `json:"read,omitempty"`
	NotUsed   []NotUsedEntry `json:"not_used,omitempty"`
	Why       string         `json:"why,omitempty"`
	Timing    *Timing        `json:"timing,omitempty"`
}

// AccessEntry records the storage path chosen for one referenced column
// during query execution.
type AccessEntry struct {
	Column   string `json:"column"`
	Decision string `json:"decision"`
}

// Reduction records segment/page/row narrowing across pruning and filtering
// stages. Counts are inclusive of the buffered hot rows where applicable.
type Reduction struct {
	SegmentsTotal     uint64 `json:"segments_total"`
	SegmentsCandidate uint64 `json:"segments_candidate"`
	PagesTotal        uint64 `json:"pages_total"`
	PagesCandidate    uint64 `json:"pages_candidate"`
	RowsTotal         uint64 `json:"rows_total"`
	RowsCandidate     uint64 `json:"rows_candidate"`
	RowsMatched       uint64 `json:"rows_matched"`
}

// ReadEntry records bytes loaded for one logical purpose during execution.
// Common purposes: "predicate payload", "aggregate payload", "group payload",
// "metadata".
type ReadEntry struct {
	Purpose string `json:"purpose"`
	Bytes   uint64 `json:"bytes"`
}

// NotUsedEntry documents an optimization that was eligible but did not fire,
// with a short reason.
type NotUsedEntry struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

// Timing carries either benchmark sample timings (first/best/avg/samples) or
// EXPLAIN ANALYZE single-run timing (Total). Both shapes can coexist; the
// renderer prefers Total when Samples is zero.
type Timing struct {
	FirstMs float64 `json:"first_ms,omitempty"`
	BestMs  float64 `json:"best_ms,omitempty"`
	AvgMs   float64 `json:"avg_ms,omitempty"`
	Samples int     `json:"samples,omitempty"`
	TotalMs float64 `json:"total_ms,omitempty"`
}

// TableReport captures the table-level Storage section of the bench report.
type TableReport struct {
	Table              string         `json:"table"`
	Rows               uint64         `json:"rows"`
	Segments           int            `json:"segments"`
	TableBytes         int64          `json:"table_bytes"`
	ColumnPayloadBytes int64          `json:"column_payload_bytes"`
	StorageOverhead    int64          `json:"storage_overhead_bytes"`
	PlainEstimate      int64          `json:"plain_estimate_bytes"`
	TableCompression   float64        `json:"table_compression"`
	BytesPerRow        float64        `json:"bytes_per_row"`
	Columns            []ColumnReport `json:"columns"`
}

// ColumnReport is one row of the per-column Columns table. Encoding,
// Compression, and Metadata are human-readable summaries; the bench renderer
// uses them as-is.
type ColumnReport struct {
	Name        string  `json:"name"`
	Type        string  `json:"type"`
	Encoding    string  `json:"encoding"`
	Compression string  `json:"compression"`
	PlainBytes  int64   `json:"plain_bytes"`
	StoredBytes int64   `json:"stored_bytes"`
	Ratio       float64 `json:"ratio"`
	Metadata    string  `json:"metadata"`
}

// Findings is the cross-query summary printed at the bottom of a benchmark
// report. Regressions only appear when a baseline was supplied.
type Findings struct {
	Slowest         string   `json:"slowest,omitempty"`
	BestCompression string   `json:"best_compression,omitempty"`
	Regressions     []string `json:"regressions,omitempty"`
	Notes           []string `json:"notes,omitempty"`
}
