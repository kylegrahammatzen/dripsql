package explain

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	humanfmt "github.com/kylegrahammatzen/dripsql/internal/format"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

type Report struct {
	Plan          string        `json:"plan"`
	Output        Output        `json:"output"`
	Selectivity   Selectivity   `json:"selectivity"`
	BytesPerMatch BytesPerMatch `json:"bytes_per_match"`
	Reduction     Reduction     `json:"reduction"`
	Access        []Access      `json:"access"`
	Read          Read          `json:"read"`
	Timing        Timing        `json:"timing"`
}

type Output struct {
	Batches int64 `json:"batches"`
	Rows    int64 `json:"rows"`
	Kept    int64 `json:"kept"`
}

type Selectivity struct {
	Matched int64   `json:"matched"`
	Total   int64   `json:"total"`
	Percent float64 `json:"percent"`
	Valid   bool    `json:"valid"`
}

type BytesPerMatch struct {
	Bytes int64   `json:"bytes"`
	Rows  int64   `json:"rows"`
	Value float64 `json:"value"`
	Valid bool    `json:"valid"`
}

type Reduction struct {
	Segments Counter      `json:"segments"`
	Pages    Counter      `json:"pages"`
	Rows     RowReduction `json:"rows"`
}

type Counter struct {
	Scanned   int64   `json:"scanned"`
	Candidate int64   `json:"candidate"`
	Pruned    int64   `json:"pruned"`
	PrunedPct float64 `json:"pruned_percent"`
}

type RowReduction struct {
	Scanned   int64 `json:"scanned"`
	Candidate int64 `json:"candidate"`
	Matched   int64 `json:"matched"`
}

type Access struct {
	Name     string `json:"name"`
	Strategy string `json:"strategy"`
	Effect   string `json:"effect"`
}

type Read struct {
	PayloadBytes          int64 `json:"payload_bytes"`
	PredicatePayloadBytes int64 `json:"predicate_payload_bytes,omitempty"`
	AggregatePayloadBytes int64 `json:"aggregate_payload_bytes,omitempty"`
}

type Timing struct {
	FirstMs float64 `json:"first_ms"`
	BestMs  float64 `json:"best_ms"`
	AvgMs   float64 `json:"avg_ms"`
	P95Ms   float64 `json:"p95_ms,omitempty"`
	FirstNs int64   `json:"first_ns,omitempty"`
	BestNs  int64   `json:"best_ns,omitempty"`
	AvgNs   int64   `json:"avg_ns,omitempty"`
	P95Ns   int64   `json:"p95_ns,omitempty"`
	Samples int     `json:"samples"`
}

func ReductionFromStats(stats storage.ExecStats) Reduction {
	return Reduction{
		Segments: NewCounter(stats.SegmentsTotal, stats.SegmentsCandidate),
		Pages:    NewCounter(stats.PagesTotal, stats.PagesCandidate),
		Rows:     RowReduction{Scanned: stats.RowsTotal, Candidate: stats.RowsCandidate, Matched: stats.RowsMatched},
	}
}

func NewCounter(scanned, candidate int64) Counter {
	pruned := scanned - candidate
	if pruned < 0 {
		pruned = 0
	}
	counter := Counter{Scanned: scanned, Candidate: candidate, Pruned: pruned}
	if scanned > 0 {
		counter.PrunedPct = float64(pruned) * 100 / float64(scanned)
	}
	return counter
}

func (r *Report) Finalize() {
	r.Reduction.Segments = NewCounter(r.Reduction.Segments.Scanned, r.Reduction.Segments.Candidate)
	r.Reduction.Pages = NewCounter(r.Reduction.Pages.Scanned, r.Reduction.Pages.Candidate)
	if r.Reduction.Rows.Scanned > 0 {
		r.Selectivity = Selectivity{
			Matched: r.Reduction.Rows.Matched,
			Total:   r.Reduction.Rows.Scanned,
			Percent: float64(r.Reduction.Rows.Matched) * 100 / float64(r.Reduction.Rows.Scanned),
			Valid:   true,
		}
	}
	if r.Reduction.Rows.Matched > 0 {
		r.BytesPerMatch = BytesPerMatch{
			Bytes: r.Read.PayloadBytes,
			Rows:  r.Reduction.Rows.Matched,
			Value: float64(r.Read.PayloadBytes) / float64(r.Reduction.Rows.Matched),
			Valid: true,
		}
	}
}

func (r Report) Table() ([]string, [][]any) {
	rows := [][]any{{"Plan", "plan", r.Plan}}
	rows = append(rows, []any{"Output", "rows", r.Output.Kept})
	if r.Selectivity.Valid {
		rows = append(rows, []any{"Selectivity", "rows", fmt.Sprintf("%.3f%% (%d / %d)", r.Selectivity.Percent, r.Selectivity.Matched, r.Selectivity.Total)})
	} else {
		rows = append(rows, []any{"Selectivity", "rows", "-"})
	}
	if r.BytesPerMatch.Valid {
		rows = append(rows, []any{"Bytes / match", "payload", fmt.Sprintf("%s (%s read, %d rows kept)", formatBytesFloat(r.BytesPerMatch.Value), formatBytes(r.BytesPerMatch.Bytes), r.BytesPerMatch.Rows)})
	} else {
		rows = append(rows, []any{"Bytes / match", "payload", "-"})
	}
	rows = append(rows,
		[]any{"Reduction", "segments", counterString(r.Reduction.Segments)},
		[]any{"Reduction", "pages", counterString(r.Reduction.Pages)},
		[]any{"Reduction", "rows", fmt.Sprintf("%d scanned -> %d candidate -> %d matched", r.Reduction.Rows.Scanned, r.Reduction.Rows.Candidate, r.Reduction.Rows.Matched)},
	)
	for _, access := range r.Access {
		value := access.Strategy
		if access.Effect != "" {
			value += " (" + access.Effect + ")"
		}
		rows = append(rows, []any{"Access", access.Name, value})
	}
	rows = append(rows, []any{"Read", "payload", formatBytes(r.Read.PayloadBytes)})
	if r.Read.PredicatePayloadBytes > 0 {
		rows = append(rows, []any{"Read", "predicate payload", formatBytes(r.Read.PredicatePayloadBytes)})
	}
	if r.Read.AggregatePayloadBytes > 0 {
		rows = append(rows, []any{"Read", "aggregate payload", formatBytes(r.Read.AggregatePayloadBytes)})
	}
	if r.Timing.Samples > 0 {
		rows = append(rows,
			[]any{"Timing", "first", r.Timing.FormatFirst()},
			[]any{"Timing", "best", r.Timing.FormatBest()},
			[]any{"Timing", "avg", r.Timing.FormatAvg()},
		)
		if r.Timing.HasP95() {
			rows = append(rows, []any{"Timing", "p95", r.Timing.FormatP95()})
		}
		rows = append(rows, []any{"Timing", "samples", r.Timing.Samples})
	}
	return []string{"section", "metric", "value"}, rows
}

func RenderText(w io.Writer, r Report, indent string) {
	fprintf(w, "%sPlan\n", indent)
	for i, line := range planTreeLines(r.Plan) {
		fprintf(w, "%s  %s%s\n", indent, strings.Repeat("  ", i), line)
	}

	fprintf(w, "%sOutput rows:    %d\n", indent, r.Output.Kept)
	if r.Selectivity.Valid {
		fprintf(w, "%sSelectivity:    %s of %s rows matched (%.3f%%)\n", indent, formatCount(r.Selectivity.Matched), formatCount(r.Selectivity.Total), r.Selectivity.Percent)
	} else {
		fprintf(w, "%sSelectivity:    -\n", indent)
	}
	if r.BytesPerMatch.Valid {
		fprintf(w, "%sBytes / match:  %s per matched row (%s read for %s rows)\n", indent, formatBytesFloat(r.BytesPerMatch.Value), formatBytes(r.BytesPerMatch.Bytes), formatCount(r.BytesPerMatch.Rows))
	} else {
		fprintf(w, "%sBytes / match:  -\n", indent)
	}

	fprintf(w, "%sReduction:\n", indent)
	fprintf(w, "%s  segments:  %s\n", indent, readLine(r.Reduction.Segments))
	fprintf(w, "%s  pages:     %s\n", indent, readLine(r.Reduction.Pages))
	fprintf(w, "%s  rows:      %s in → %s matched\n", indent, formatCount(r.Reduction.Rows.Scanned), formatCount(r.Reduction.Rows.Matched))

	fprintf(w, "%sStrategy:\n", indent)
	if len(r.Access) == 0 {
		fprintf(w, "%s  -\n", indent)
	} else {
		for _, access := range r.Access {
			effect := ""
			if access.Effect != "" {
				effect = " (" + access.Effect + ")"
			}
			fprintf(w, "%s  %s: %s%s\n", indent, access.Name, access.Strategy, effect)
		}
	}

	fprintf(w, "%sRead:\n", indent)
	fprintf(w, "%s  payload:            %s\n", indent, formatBytes(r.Read.PayloadBytes))
	fprintf(w, "%s  predicate payload:  %s\n", indent, formatOptionalBytes(r.Read.PredicatePayloadBytes))
	fprintf(w, "%s  aggregate payload:  %s\n", indent, formatOptionalBytes(r.Read.AggregatePayloadBytes))
	if r.Timing.Samples > 0 {
		fprintf(w, "%sTiming:\n", indent)
		fprintf(w, "%s  first:    %s\n", indent, r.Timing.FormatFirst())
		fprintf(w, "%s  best:     %s\n", indent, r.Timing.FormatBest())
		fprintf(w, "%s  avg:      %s\n", indent, r.Timing.FormatAvg())
		if r.Timing.HasP95() {
			fprintf(w, "%s  p95:      %s\n", indent, r.Timing.FormatP95())
		}
		fprintf(w, "%s  samples:  %d\n", indent, r.Timing.Samples)
	}
}

// planTreeLines parses a "A(args) -> B(args) -> C(args)" plan string into
// indented lines so the pipeline reads top-to-bottom.
func planTreeLines(plan string) []string {
	if plan == "" {
		return []string{"-"}
	}
	parts := strings.Split(plan, " -> ")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, formatPlanNode(p))
	}
	return out
}

func formatPlanNode(node string) string {
	open := strings.IndexByte(node, '(')
	if open < 0 || !strings.HasSuffix(node, ")") {
		return node
	}
	name := node[:open]
	args := node[open+1 : len(node)-1]
	if args == "" {
		return name
	}
	return name + "  " + args
}

// readLine renders a Counter as "X/Y read, Z pruned" — tight and readable.
func readLine(c Counter) string {
	pruned := c.Pruned
	if pruned <= 0 {
		return fmt.Sprintf("%s/%s read, none pruned", formatCount(c.Candidate), formatCount(c.Scanned))
	}
	return fmt.Sprintf("%s/%s read, %s pruned (%.1f%%)", formatCount(c.Candidate), formatCount(c.Scanned), formatCount(pruned), c.PrunedPct)
}

// formatCount adds thousands separators to make 6-digit row counts readable
// at a glance.
func formatCount(n int64) string {
	negative := n < 0
	if negative {
		n = -n
	}
	if n < 1000 {
		s := strconv.FormatInt(n, 10)
		if negative {
			return "-" + s
		}
		return s
	}
	digits := strconv.FormatInt(n, 10)
	out := make([]byte, 0, len(digits)+len(digits)/3)
	rem := len(digits) % 3
	if rem > 0 {
		out = append(out, digits[:rem]...)
	}
	for i := rem; i < len(digits); i += 3 {
		if len(out) > 0 {
			out = append(out, ',')
		}
		out = append(out, digits[i:i+3]...)
	}
	if negative {
		return "-" + string(out)
	}
	return string(out)
}

func fprintf(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format, args...)
}

func counterString(counter Counter) string {
	return fmt.Sprintf("%d scanned -> %d candidate (%.1f%% pruned)", counter.Scanned, counter.Candidate, counter.PrunedPct)
}

func formatOptionalBytes(n int64) string {
	if n <= 0 {
		return "-"
	}
	return formatBytes(n)
}

func formatBytes(n int64) string {
	if n < 0 {
		return "-" + formatBytes(-n)
	}
	const unit = 1024
	if n < unit {
		return strconv.FormatInt(n, 10) + " B"
	}
	value := float64(n)
	units := []string{"KiB", "MiB", "GiB", "TiB"}
	for i, suffix := range units {
		value /= unit
		if value < unit || i == len(units)-1 {
			return strconv.FormatFloat(value, 'f', 2, 64) + " " + suffix
		}
	}
	return strconv.FormatInt(n, 10) + " B"
}

func formatBytesFloat(n float64) string {
	if n < 0 {
		return "-" + formatBytesFloat(-n)
	}
	const unit = 1024
	if n < unit {
		if n == float64(int64(n)) {
			return strconv.FormatInt(int64(n), 10) + " B"
		}
		return strconv.FormatFloat(n, 'f', 2, 64) + " B"
	}
	value := n
	units := []string{"KiB", "MiB", "GiB", "TiB"}
	for i, suffix := range units {
		value /= unit
		if value < unit || i == len(units)-1 {
			return strconv.FormatFloat(value, 'f', 2, 64) + " " + suffix
		}
	}
	return strconv.FormatFloat(n, 'f', 2, 64) + " B"
}

func (t Timing) FormatFirst() string {
	return formatTiming(t.FirstNs, t.FirstMs)
}

func (t Timing) FormatBest() string {
	return formatTiming(t.BestNs, t.BestMs)
}

func (t Timing) FormatAvg() string {
	return formatTiming(t.AvgNs, t.AvgMs)
}

func (t Timing) FormatP95() string {
	return formatTiming(t.P95Ns, t.P95Ms)
}

func (t Timing) HasP95() bool {
	return t.P95Ns != 0 || t.P95Ms != 0
}

func formatTiming(ns int64, ms float64) string {
	if ns != 0 {
		return humanfmt.Duration(time.Duration(ns))
	}
	return humanfmt.Milliseconds(ms)
}
