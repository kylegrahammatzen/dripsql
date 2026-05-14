package engine

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
	FirstNs int64 `json:"first_ns"`
	BestNs  int64 `json:"best_ns"`
	AvgNs   int64 `json:"avg_ns"`
	P95Ns   int64 `json:"p95_ns,omitempty"`
	Samples int   `json:"samples"`
}

func ReductionFromStats(stats storage.QueryStats) Reduction {
	return Reduction{
		Segments: NewCounter(stats.SegmentsTotal, stats.SegmentsCandidate),
		Pages:    NewCounter(stats.PagesTotal, stats.PagesCandidate),
		Rows:     RowReduction{Scanned: stats.RowsTotal, Candidate: stats.RowsCandidate, Matched: stats.RowsMatched},
	}
}

func NewCounter(scanned, candidate int64) Counter {
	pruned := max(scanned-candidate, 0)
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
		rows = append(rows, []any{"Bytes / match", "payload", fmt.Sprintf("%s (%s read, %d rows kept)", humanfmt.Bytes(r.BytesPerMatch.Value), humanfmt.Bytes(float64(r.BytesPerMatch.Bytes)), r.BytesPerMatch.Rows)})
	} else {
		rows = append(rows, []any{"Bytes / match", "payload", "-"})
	}
	rows = append(rows,
		[]any{"Reduction", "segments", fmt.Sprintf("%d scanned -> %d candidate (%.1f%% pruned)", r.Reduction.Segments.Scanned, r.Reduction.Segments.Candidate, r.Reduction.Segments.PrunedPct)},
		[]any{"Reduction", "pages", fmt.Sprintf("%d scanned -> %d candidate (%.1f%% pruned)", r.Reduction.Pages.Scanned, r.Reduction.Pages.Candidate, r.Reduction.Pages.PrunedPct)},
		[]any{"Reduction", "rows", fmt.Sprintf("%d scanned -> %d candidate -> %d matched", r.Reduction.Rows.Scanned, r.Reduction.Rows.Candidate, r.Reduction.Rows.Matched)},
	)
	for _, access := range r.Access {
		value := access.Strategy
		if access.Effect != "" {
			value += " (" + access.Effect + ")"
		}
		rows = append(rows, []any{"Access", access.Name, value})
	}
	rows = append(rows, []any{"Read", "payload", humanfmt.Bytes(float64(r.Read.PayloadBytes))})
	if r.Read.PredicatePayloadBytes > 0 {
		rows = append(rows, []any{"Read", "predicate payload", humanfmt.Bytes(float64(r.Read.PredicatePayloadBytes))})
	}
	if r.Read.AggregatePayloadBytes > 0 {
		rows = append(rows, []any{"Read", "aggregate payload", humanfmt.Bytes(float64(r.Read.AggregatePayloadBytes))})
	}
	if r.Timing.Samples > 0 {
		rows = append(rows,
			[]any{"Timing", "first", humanfmt.Duration(time.Duration(r.Timing.FirstNs))},
			[]any{"Timing", "best", humanfmt.Duration(time.Duration(r.Timing.BestNs))},
			[]any{"Timing", "avg", humanfmt.Duration(time.Duration(r.Timing.AvgNs))},
		)
		if r.Timing.P95Ns != 0 {
			rows = append(rows, []any{"Timing", "p95", humanfmt.Duration(time.Duration(r.Timing.P95Ns))})
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
		fprintf(w, "%sBytes / match:  %s per matched row (%s read for %s rows)\n", indent, humanfmt.Bytes(r.BytesPerMatch.Value), humanfmt.Bytes(float64(r.BytesPerMatch.Bytes)), formatCount(r.BytesPerMatch.Rows))
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
	fprintf(w, "%s  payload:            %s\n", indent, humanfmt.Bytes(float64(r.Read.PayloadBytes)))
	predicatePayload := "-"
	if r.Read.PredicatePayloadBytes > 0 {
		predicatePayload = humanfmt.Bytes(float64(r.Read.PredicatePayloadBytes))
	}
	aggregatePayload := "-"
	if r.Read.AggregatePayloadBytes > 0 {
		aggregatePayload = humanfmt.Bytes(float64(r.Read.AggregatePayloadBytes))
	}
	fprintf(w, "%s  predicate payload:  %s\n", indent, predicatePayload)
	fprintf(w, "%s  aggregate payload:  %s\n", indent, aggregatePayload)
	if r.Timing.Samples > 0 {
		fprintf(w, "%sTiming:\n", indent)
		fprintf(w, "%s  first:    %s\n", indent, humanfmt.Duration(time.Duration(r.Timing.FirstNs)))
		fprintf(w, "%s  best:     %s\n", indent, humanfmt.Duration(time.Duration(r.Timing.BestNs)))
		fprintf(w, "%s  avg:      %s\n", indent, humanfmt.Duration(time.Duration(r.Timing.AvgNs)))
		if r.Timing.P95Ns != 0 {
			fprintf(w, "%s  p95:      %s\n", indent, humanfmt.Duration(time.Duration(r.Timing.P95Ns)))
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

func readLine(c Counter) string {
	pruned := c.Pruned
	if pruned <= 0 {
		return fmt.Sprintf("%s/%s read, none pruned", formatCount(c.Candidate), formatCount(c.Scanned))
	}
	return fmt.Sprintf("%s/%s read, %s pruned (%.1f%%)", formatCount(c.Candidate), formatCount(c.Scanned), formatCount(pruned), c.PrunedPct)
}

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
