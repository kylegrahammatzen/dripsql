package explain

import (
	"fmt"
	"io"
	"strconv"
	"strings"
)

// RenderOptions controls optional formatting choices for text rendering.
// Empty values use sensible defaults; pass &RenderOptions{} for the default
// shape used by EXPLAIN and the bench driver's per-query block.
type RenderOptions struct {
	// IncludeSQL prints the SQL block under the query name when set.
	IncludeSQL bool
}

// Render writes the human-readable per-query report to w. Sections that the
// QueryReport leaves nil/empty are skipped, which is how EXPLAIN (without
// ANALYZE) hides Reduction/Read/Timing.
func Render(w io.Writer, r *QueryReport, opts RenderOptions) error {
	if r == nil {
		return nil
	}
	bw := newWriter(w)
	if r.Name != "" {
		bw.line(r.Name)
	}
	if opts.IncludeSQL && r.SQL != "" {
		writeSQL(bw, r.SQL)
		bw.blank()
	}
	if r.Plan != nil {
		writePlan(bw, r.Plan, "  ")
	}
	writeQueryBody(bw, r)
	if r.Timing != nil {
		bw.blank()
		writeTiming(bw, r.Timing)
	}
	return bw.err
}

func writeSQL(w *writer, sql string) {
	for line := range strings.SplitSeq(strings.TrimRight(sql, "\n"), "\n") {
		w.line("  " + line)
	}
}

func writePlan(w *writer, node *PlanNode, indent string) {
	w.line(indent + planLabel(node))
	for _, child := range node.Children {
		writePlan(w, child, indent+"└── ")
	}
	// once we descend past a "└── ", subsequent levels use "    " as a prefix.
	// Today every plan has at most one child per node, so the renderer above
	// handles depth-2 cases (Count -> ReadSegments) correctly. Deeper trees
	// would need tree-state tracking.
}

func planLabel(node *PlanNode) string {
	if node == nil {
		return ""
	}
	if node.Table != "" && !strings.Contains(node.Op, "(") {
		return node.Op + "(" + node.Table + ")"
	}
	return node.Op
}

func writeQueryBody(w *writer, r *QueryReport) {
	body := []sectionLine{}
	if len(r.Predicate) > 0 {
		body = append(body, sectionLine{label: "Predicate:", values: r.Predicate})
	}
	if len(r.Access) > 0 {
		entries := make([]string, len(r.Access))
		labelWidth := 0
		for _, e := range r.Access {
			if len(e.Column) > labelWidth {
				labelWidth = len(e.Column)
			}
		}
		for i, e := range r.Access {
			entries[i] = padRight(e.Column+":", labelWidth+1) + " " + e.Decision
		}
		body = append(body, sectionLine{label: "Access:", values: entries})
	}
	if r.Reduction != nil {
		body = append(body, sectionLine{label: "Reduction:", values: reductionLines(r.Reduction)})
	}
	if len(r.Read) > 0 {
		body = append(body, sectionLine{label: "Read:", values: readLines(r.Read)})
	}
	if len(r.NotUsed) > 0 {
		entries := make([]string, len(r.NotUsed))
		for i, n := range r.NotUsed {
			entries[i] = n.Name + ": " + n.Reason
		}
		body = append(body, sectionLine{label: "Not used:", values: entries})
	}
	if r.Why != "" {
		header := "Why:"
		if r.Timing != nil && r.Timing.Samples > 0 {
			// keep the canonical "Why" header; the slow/fast variants are
			// future work once a heuristic exists.
		}
		body = append(body, sectionLine{label: header, values: wrap(r.Why, 78)})
	}

	first := true
	for _, sec := range body {
		if !first {
			w.blank()
		}
		first = false
		w.line("      " + sec.label)
		for _, v := range sec.values {
			w.line("        " + v)
		}
	}
}

type sectionLine struct {
	label  string
	values []string
}

func reductionLines(r *Reduction) []string {
	const labelWidth = 9
	out := make([]string, 0, 3)
	if r.SegmentsTotal > 0 || r.SegmentsCandidate > 0 {
		out = append(out, padRight("segments:", labelWidth)+" "+
			commas(r.SegmentsTotal)+" scanned -> "+commas(r.SegmentsCandidate)+" candidate")
	}
	if r.PagesTotal > 0 || r.PagesCandidate > 0 {
		out = append(out, padRight("pages:", labelWidth)+" "+
			commas(r.PagesTotal)+" scanned -> "+commas(r.PagesCandidate)+" candidate")
	}
	if r.RowsTotal > 0 || r.RowsCandidate > 0 {
		row := padRight("rows:", labelWidth) + " " +
			commas(r.RowsTotal) + " scanned -> " +
			commas(r.RowsCandidate) + " candidate"
		if r.RowsMatched > 0 {
			row += " -> " + commas(r.RowsMatched) + " matched"
		}
		out = append(out, row)
	} else if r.RowsMatched > 0 {
		out = append(out, padRight("matched:", labelWidth)+" "+commas(r.RowsMatched)+" rows")
	}
	return out
}

func readLines(entries []ReadEntry) []string {
	width := 0
	for _, e := range entries {
		if len(e.Purpose)+1 > width {
			width = len(e.Purpose) + 1
		}
	}
	out := make([]string, len(entries))
	for i, e := range entries {
		out[i] = padRight(e.Purpose+":", width) + " " + humanBytes(e.Bytes)
	}
	return out
}

func writeTiming(w *writer, t *Timing) {
	w.line("  Timing:")
	if t.Samples > 0 {
		const labelWidth = 8
		w.line("    " + padRight("first:", labelWidth) + " " + formatMs(t.FirstMs))
		w.line("    " + padRight("best:", labelWidth) + " " + formatMs(t.BestMs))
		w.line("    " + padRight("avg:", labelWidth) + " " + formatMs(t.AvgMs))
		w.line("    " + padRight("samples:", labelWidth) + " " + strconv.Itoa(t.Samples))
		return
	}
	w.line("    total: " + formatMs(t.TotalMs))
}

func formatMs(ms float64) string {
	if ms >= 1000 {
		return strconv.FormatFloat(ms/1000, 'f', 2, 64) + "s"
	}
	if ms >= 1 {
		return strconv.FormatFloat(ms, 'f', 1, 64) + "ms"
	}
	if ms >= 0.001 {
		return strconv.FormatFloat(ms*1000, 'f', 1, 64) + "us"
	}
	return strconv.FormatFloat(ms*1_000_000, 'f', 0, 64) + "ns"
}

func humanBytes(b uint64) string {
	const (
		kib = 1024
		mib = 1024 * kib
		gib = 1024 * mib
	)
	switch {
	case b >= gib:
		return strconv.FormatFloat(float64(b)/gib, 'f', 2, 64) + " GiB"
	case b >= mib:
		return strconv.FormatFloat(float64(b)/mib, 'f', 2, 64) + " MiB"
	case b >= kib:
		return strconv.FormatFloat(float64(b)/kib, 'f', 2, 64) + " KiB"
	}
	return strconv.FormatUint(b, 10) + " B"
}

func commas(n uint64) string {
	s := strconv.FormatUint(n, 10)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	pre := len(s) % 3
	if pre > 0 {
		b.WriteString(s[:pre])
		if len(s) > pre {
			b.WriteByte(',')
		}
	}
	for i := pre; i < len(s); i += 3 {
		if i > pre {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}

func wrap(text string, width int) []string {
	if text == "" {
		return nil
	}
	words := strings.Fields(text)
	var out []string
	var line strings.Builder
	for _, w := range words {
		if line.Len() == 0 {
			line.WriteString(w)
			continue
		}
		if line.Len()+1+len(w) > width {
			out = append(out, line.String())
			line.Reset()
			line.WriteString(w)
			continue
		}
		line.WriteByte(' ')
		line.WriteString(w)
	}
	if line.Len() > 0 {
		out = append(out, line.String())
	}
	return out
}

type writer struct {
	w   io.Writer
	err error
}

func newWriter(w io.Writer) *writer {
	return &writer{w: w}
}

func (w *writer) line(s string) {
	if w.err != nil {
		return
	}
	_, w.err = fmt.Fprintln(w.w, s)
}

func (w *writer) blank() {
	w.line("")
}
