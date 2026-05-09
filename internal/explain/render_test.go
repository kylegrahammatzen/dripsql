package explain

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestRender_Count_FullReport(t *testing.T) {
	r := &QueryReport{
		Name: "event checkout for tenant",
		Plan: &PlanNode{
			Op:       "Count",
			Children: []*PlanNode{{Op: "ReadSegments", Table: "events"}},
		},
		Predicate: []string{"tenant_id = 42", "event_type = 'checkout'"},
		Access: []AccessEntry{
			{Column: "tenant_id", Decision: "min/max prune"},
			{Column: "event_type", Decision: "text summary prune"},
			{Column: "count(*)", Decision: "raw count loop"},
		},
		Reduction: &Reduction{
			SegmentsTotal: 100, SegmentsCandidate: 7,
			PagesTotal: 25000, PagesCandidate: 140,
			RowsTotal: 100_000_000, RowsCandidate: 560_000, RowsMatched: 18_420,
		},
		Read: []ReadEntry{
			{Purpose: "predicate payload", Bytes: 6_800_000},
			{Purpose: "metadata", Bytes: 1_100_000},
		},
		NotUsed: []NotUsedEntry{
			{Name: "metadata count", Reason: "no exact count for combined predicate"},
		},
		Why: "Both predicate columns pruned at segment+page level via metadata; only 0.56% of candidate rows had to be loaded.",
		Timing: &Timing{
			FirstMs: 9.1, BestMs: 8.2, AvgMs: 8.4, Samples: 7,
		},
	}

	out := mustRender(t, r, RenderOptions{})

	assertContains(t, out, "event checkout for tenant")
	assertContains(t, out, "Count")
	assertContains(t, out, "└── ReadSegments(events)")
	assertContains(t, out, "Predicate:")
	assertContains(t, out, "tenant_id = 42")
	assertContains(t, out, "event_type = 'checkout'")
	assertContains(t, out, "Access:")
	assertContains(t, out, "tenant_id:  min/max prune")
	assertContains(t, out, "event_type: text summary prune")
	assertContains(t, out, "Reduction:")
	assertContains(t, out, "segments: 100 scanned -> 7 candidate")
	assertContains(t, out, "pages:    25,000 scanned -> 140 candidate")
	assertContains(t, out, "rows:     100,000,000 scanned -> 560,000 candidate -> 18,420 matched")
	assertContains(t, out, "Read:")
	assertContains(t, out, "predicate payload: 6.48 MiB")
	assertContains(t, out, "metadata:          1.05 MiB")
	assertContains(t, out, "Not used:")
	assertContains(t, out, "metadata count: no exact count for combined predicate")
	assertContains(t, out, "Why:")
	assertContains(t, out, "Timing:")
	assertContains(t, out, "first:   9.1ms")
	assertContains(t, out, "best:    8.2ms")
	assertContains(t, out, "avg:     8.4ms")
	assertContains(t, out, "samples: 7")
}

func TestRender_Explain_NoAnalyzeOmitsExecutionSections(t *testing.T) {
	r := &QueryReport{
		Plan: &PlanNode{
			Op:       "Count",
			Children: []*PlanNode{{Op: "ReadSegments", Table: "events"}},
		},
		Predicate: []string{"tenant_id = 42", "event_type = 'checkout'"},
		Access: []AccessEntry{
			{Column: "tenant_id", Decision: "min/max prune eligible"},
			{Column: "event_type", Decision: "text summary prune eligible"},
			{Column: "count(*)", Decision: "raw count loop eligible"},
		},
		NotUsed: []NotUsedEntry{
			{Name: "metadata count", Reason: "no exact count for combined predicate"},
		},
	}

	out := mustRender(t, r, RenderOptions{})

	assertContains(t, out, "Predicate:")
	assertContains(t, out, "Access:")
	assertContains(t, out, "Not used:")

	for _, banned := range []string{"Reduction:", "Read:", "Timing:"} {
		if strings.Contains(out, banned) {
			t.Errorf("EXPLAIN (no analyze) output should not contain %q\n%s", banned, out)
		}
	}
}

func TestRender_ExplainAnalyze_TimingTotal(t *testing.T) {
	r := &QueryReport{
		Plan: &PlanNode{
			Op:       "Count",
			Children: []*PlanNode{{Op: "ReadSegments", Table: "events"}},
		},
		Reduction: &Reduction{
			SegmentsTotal: 100, SegmentsCandidate: 7,
			PagesTotal: 25000, PagesCandidate: 140,
			RowsTotal: 100_000_000, RowsCandidate: 560_000, RowsMatched: 18_420,
		},
		Read: []ReadEntry{
			{Purpose: "predicate payload", Bytes: 6_800_000},
			{Purpose: "metadata", Bytes: 1_100_000},
		},
		Timing: &Timing{TotalMs: 8.4},
	}

	out := mustRender(t, r, RenderOptions{})
	assertContains(t, out, "Timing:")
	assertContains(t, out, "total: 8.4ms")
}

func TestRender_GroupedCountIncludesGroupHeader(t *testing.T) {
	r := &QueryReport{
		Plan: &PlanNode{
			Op:       "Group(country), Count",
			Children: []*PlanNode{{Op: "ReadSegments", Table: "events"}},
		},
		Predicate: []string{"event_type = 'checkout'"},
	}
	out := mustRender(t, r, RenderOptions{})
	assertContains(t, out, "Group(country), Count")
	assertContains(t, out, "└── ReadSegments(events)")
}

func TestRender_NilReportIsNoOp(t *testing.T) {
	var buf bytes.Buffer
	if err := Render(&buf, nil, RenderOptions{}); err != nil {
		t.Fatalf("render nil: %v", err)
	}
	if buf.Len() != 0 {
		t.Errorf("nil report produced output: %q", buf.String())
	}
}

func TestRender_IncludesSQLOnlyWhenRequested(t *testing.T) {
	r := &QueryReport{
		Name: "q",
		SQL:  "SELECT count(*) FROM events;",
		Plan: &PlanNode{Op: "Count", Children: []*PlanNode{{Op: "ReadSegments", Table: "events"}}},
	}
	off := mustRender(t, r, RenderOptions{IncludeSQL: false})
	if strings.Contains(off, "SELECT count(*) FROM events;") {
		t.Errorf("SQL leaked when IncludeSQL=false:\n%s", off)
	}
	on := mustRender(t, r, RenderOptions{IncludeSQL: true})
	assertContains(t, on, "SELECT count(*) FROM events;")
}

func TestHumanBytes(t *testing.T) {
	cases := []struct {
		in   uint64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{2048, "2.00 KiB"},
		{1_500_000, "1.43 MiB"},
		{2_000_000_000, "1.86 GiB"},
	}
	for _, tc := range cases {
		got := humanBytes(tc.in)
		if got != tc.want {
			t.Errorf("humanBytes(%d) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestCommas(t *testing.T) {
	cases := map[uint64]string{
		0:           "0",
		42:          "42",
		1000:        "1,000",
		1_234_567:   "1,234,567",
		100_000_000: "100,000,000",
	}
	for in, want := range cases {
		if got := commas(in); got != want {
			t.Errorf("commas(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestRenderJSON_RoundTrip(t *testing.T) {
	r := &QueryReport{
		Name: "q",
		Plan: &PlanNode{Op: "Count", Children: []*PlanNode{{Op: "ReadSegments", Table: "events"}}},
		Reduction: &Reduction{
			SegmentsTotal:     100,
			SegmentsCandidate: 7,
			PagesTotal:        25000,
			PagesCandidate:    140,
		},
		Read: []ReadEntry{
			{Purpose: "predicate payload", Bytes: 6_800_000},
		},
	}
	var buf bytes.Buffer
	if err := RenderJSON(&buf, r); err != nil {
		t.Fatalf("RenderJSON: %v", err)
	}
	var got QueryReport
	if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, buf.String())
	}
	if got.Name != r.Name {
		t.Errorf("name = %q, want %q", got.Name, r.Name)
	}
	if got.Plan == nil || got.Plan.Op != "Count" {
		t.Errorf("plan = %+v, want Count", got.Plan)
	}
	if got.Reduction == nil || got.Reduction.SegmentsTotal != 100 {
		t.Errorf("reduction = %+v", got.Reduction)
	}
	if len(got.Read) != 1 || got.Read[0].Bytes != 6_800_000 {
		t.Errorf("read = %+v", got.Read)
	}
}

func mustRender(t *testing.T, r *QueryReport, opts RenderOptions) string {
	t.Helper()
	var buf bytes.Buffer
	if err := Render(&buf, r, opts); err != nil {
		t.Fatalf("Render: %v", err)
	}
	return buf.String()
}

func assertContains(t *testing.T, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Errorf("missing %q in:\n%s", needle, haystack)
	}
}
