package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/explain"
)

func TestWriteDiffPrintsRegressionsOnly(t *testing.T) {
	baseline := benchReport{
		Profile: "structured",
		Rows:    1000,
		Env:     envInfo{Commit: "abc123de"},
		Queries: []queryReport{
			{Name: "fast", Timing: explain.Timing{AvgMs: 10}},
			{Name: "regress", Timing: explain.Timing{AvgMs: 100}},
			{Name: "stable", Timing: explain.Timing{AvgMs: 50}},
		},
	}
	current := benchReport{
		Profile: "structured",
		Rows:    1000,
		Queries: []queryReport{
			{Name: "fast", Timing: explain.Timing{AvgMs: 10.4}},
			{Name: "regress", Timing: explain.Timing{AvgMs: 130}},
			{Name: "stable", Timing: explain.Timing{AvgMs: 51}},
		},
	}
	var buf bytes.Buffer
	if err := writeDiff(&buf, baseline, current, false); err != nil {
		t.Fatalf("writeDiff: %v", err)
	}
	out := buf.String()

	if !strings.Contains(out, "abc123de") {
		t.Errorf("missing commit:\n%s", out)
	}
	if !strings.Contains(out, "regress: avg 100.0ms -> 130.0ms (+30.0%) regression") {
		t.Errorf("missing regress line:\n%s", out)
	}
	if strings.Contains(out, "stable") {
		t.Errorf("stable query should not render under threshold:\n%s", out)
	}
	if strings.Contains(out, "fast") {
		t.Errorf("fast query should not render under threshold:\n%s", out)
	}
}

func TestWriteDiffShowAllPrintsEveryQuery(t *testing.T) {
	baseline := benchReport{Profile: "structured", Rows: 1000, Queries: []queryReport{{Name: "stable", Timing: explain.Timing{AvgMs: 50}}}}
	current := benchReport{Profile: "structured", Rows: 1000, Queries: []queryReport{{Name: "stable", Timing: explain.Timing{AvgMs: 51}}}}

	var buf bytes.Buffer
	if err := writeDiff(&buf, baseline, current, true); err != nil {
		t.Fatalf("writeDiff: %v", err)
	}
	if !strings.Contains(buf.String(), "stable: avg 50.0ms -> 51.0ms") {
		t.Errorf("show-all should include stable query, got:\n%s", buf.String())
	}
}

func TestWriteComparisonPrintsCompactSummary(t *testing.T) {
	baseline := []benchReport{{
		Profile: "structured",
		Rows:    1000,
		Load:    loadStats{Elapsed: 10 * time.Millisecond},
		Queries: []queryReport{{Name: "regress", Timing: explain.Timing{AvgMs: 100}}},
	}}
	current := []benchReport{{
		Profile: "structured",
		Rows:    1000,
		Load:    loadStats{Elapsed: 20 * time.Millisecond},
		Queries: []queryReport{{Name: "regress", Timing: explain.Timing{AvgMs: 130}}},
	}}

	var buf bytes.Buffer
	if err := writeComparison(&buf, baseline, current, false, false); err != nil {
		t.Fatalf("writeComparison: %v", err)
	}
	out := buf.String()
	for _, want := range []string{"DripSQL v3 Benchmark Comparison", "Setup", "Queries", "structured", "regress", "regression"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in output:\n%s", want, out)
		}
	}
	if strings.Contains(out, "DripSQL v3 Benchmark\nDirectory:") {
		t.Fatalf("comparison should not include full detailed report:\n%s", out)
	}
}

func TestWriteDiffRejectsShapeMismatch(t *testing.T) {
	baseline := benchReport{Profile: "structured", Rows: 1000}
	current := benchReport{Profile: "structured", Rows: 5000}
	var buf bytes.Buffer
	err := writeDiff(&buf, baseline, current, false)
	if err == nil {
		t.Fatal("expected error for row mismatch")
	}
	if !strings.Contains(err.Error(), "rows") {
		t.Errorf("error = %q, want one mentioning rows", err.Error())
	}
}

func TestPercentDeltaHandlesZeroBaseline(t *testing.T) {
	if got := percentDelta(0, 0); got != 0 {
		t.Errorf("0,0 = %f, want 0", got)
	}
	if got := percentDelta(0, 5); got != 100 {
		t.Errorf("0,5 = %f, want 100", got)
	}
}

func TestLoadBaselinesReadsConcatenatedReports(t *testing.T) {
	path := filepath.Join(t.TempDir(), "baseline.json")
	data := []byte(`{"profile":"structured","rows":1000,"queries":[{"query_name":"a","timing":{"avg_ms":1}}]}
{"profile":"structured","rows":2000,"queries":[{"query_name":"a","timing":{"avg_ms":2}}]}
`)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write baseline: %v", err)
	}
	reports, err := loadBaselines(path, []string{"structured"})
	if err != nil {
		t.Fatalf("loadBaselines: %v", err)
	}
	if len(reports) != 2 {
		t.Fatalf("reports = %d, want 2", len(reports))
	}
	matched, ok := findBaseline(reports, "structured", 2000)
	if !ok || matched.Queries[0].Timing.AvgMs != 2 {
		t.Fatalf("matched = %#v ok=%v", matched, ok)
	}
}

func TestFindBaselineMatchesMode(t *testing.T) {
	reports := []benchReport{
		{Profile: "structured", Mode: benchModeSameProcess, Rows: 1000, Queries: []queryReport{{Name: "same"}}},
		{Profile: "structured", Mode: benchModeWarmReopen, Rows: 1000, Queries: []queryReport{{Name: "warm"}}},
	}
	matched, ok := findBaseline(reports, "structured", 1000, benchModeWarmReopen)
	if !ok || matched.Queries[0].Name != "warm" {
		t.Fatalf("matched = %#v ok=%v", matched, ok)
	}
	matched, ok = findBaseline([]benchReport{{Profile: "structured", Rows: 1000, Queries: []queryReport{{Name: "legacy"}}}}, "structured", 1000, benchModeSameProcess)
	if !ok || matched.Queries[0].Name != "legacy" {
		t.Fatalf("legacy matched = %#v ok=%v", matched, ok)
	}
}

func TestLoadBaselinesReadsProfileFilesFromDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "structured.json"), []byte(`{"profile":"structured","rows":1000,"queries":[{"query_name":"a","timing":{"avg_ms":1}}]}`), 0o644); err != nil {
		t.Fatalf("write structured baseline: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "random.json"), []byte(`{"profile":"random","rows":1000,"queries":[{"query_name":"a","timing":{"avg_ms":2}}]}`), 0o644); err != nil {
		t.Fatalf("write random baseline: %v", err)
	}

	reports, err := loadBaselines(dir, []string{"structured", "random"})
	if err != nil {
		t.Fatalf("loadBaselines dir: %v", err)
	}
	if len(reports) != 2 {
		t.Fatalf("reports = %d, want 2", len(reports))
	}
	matched, ok := findBaseline(reports, "random", 1000)
	if !ok || matched.Queries[0].Timing.AvgMs != 2 {
		t.Fatalf("matched = %#v ok=%v", matched, ok)
	}
}
