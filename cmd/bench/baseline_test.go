package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/explain"
)

func TestWriteDiffPrintsRegressionsOnly(t *testing.T) {
	baseline := benchReport{
		Profile: "structured",
		Rows:    1000,
		Env:     envInfo{Commit: "abc123de"},
		Queries: []*explain.QueryReport{
			{Name: "fast", Timing: &explain.Timing{AvgMs: 10}},
			{Name: "regress", Timing: &explain.Timing{AvgMs: 100}},
			{Name: "stable", Timing: &explain.Timing{AvgMs: 50}},
		},
	}
	current := benchReport{
		Profile: "structured",
		Rows:    1000,
		Queries: []*explain.QueryReport{
			{Name: "fast", Timing: &explain.Timing{AvgMs: 10.4}},
			{Name: "regress", Timing: &explain.Timing{AvgMs: 130}},
			{Name: "stable", Timing: &explain.Timing{AvgMs: 51}},
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
	baseline := benchReport{
		Profile: "structured", Rows: 1000,
		Queries: []*explain.QueryReport{
			{Name: "stable", Timing: &explain.Timing{AvgMs: 50}},
		},
	}
	current := benchReport{
		Profile: "structured", Rows: 1000,
		Queries: []*explain.QueryReport{
			{Name: "stable", Timing: &explain.Timing{AvgMs: 51}},
		},
	}
	var buf bytes.Buffer
	if err := writeDiff(&buf, baseline, current, true); err != nil {
		t.Fatalf("writeDiff: %v", err)
	}
	if !strings.Contains(buf.String(), "stable: avg 50.0ms -> 51.0ms") {
		t.Errorf("show-all should include stable query, got:\n%s", buf.String())
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
		t.Errorf("0,5 = %f, want 100 (signal new measurement)", got)
	}
}
