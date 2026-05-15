// Bench smoke tests: percentile arithmetic, the catalog stays self-consistent, summary numbers are sane.
// No subprocess. The main() exit-on-error contract is exercised by hand via go run.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/engine"
)

func mustWriteReport(t *testing.T, path string, r runReport) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := json.NewEncoder(f).Encode(r); err != nil {
		t.Fatal(err)
	}
}

func TestPercentile_Interpolation(t *testing.T) {
	sorted := []float64{1, 2, 3, 4, 5}
	cases := []struct {
		p, want float64
	}{
		{0, 1},
		{0.5, 3},
		{0.95, 4.8},
		{1, 5},
	}
	for _, c := range cases {
		got := percentile(sorted, c.p)
		if math.Abs(got-c.want) > 1e-9 {
			t.Errorf("percentile(%g) = %g, want %g", c.p, got, c.want)
		}
	}
}

func TestPercentile_Empty(t *testing.T) {
	if got := percentile(nil, 0.5); got != 0 {
		t.Fatalf("empty percentile = %v, want 0", got)
	}
}

func TestCatalog_QueriesPointAtKnownDatasets(t *testing.T) {
	for name, q := range queries {
		if _, ok := datasets[q.dataset]; !ok {
			t.Errorf("query %q references unknown dataset %q", name, q.dataset)
		}
	}
}

func TestSummarize_Shape(t *testing.T) {
	durations := []time.Duration{
		2 * time.Millisecond,
		1 * time.Millisecond,
		3 * time.Millisecond,
		5 * time.Millisecond,
		4 * time.Millisecond,
	}
	r := summarize("q", "ds", 100, 2048, durations, durations, 1)
	if r.Min != 1.0 || r.Max != 5.0 || r.Median != 3.0 {
		t.Fatalf("min/median/max = %v/%v/%v, want 1/3/5", r.Min, r.Median, r.Max)
	}
	if r.Mean != 3.0 {
		t.Fatalf("mean = %v, want 3", r.Mean)
	}
	if r.Runs != 5 {
		t.Fatalf("runs = %d, want 5", r.Runs)
	}
}

func TestDataset_UsersSetupSeedsRows(t *testing.T) {
	dir := t.TempDir()
	db, err := engine.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()
	if err := setupUsers(context.Background(), db, 4096, 2048); err != nil {
		t.Fatalf("setupUsers: %v", err)
	}
	rows, err := db.Query(context.Background(), "SELECT count(*) FROM users")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if rows.Values[0][0].(int64) != 4096 {
		t.Fatalf("count = %v, want 4096", rows.Values[0][0])
	}
}

func TestMannWhitney_NoDifference(t *testing.T) {
	a := []float64{10, 11, 9, 10, 10}
	b := []float64{10, 11, 9, 10, 10}
	_, p := mannWhitneyU(a, b)
	if p < 0.9 {
		t.Fatalf("identical samples p = %v, want close to 1", p)
	}
}

func TestMannWhitney_LargeDifference(t *testing.T) {
	a := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	b := []float64{100, 110, 120, 130, 140, 150, 160, 170, 180, 190}
	_, p := mannWhitneyU(a, b)
	if p > 0.01 {
		t.Fatalf("clearly different samples p = %v, want < 0.01", p)
	}
}

func TestMannWhitney_EmptySamples(t *testing.T) {
	if _, p := mannWhitneyU(nil, []float64{1, 2}); p != 1 {
		t.Fatalf("empty base p = %v, want 1", p)
	}
}

func TestCompareReports_Roundtrip(t *testing.T) {
	dir := t.TempDir()
	basePath := dir + "/base.json"
	headPath := dir + "/head.json"
	mustWriteReport(t, basePath, runReport{Query: "q1", Median: 10, Durations: []float64{9, 10, 11}})
	mustWriteReport(t, headPath, runReport{Query: "q1", Median: 20, Durations: []float64{19, 20, 21}})
	var out bytes.Buffer
	if err := compareReports(basePath, headPath, 0.5, &out); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	if !strings.Contains(got, "q1") || !strings.Contains(got, "regressed") {
		t.Fatalf("compare output missing expected fields: %q", got)
	}
}

func TestFormatMs(t *testing.T) {
	cases := []struct {
		in   float64
		want string
	}{
		{0.5, "500 us"},
		{12.34, "12.34 ms"},
		{1500, "1.500 s"},
	}
	for _, c := range cases {
		if got := formatMs(c.in); got != c.want {
			t.Errorf("formatMs(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}
