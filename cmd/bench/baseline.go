package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/explain"
	humanfmt "github.com/kylegrahammatzen/dripsql/internal/format"
)

const regressionThresholdPct = 5.0

func loadBaselines(path string, profiles []string) ([]benchReport, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("read baseline: %w", err)
	}
	if !info.IsDir() {
		return loadBaselineFile(path)
	}
	var reports []benchReport
	for _, profile := range profiles {
		profileReports, err := loadBaselineFile(filepath.Join(path, profile+".json"))
		if err != nil {
			return nil, err
		}
		reports = append(reports, profileReports...)
	}
	if len(reports) == 0 {
		return nil, fmt.Errorf("baseline is empty")
	}
	return reports, nil
}

func loadBaselineFile(path string) ([]benchReport, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read baseline: %w", err)
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	var reports []benchReport
	for {
		var report benchReport
		err := dec.Decode(&report)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("parse baseline: %w", err)
		}
		reports = append(reports, report)
	}
	if len(reports) == 0 {
		return nil, fmt.Errorf("baseline is empty")
	}
	return reports, nil
}

func findBaseline(reports []benchReport, profile string, rows int64, mode ...string) (benchReport, bool) {
	wantMode := benchModeSameProcess
	if len(mode) != 0 {
		wantMode = effectiveMode(mode[0])
	}
	for _, report := range reports {
		if report.Profile == profile && report.Rows == rows && effectiveMode(report.Mode) == wantMode {
			return report, true
		}
	}
	return benchReport{}, false
}

// writeDiff prints per-query avg-ms deltas between baseline and current. By
// default only deltas whose magnitude exceeds regressionThresholdPct render;
// pass showAll=true to render every query.
func writeDiff(w io.Writer, baseline, current benchReport, showAll bool) error {
	if baseline.Profile != current.Profile {
		return fmt.Errorf("baseline profile %q does not match current %q", baseline.Profile, current.Profile)
	}
	if baseline.Rows != current.Rows {
		return fmt.Errorf("baseline rows %d does not match current %d", baseline.Rows, current.Rows)
	}
	if effectiveMode(baseline.Mode) != effectiveMode(current.Mode) {
		return fmt.Errorf("baseline mode %q does not match current %q", effectiveMode(baseline.Mode), effectiveMode(current.Mode))
	}
	commit := baseline.Env.Commit
	if commit == "" {
		commit = "unknown"
	}
	fprintf(w, "Compared to baseline (commit %s):\n", commit)
	prev := make(map[string]queryReport, len(baseline.Queries))
	for _, q := range baseline.Queries {
		prev[q.Name] = q
	}
	for _, q := range current.Queries {
		base, hadBaseline := prev[q.Name]
		if !hadBaseline {
			if showAll {
				fprintf(w, "  %s: avg %s (no baseline)\n", q.Name, q.Timing.FormatAvg())
			}
			continue
		}
		if base.Timing.AvgMs == 0 && base.Timing.AvgNs == 0 {
			if showAll {
				fprintf(w, "  %s: avg - -> %s (no baseline timing)\n", q.Name, q.Timing.FormatAvg())
			}
			continue
		}
		ours := q.Timing.AvgMs
		theirs := base.Timing.AvgMs
		delta := percentDelta(theirs, ours)
		if !showAll && delta <= regressionThresholdPct {
			continue
		}
		marker := ""
		if delta > regressionThresholdPct {
			marker = " regression"
		}
		fprintf(w, "  %s: avg %s -> %s (%+.1f%%)%s\n", q.Name, base.Timing.FormatAvg(), q.Timing.FormatAvg(), delta, marker)
	}
	return nil
}

type comparisonDetail struct {
	report benchReport
	query  queryReport
}

func writeComparison(w io.Writer, baselines []benchReport, current []benchReport, showAll bool, details bool) error {
	fprintf(w, "DripSQL v3 Benchmark Comparison\n")
	fprintf(w, "Threshold: %.1f%%\n", regressionThresholdPct)
	fprintf(w, "\nLoad\n")
	fprintf(w, "Profile      Mode          Rows       Baseline    Current     Delta\n")
	var loadDeltaTotal float64
	var loadCompared int
	var loadRegressions int
	for _, report := range current {
		baseline, ok := findBaseline(baselines, report.Profile, report.Rows, report.Mode)
		if !ok {
			return fmt.Errorf("baseline for profile %q mode %q rows %d not found", report.Profile, effectiveMode(report.Mode), report.Rows)
		}
		baseMs := msOf(baseline.Load.Elapsed)
		curMs := msOf(report.Load.Elapsed)
		delta := percentDelta(baseMs, curMs)
		loadDeltaTotal += delta
		loadCompared++
		if delta > regressionThresholdPct {
			loadRegressions++
		}
		fprintf(w, "%-12s %-12s %10d %10s %10s %8.1f%%\n", report.Profile, effectiveMode(report.Mode), report.Rows, humanfmt.Duration(baseline.Load.Elapsed), humanfmt.Duration(report.Load.Elapsed), delta)
	}

	fprintf(w, "\nQueries\n")
	fprintf(w, "Profile      Mode          Rows       Query                         Baseline    Current     Delta\n")
	printed := false
	var queryDeltaTotal float64
	var queryCompared int
	var queryRegressions int
	var queryImprovements int
	detailRows := []comparisonDetail{}
	for _, report := range current {
		baseline, ok := findBaseline(baselines, report.Profile, report.Rows, report.Mode)
		if !ok {
			return fmt.Errorf("baseline for profile %q mode %q rows %d not found", report.Profile, effectiveMode(report.Mode), report.Rows)
		}
		prev := make(map[string]queryReport, len(baseline.Queries))
		for _, q := range baseline.Queries {
			prev[q.Name] = q
		}
		for _, q := range report.Queries {
			base, hadBaseline := prev[q.Name]
			if !hadBaseline {
				if showAll {
					fprintf(w, "%-12s %-12s %10d %-29s %10s %10s %8s\n", report.Profile, effectiveMode(report.Mode), report.Rows, trimQueryName(q.Name), "-", q.Timing.FormatAvg(), "new")
					if details {
						detailRows = append(detailRows, comparisonDetail{report: report, query: q})
					}
					printed = true
				}
				continue
			}
			if base.Timing.AvgMs == 0 && base.Timing.AvgNs == 0 {
				if showAll {
					fprintf(w, "%-12s %-12s %10d %-29s %10s %10s %8s\n", report.Profile, effectiveMode(report.Mode), report.Rows, trimQueryName(q.Name), "-", q.Timing.FormatAvg(), "no base")
					if details {
						detailRows = append(detailRows, comparisonDetail{report: report, query: q})
					}
					printed = true
				}
				continue
			}
			ours := q.Timing.AvgMs
			theirs := base.Timing.AvgMs
			delta := percentDelta(theirs, ours)
			queryDeltaTotal += delta
			queryCompared++
			if delta > regressionThresholdPct {
				queryRegressions++
			} else if delta < -regressionThresholdPct {
				queryImprovements++
			}
			if !showAll && delta <= regressionThresholdPct {
				continue
			}
			marker := ""
			if delta > regressionThresholdPct {
				marker = " regression"
			}
			fprintf(w, "%-12s %-12s %10d %-29s %10s %10s %+7.1f%%%s\n", report.Profile, effectiveMode(report.Mode), report.Rows, trimQueryName(q.Name), base.Timing.FormatAvg(), q.Timing.FormatAvg(), delta, marker)
			if details {
				detailRows = append(detailRows, comparisonDetail{report: report, query: q})
			}
			printed = true
		}
	}
	if !printed {
		fprintf(w, "No query regressions over %.1f%%. Use -show-all to print every query.\n", regressionThresholdPct)
	}
	fprintf(w, "\nSummary\n")
	if loadCompared > 0 {
		fprintf(w, "Load avg delta:  %+.1f%% (%d regressions over %.1f%%)\n", loadDeltaTotal/float64(loadCompared), loadRegressions, regressionThresholdPct)
	}
	if queryCompared > 0 {
		fprintf(w, "Query avg delta: %+.1f%% (%d regressions, %d improvements over %.1f%%)\n", queryDeltaTotal/float64(queryCompared), queryRegressions, queryImprovements, regressionThresholdPct)
	}
	if details && len(detailRows) > 0 {
		fprintf(w, "\nDetails\n")
		for i, detail := range detailRows {
			if i > 0 {
				fprintf(w, "\n")
			}
			fprintf(w, "%s / %s / %d rows / %s\n", detail.report.Profile, effectiveMode(detail.report.Mode), detail.report.Rows, detail.query.Name)
			fprintf(w, "  %s\n", detail.query.SQL)
			explain.RenderText(w, detail.query.Explain, "  ")
		}
	}
	return nil
}

func msOf(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / float64(time.Millisecond)
}

func percentDelta(prev, cur float64) float64 {
	if prev == 0 {
		if cur == 0 {
			return 0
		}
		return 100
	}
	return (cur - prev) / prev * 100
}

func trimQueryName(name string) string {
	const max = 29
	if len(name) <= max {
		return name
	}
	if max <= 1 {
		return name[:max]
	}
	return name[:max-3] + "..."
}

func fprintf(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format, args...)
}
