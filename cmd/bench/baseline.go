package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/kylegrahammatzen/dripsql/internal/explain"
)

const regressionThresholdPct = 5.0

func loadBaseline(path string) (benchReport, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return benchReport{}, fmt.Errorf("read baseline: %w", err)
	}
	var report benchReport
	if err := json.Unmarshal(data, &report); err != nil {
		return benchReport{}, fmt.Errorf("parse baseline: %w", err)
	}
	return report, nil
}

// writeDiff prints per-query avg-ms deltas between baseline and current. By
// default only deltas whose magnitude exceeds regressionThresholdPct render;
// pass showAll=true to render every query. The diff bails when the two runs
// have different shape (rows, profile) since the comparison wouldn't be
// honest.
func writeDiff(w io.Writer, baseline, current benchReport, showAll bool) error {
	if baseline.Profile != current.Profile {
		return fmt.Errorf("baseline profile %q does not match current %q", baseline.Profile, current.Profile)
	}
	if baseline.Rows != current.Rows {
		return fmt.Errorf("baseline rows %d does not match current %d", baseline.Rows, current.Rows)
	}
	commit := baseline.Env.Commit
	if commit == "" {
		commit = "unknown"
	}
	fmt.Fprintf(w, "Compared to baseline (commit %s):\n", commit)
	prev := indexAvgs(baseline.Queries)
	for _, q := range current.Queries {
		ours := avgOf(q)
		theirs, hadBaseline := prev[q.Name]
		if !hadBaseline {
			if showAll {
				fmt.Fprintf(w, "  %s: avg %s (no baseline)\n", q.Name, formatMsField(ours))
			}
			continue
		}
		delta := percentDelta(theirs, ours)
		if !showAll && absFloat(delta) < regressionThresholdPct {
			continue
		}
		marker := ""
		if delta > regressionThresholdPct {
			marker = " regression"
		}
		fmt.Fprintf(w, "  %s: avg %s -> %s (%+.1f%%)%s\n",
			q.Name,
			formatMsField(theirs),
			formatMsField(ours),
			delta,
			marker)
	}
	return nil
}

func indexAvgs(queries []*explain.QueryReport) map[string]float64 {
	out := make(map[string]float64, len(queries))
	for _, q := range queries {
		out[q.Name] = avgOf(q)
	}
	return out
}

func avgOf(q *explain.QueryReport) float64 {
	if q == nil || q.Timing == nil {
		return 0
	}
	return q.Timing.AvgMs
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

func absFloat(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
