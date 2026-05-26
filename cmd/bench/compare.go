// Compare two bench JSON outputs by median delta and flag when the move exceeds a threshold percent.
// Use benchstat for distributional rigor, this is the quick "did the median move" check.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
)

func compareReports(basePath, headPath string, thresholdPct float64, out io.Writer) error {
	base, err := loadReports(basePath)
	if err != nil {
		return fmt.Errorf("load base: %w", err)
	}
	head, err := loadReports(headPath)
	if err != nil {
		return fmt.Errorf("load head: %w", err)
	}
	names := joinedQueryNames(base, head)
	fmt.Fprintf(out, "%-22s %14s %14s %10s %s\n", "query", "base (median)", "head (median)", "delta", "verdict")
	for _, name := range names {
		b, bok := base[name]
		h, hok := head[name]
		if !bok {
			fmt.Fprintf(out, "%-22s %14s %14s %10s %s\n", name, "missing", formatMs(h.Median), "", "head-only")
			continue
		}
		if !hok {
			fmt.Fprintf(out, "%-22s %14s %14s %10s %s\n", name, formatMs(b.Median), "missing", "", "base-only")
			continue
		}
		delta := 0.0
		if b.Median > 0 {
			delta = 100 * (h.Median - b.Median) / b.Median
		}
		verdict := "~"
		if math.Abs(delta) > thresholdPct {
			if delta > 0 {
				verdict = "regressed"
			} else {
				verdict = "improved"
			}
		}
		fmt.Fprintf(out, "%-22s %14s %14s %+9.2f%% %s\n",
			name, formatMs(b.Median), formatMs(h.Median), delta, verdict)
	}
	return nil
}

func loadReports(path string) (map[string]runReport, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := make(map[string]runReport)
	dec := json.NewDecoder(f)
	for dec.More() {
		var r runReport
		if err := dec.Decode(&r); err != nil {
			return nil, err
		}
		out[r.Query] = r
	}
	if len(out) == 0 {
		f.Seek(0, 0)
		var r runReport
		if err := json.NewDecoder(f).Decode(&r); err != nil {
			return nil, err
		}
		out[r.Query] = r
	}
	return out, nil
}

func joinedQueryNames(a, b map[string]runReport) []string {
	seen := make(map[string]struct{})
	var out []string
	for k := range a {
		if _, ok := seen[k]; !ok {
			seen[k] = struct{}{}
			out = append(out, k)
		}
	}
	for k := range b {
		if _, ok := seen[k]; !ok {
			seen[k] = struct{}{}
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}
