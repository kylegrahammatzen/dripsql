// Bench harness that loads a synthetic dataset and runs a query N times printing a benchstat-style line.
// Subcommand list enumerates the catalog and compare diffs two JSON outputs.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/engine"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

const benchDir = "./bench-db"

type runReport struct {
	Query       string        `json:"query"`
	Dataset     string        `json:"dataset"`
	Rows        int           `json:"rows"`
	SegmentRows int           `json:"segment_rows"`
	Mode        string        `json:"mode"`
	Cold        float64       `json:"cold_ms,omitempty"`
	Runs        int           `json:"runs"`
	Durations   []float64     `json:"durations_ms"`
	Min         float64       `json:"min_ms"`
	Median      float64       `json:"median_ms"`
	P95         float64       `json:"p95_ms"`
	Max         float64       `json:"max_ms"`
	Mean        float64       `json:"mean_ms"`
	StdDev      float64       `json:"stddev_ms"`
	IOReadMs    float64       `json:"io_read_ms"`
	DecodeMs    float64       `json:"decode_ms"`
	ExecMs      float64       `json:"exec_ms"`
	Env         envReport     `json:"env"`
	Result      int           `json:"result_rows"`
	WallSetup   time.Duration `json:"-"`
}

type envReport struct {
	GoVersion  string `json:"go_version"`
	GOOS       string `json:"goos"`
	GOARCH     string `json:"goarch"`
	GOMAXPROCS int    `json:"gomaxprocs"`
	NumCPU     int    `json:"num_cpu"`
}

func main() {
	args := os.Args[1:]
	if len(args) > 0 {
		switch args[0] {
		case "list":
			listCatalog(os.Stdout)
			return
		case "compare":
			runCompare(args[1:])
			return
		}
	}
	runBench(args)
}

func runBench(args []string) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	queryName := fs.String("query", "count", "query name from the catalog (see `bench list`)")
	rows := fs.Int("rows", 100_000, "rows to seed in the dataset")
	runs := fs.Int("runs", 10, "number of timed query runs")
	jsonOut := fs.Bool("json", false, "emit JSON summary on stdout instead of a benchstat-style line")
	cpuProfile := fs.String("cpuprofile", "", "write a CPU profile to this path (captures the timed runs only)")
	mode := fs.String("mode", "hot", "hot or cold-soft, cold-soft closes and reopens the DB between runs")
	fs.Parse(args)

	q, ok := queries[*queryName]
	if !ok {
		fmt.Fprintf(os.Stderr, "bench: unknown query %q, use `bench list` to see options\n", *queryName)
		os.Exit(2)
	}
	ds, ok := datasets[q.dataset]
	if !ok {
		fmt.Fprintf(os.Stderr, "bench: query %q references unknown dataset %q\n", q.name, q.dataset)
		os.Exit(2)
	}
	if *runs < 1 {
		fmt.Fprintln(os.Stderr, "bench: -runs must be at least 1")
		os.Exit(2)
	}
	if *mode != "hot" && *mode != "cold-soft" {
		fmt.Fprintf(os.Stderr, "bench: -mode must be 'hot' or 'cold-soft' (got %q)\n", *mode)
		os.Exit(2)
	}

	segmentRows := *rows
	if segmentRows > vector.StandardBatchRows {
		segmentRows = vector.StandardBatchRows
	}
	if segmentRows < 1 {
		segmentRows = 1
	}

	if err := os.RemoveAll(benchDir); err != nil {
		fmt.Fprintln(os.Stderr, "bench: reset dir:", err)
		os.Exit(1)
	}

	ctx := context.Background()
	db, err := engine.Open(benchDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bench: open:", err)
		os.Exit(1)
	}

	setupStart := time.Now()
	if err := ds.setup(ctx, db, *rows, segmentRows); err != nil {
		db.Close()
		fmt.Fprintln(os.Stderr, "bench: dataset setup:", err)
		os.Exit(1)
	}
	setup := time.Since(setupStart)

	sqlText := q.sql(*rows)
	if *mode == "hot" {
		if _, err := db.Query(ctx, sqlText); err != nil {
			db.Close()
			fmt.Fprintln(os.Stderr, "bench: warmup query failed:", err)
			os.Exit(1)
		}
	}

	durations := make([]time.Duration, 0, *runs)
	var lastRows int
	storage.ResetTimings()

	if *cpuProfile != "" {
		pf, err := os.Create(*cpuProfile)
		if err != nil {
			db.Close()
			fmt.Fprintln(os.Stderr, "bench: create cpuprofile:", err)
			os.Exit(1)
		}
		if err := pprof.StartCPUProfile(pf); err != nil {
			pf.Close()
			db.Close()
			fmt.Fprintln(os.Stderr, "bench: start cpuprofile:", err)
			os.Exit(1)
		}
		defer func() {
			pprof.StopCPUProfile()
			pf.Close()
		}()
	}
	for i := range *runs {
		if *mode == "cold-soft" {
			if err := db.Close(); err != nil {
				fmt.Fprintln(os.Stderr, "bench: close between runs", err)
				os.Exit(1)
			}
			db, err = engine.Open(benchDir)
			if err != nil {
				fmt.Fprintln(os.Stderr, "bench: reopen between runs", err)
				os.Exit(1)
			}
		}
		start := time.Now()
		result, err := db.Query(ctx, sqlText)
		elapsed := time.Since(start)
		if err != nil {
			db.Close()
			fmt.Fprintln(os.Stderr, "bench: run", i, "failed:", err)
			os.Exit(1)
		}
		durations = append(durations, elapsed)
		lastRows = len(result.Values)
		if i == 0 && *mode == "cold-soft" {
			storage.ResetTimings()
		}
	}
	db.Close()

	statRuns := durations
	var coldMs float64
	if *mode == "cold-soft" && len(durations) > 1 {
		coldMs = float64(durations[0].Microseconds()) / 1000.0
		statRuns = durations[1:]
	}
	denom := len(statRuns)
	if denom < 1 {
		denom = 1
	}
	ioNs, decodeNs := storage.ReadTimings()
	rep := summarize(q.name, ds.name, *rows, segmentRows, durations, statRuns, lastRows)
	rep.Mode = *mode
	rep.WallSetup = setup
	rep.Cold = coldMs
	rep.IOReadMs = float64(ioNs) / 1e6 / float64(denom)
	rep.DecodeMs = float64(decodeNs) / 1e6 / float64(denom)
	rep.ExecMs = rep.Median - rep.IOReadMs - rep.DecodeMs
	if rep.ExecMs < 0 {
		rep.ExecMs = 0
	}

	if *jsonOut {
		if err := json.NewEncoder(os.Stdout).Encode(rep); err != nil {
			fmt.Fprintln(os.Stderr, "bench: encode json:", err)
			os.Exit(1)
		}
		return
	}
	printBenchstatLine(os.Stdout, rep)
}

func runCompare(args []string) {
	fs := flag.NewFlagSet("compare", flag.ExitOnError)
	threshold := fs.Float64("threshold", 10.0, "absolute median-delta percent that flips the verdict away from ~")
	fs.Parse(args)
	rest := fs.Args()
	if len(rest) != 2 {
		fmt.Fprintln(os.Stderr, "usage: bench compare base.json head.json [-threshold 10.0]")
		os.Exit(2)
	}
	if err := compareReports(rest[0], rest[1], *threshold, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "bench: compare", err)
		os.Exit(1)
	}
}

func summarize(query, dataset string, rows, segmentRows int, allDurations, statRuns []time.Duration, resultRows int) runReport {
	all := toMs(allDurations)
	ms := toMs(statRuns)
	sorted := append([]float64(nil), ms...)
	sort.Float64s(sorted)

	var sum float64
	for _, v := range ms {
		sum += v
	}
	mean := sum / float64(len(ms))
	var variance float64
	for _, v := range ms {
		variance += (v - mean) * (v - mean)
	}
	stddev := 0.0
	if len(ms) > 1 {
		stddev = math.Sqrt(variance / float64(len(ms)-1))
	}

	return runReport{
		Query:       query,
		Dataset:     dataset,
		Rows:        rows,
		SegmentRows: segmentRows,
		Runs:        len(all),
		Durations:   all,
		Min:         sorted[0],
		Median:      percentile(sorted, 0.5),
		P95:         percentile(sorted, 0.95),
		Max:         sorted[len(sorted)-1],
		Mean:        mean,
		StdDev:      stddev,
		Result:      resultRows,
		Env:         collectEnv(),
	}
}

func toMs(durations []time.Duration) []float64 {
	out := make([]float64, len(durations))
	for i, d := range durations {
		out[i] = float64(d.Microseconds()) / 1000.0
	}
	return out
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	rank := p * float64(len(sorted)-1)
	lo := int(math.Floor(rank))
	hi := int(math.Ceil(rank))
	if lo == hi {
		return sorted[lo]
	}
	frac := rank - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}

func collectEnv() envReport {
	return envReport{
		GoVersion:  runtime.Version(),
		GOOS:       runtime.GOOS,
		GOARCH:     runtime.GOARCH,
		GOMAXPROCS: runtime.GOMAXPROCS(0),
		NumCPU:     runtime.NumCPU(),
	}
}

func printBenchstatLine(w io.Writer, r runReport) {
	pct := 0.0
	if r.Median > 0 {
		pct = 100 * r.StdDev / r.Median
	}
	name := fmt.Sprintf("Benchmark_%s/rows=%d-%d", r.Query, r.Rows, r.Env.GOMAXPROCS)
	fmt.Fprintf(w, "%s  runs=%d  wall=%s (+/- %.2f%%)  io=%s  dec=%s  exec=%s  result=%d rows\n",
		name, r.Runs, formatMs(r.Median), pct, formatMs(r.IOReadMs), formatMs(r.DecodeMs), formatMs(r.ExecMs), r.Result)
}

func formatMs(ms float64) string {
	switch {
	case ms < 1:
		return fmt.Sprintf("%.0f us", ms*1000)
	case ms < 1000:
		return fmt.Sprintf("%.2f ms", ms)
	default:
		return fmt.Sprintf("%.3f s", ms/1000)
	}
}

func listCatalog(w io.Writer) {
	fmt.Fprintln(w, "Datasets:")
	dsNames := make([]string, 0, len(datasets))
	for name := range datasets {
		dsNames = append(dsNames, name)
	}
	sort.Strings(dsNames)
	for _, name := range dsNames {
		fmt.Fprintf(w, "  %s\n", name)
	}
	fmt.Fprintln(w, "Queries:")
	qNames := make([]string, 0, len(queries))
	for name := range queries {
		qNames = append(qNames, name)
	}
	sort.Strings(qNames)
	for _, name := range qNames {
		fmt.Fprintf(w, "  %-20s dataset=%s\n", name, queries[name].dataset)
	}
}

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
