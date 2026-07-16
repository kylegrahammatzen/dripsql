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
	Query        string        `json:"query"`
	Dataset      string        `json:"dataset"`
	Rows         int           `json:"rows"`
	SegmentRows  int           `json:"segment_rows"`
	Mode         string        `json:"mode"`
	Cold         float64       `json:"cold_ms,omitempty"`
	Runs         int           `json:"runs"`
	Durations    []float64     `json:"durations_ms"`
	Min          float64       `json:"min_ms"`
	Median       float64       `json:"median_ms"`
	P95          float64       `json:"p95_ms"`
	Max          float64       `json:"max_ms"`
	Mean         float64       `json:"mean_ms"`
	StdDev       float64       `json:"stddev_ms"`
	IOReadMs     float64       `json:"io_read_ms"`
	DecodeMs     float64       `json:"decode_ms"`
	ExecMs       float64       `json:"exec_ms"`
	IOReadMsRuns []float64     `json:"io_read_ms_runs"`
	DecodeMsRuns []float64     `json:"decode_ms_runs"`
	Env          envReport     `json:"env"`
	Result       int           `json:"result_rows"`
	WallSetup    time.Duration `json:"-"`
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
	mode := fs.String("mode", "hot", "hot, cold-soft (close and reopen the DB between runs), or cold (also purge the OS standby list, needs elevation)")
	reuse := fs.Bool("reuse", false, "reuse the existing bench-db when its manifest matches dataset, rows, and segment rows")
	columnar := fs.Bool("columnar", false, "drain results through QueryBatches instead of Query to measure the columnar path")
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
	if *mode != "hot" && *mode != "cold-soft" && *mode != "cold" {
		fmt.Fprintf(os.Stderr, "bench: -mode must be 'hot', 'cold-soft', or 'cold' (got %q)\n", *mode)
		os.Exit(2)
	}
	coldMode := *mode != "hot"
	purgeCache := *mode == "cold"

	segmentRows := *rows
	if segmentRows > vector.StandardBatchRows {
		segmentRows = vector.StandardBatchRows
	}
	if segmentRows < 1 {
		segmentRows = 1
	}

	seeded := *reuse && manifestMatches(ds.name, *rows, segmentRows)
	if !seeded {
		_ = os.Remove(manifestPath())
		if err := os.RemoveAll(benchDir); err != nil {
			fmt.Fprintln(os.Stderr, "bench: reset dir:", err)
			os.Exit(1)
		}
	}

	ctx := context.Background()
	db, err := engine.Open(benchDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bench: open:", err)
		os.Exit(1)
	}

	var setup time.Duration
	if !seeded {
		setupStart := time.Now()
		if err := ds.setup(ctx, db, *rows, segmentRows); err != nil {
			db.Close()
			fmt.Fprintln(os.Stderr, "bench: dataset setup:", err)
			os.Exit(1)
		}
		setup = time.Since(setupStart)
		writeManifest(ds.name, *rows, segmentRows)
	}

	sqlText := q.sql(*rows)
	if *mode == "hot" {
		if _, err := db.Query(ctx, sqlText); err != nil {
			db.Close()
			fmt.Fprintln(os.Stderr, "bench: warmup query failed:", err)
			os.Exit(1)
		}
	}

	durations := make([]time.Duration, 0, *runs)
	ioRuns := make([]float64, 0, *runs)
	decodeRuns := make([]float64, 0, *runs)
	var lastRows int

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
		if coldMode {
			if err := db.Close(); err != nil {
				fmt.Fprintln(os.Stderr, "bench: close between runs", err)
				os.Exit(1)
			}
			if purgeCache {
				if err := purgeStandbyList(); err != nil {
					fmt.Fprintln(os.Stderr, "bench: standby list purge failed, falling back to cold-soft (run from an elevated shell for true cold):", err)
					purgeCache = false
				}
			}
			db, err = engine.Open(benchDir)
			if err != nil {
				fmt.Fprintln(os.Stderr, "bench: reopen between runs", err)
				os.Exit(1)
			}
		}
		storage.ResetTimings()
		start := time.Now()
		var gotRows int
		var runErr error
		if *columnar {
			var result *engine.BatchRows
			result, runErr = db.QueryBatches(ctx, sqlText)
			if runErr == nil {
				gotRows = result.Rows
			}
		} else {
			var result *engine.Rows
			result, runErr = db.Query(ctx, sqlText)
			if runErr == nil {
				gotRows = len(result.Values)
			}
		}
		elapsed := time.Since(start)
		if runErr != nil {
			db.Close()
			fmt.Fprintln(os.Stderr, "bench: run", i, "failed:", runErr)
			os.Exit(1)
		}
		durations = append(durations, elapsed)
		lastRows = gotRows
		ioNs, decodeNs := storage.ReadTimings()
		ioRuns = append(ioRuns, float64(ioNs)/1e6)
		decodeRuns = append(decodeRuns, float64(decodeNs)/1e6)
	}
	db.Close()

	statRuns := durations
	statIO, statDecode := ioRuns, decodeRuns
	var coldMs float64
	if coldMode && len(durations) > 1 {
		coldMs = float64(durations[0].Microseconds()) / 1000.0
		statRuns = durations[1:]
		statIO = ioRuns[1:]
		statDecode = decodeRuns[1:]
	}
	rep := summarize(q.name, ds.name, *rows, segmentRows, durations, statRuns, lastRows)
	rep.Mode = *mode
	rep.WallSetup = setup
	rep.Cold = coldMs
	rep.IOReadMsRuns = ioRuns
	rep.DecodeMsRuns = decodeRuns
	rep.IOReadMs = meanOf(statIO)
	rep.DecodeMs = meanOf(statDecode)
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

type benchManifest struct {
	Dataset     string `json:"dataset"`
	Rows        int    `json:"rows"`
	SegmentRows int    `json:"segment_rows"`
}

// The manifest lives beside the DB dir because the engine owns everything inside it.
func manifestPath() string { return benchDir + ".manifest.json" }

func manifestMatches(dataset string, rows, segmentRows int) bool {
	data, err := os.ReadFile(manifestPath())
	if err != nil {
		return false
	}
	var m benchManifest
	if err := json.Unmarshal(data, &m); err != nil {
		return false
	}
	return m.Dataset == dataset && m.Rows == rows && m.SegmentRows == segmentRows
}

// writeManifest failures only cost a reseed on the next -reuse run, so they are not fatal.
func writeManifest(dataset string, rows, segmentRows int) {
	data, err := json.Marshal(benchManifest{Dataset: dataset, Rows: rows, SegmentRows: segmentRows})
	if err != nil {
		return
	}
	_ = os.WriteFile(manifestPath(), data, 0o644)
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

func meanOf(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	var sum float64
	for _, v := range vals {
		sum += v
	}
	return sum / float64(len(vals))
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
	if r.Cold > 0 && len(r.IOReadMsRuns) > 0 {
		fmt.Fprintf(w, "  cold run0  wall=%s  io=%s  dec=%s\n",
			formatMs(r.Cold), formatMs(r.IOReadMsRuns[0]), formatMs(r.DecodeMsRuns[0]))
	}
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
		return nil, fmt.Errorf("no reports in %s", path)
	}
	return out, nil
}

func joinedQueryNames(a, b map[string]runReport) []string {
	out := make([]string, 0, len(a)+len(b))
	for k := range a {
		out = append(out, k)
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}
