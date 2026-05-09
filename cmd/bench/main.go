// Command bench is the DripSQL workload benchmark driver. It loads a
// generated dataset into a fresh engine.DB, runs a registered query set, and
// prints a sectioned report (Header / Load / Storage / Columns / Query
// Benchmark / Findings) that explains the storage layout and per-query
// access path. See explain-benchmark-reporting.md for the report shape.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/engine"
	"github.com/kylegrahammatzen/dripsql/internal/explain"
)

type rowsList []int64

func (r *rowsList) String() string {
	parts := make([]string, len(*r))
	for i, n := range *r {
		parts[i] = strconv.FormatInt(n, 10)
	}
	return strings.Join(parts, ",")
}

func (r *rowsList) Set(value string) error {
	if value == "" {
		return fmt.Errorf("rows list is empty")
	}
	*r = (*r)[:0]
	for part := range strings.SplitSeq(value, ",") {
		n, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
		if err != nil {
			return fmt.Errorf("rows %q: %w", part, err)
		}
		if n <= 0 {
			return fmt.Errorf("rows %q must be positive", part)
		}
		*r = append(*r, n)
	}
	return nil
}

type options struct {
	rows     rowsList
	dir      string
	keep     bool
	runs     int
	profile  string
	emit     string
	baseline string
	showAll  bool
}

func parseOptions(args []string) (options, error) {
	opts := options{
		rows:    rowsList{100_000},
		runs:    7,
		profile: "structured",
		emit:    "text",
	}
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	fs.Var(&opts.rows, "rows", "comma-separated row counts to benchmark")
	fs.StringVar(&opts.dir, "dir", "", "database directory (default: tempdir)")
	fs.BoolVar(&opts.keep, "keep", false, "keep the database directory after exit")
	fs.IntVar(&opts.runs, "runs", opts.runs, "number of timing samples per query")
	fs.StringVar(&opts.profile, "profile", opts.profile, "data profile (structured)")
	fs.StringVar(&opts.emit, "emit", opts.emit, "output format (text|json)")
	fs.StringVar(&opts.baseline, "baseline", "", "compare against a previously emitted JSON report")
	fs.BoolVar(&opts.showAll, "show-all", false, "in baseline mode, show every query (not just regressions)")
	if err := fs.Parse(args); err != nil {
		return options{}, err
	}
	if opts.runs <= 0 {
		return options{}, fmt.Errorf("-runs must be positive")
	}
	if _, err := builderFor(opts.profile); err != nil {
		return options{}, err
	}
	if opts.emit != "text" && opts.emit != "json" {
		return options{}, fmt.Errorf("-emit %q must be text or json", opts.emit)
	}
	return opts, nil
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, out io.Writer) error {
	opts, err := parseOptions(args)
	if err != nil {
		return err
	}
	for _, rows := range opts.rows {
		if err := runOne(opts, rows, out); err != nil {
			return err
		}
	}
	return nil
}

func runOne(opts options, rows int64, out io.Writer) error {
	dir := opts.dir
	cleanup := func() error { return nil }
	if dir == "" {
		tmp, err := os.MkdirTemp("", "dripsql-bench-*")
		if err != nil {
			return err
		}
		dir = tmp
		if !opts.keep {
			cleanup = func() error { return os.RemoveAll(tmp) }
		}
	}
	defer func() { _ = cleanup() }()

	ctx := context.Background()
	db, err := engine.Open(ctx, dir, engine.Options{})
	if err != nil {
		return err
	}
	defer db.Close()

	if err := createSchema(ctx, db); err != nil {
		return fmt.Errorf("create schema: %w", err)
	}
	def, ok := db.Table("events")
	if !ok {
		return fmt.Errorf("schema missing events table")
	}

	builder, err := builderFor(opts.profile)
	if err != nil {
		return err
	}
	loadStart := time.Now()
	loadStats, err := load(ctx, db.RawStore(), def, rows, builder)
	if err != nil {
		return fmt.Errorf("load: %w", err)
	}
	loadStats.Elapsed = time.Since(loadStart)
	if err := db.RawStore().FlushAllBuffered(ctx); err != nil {
		return fmt.Errorf("flush: %w", err)
	}

	tableStats, err := db.RawStore().TableStats(ctx, def)
	if err != nil {
		return fmt.Errorf("table stats: %w", err)
	}
	loadStats.Segments = tableStats.Segments
	loadStats.BytesTotal = tableStats.ColumnPayloadBytes
	if loadStats.Rows > 0 {
		loadStats.BytesPerRow = float64(loadStats.BytesTotal) / float64(loadStats.Rows)
	}

	queryReports, err := runQuerySet(ctx, db, opts.runs)
	if err != nil {
		return err
	}

	env := captureEnv()
	report := benchReport{
		Env:        env,
		Dir:        dir,
		Profile:    opts.profile,
		Rows:       rows,
		Runs:       opts.runs,
		LoadStats:  loadStats,
		TableStats: tableStats,
		Queries:    queryReports,
	}
	if opts.emit == "json" {
		return emitJSON(out, report)
	}
	if err := emitText(out, report); err != nil {
		return err
	}
	if opts.baseline != "" {
		baseline, err := loadBaseline(opts.baseline)
		if err != nil {
			return err
		}
		fmt.Fprintln(out)
		if err := writeDiff(out, baseline, report, opts.showAll); err != nil {
			return err
		}
	}
	return nil
}

// runQuerySet runs every registered query opts.Runs times, captures
// first/best/avg/samples timings, and returns one *explain.QueryReport per
// query with Reduction/Read/Access populated by EXPLAIN ANALYZE.
func runQuerySet(ctx context.Context, db *engine.DB, runs int) ([]*explain.QueryReport, error) {
	out := make([]*explain.QueryReport, 0, len(registeredQueries))
	for _, q := range registeredQueries {
		report, err := db.Explain(ctx, q.SQL, engine.ExplainOptions{Analyze: true})
		if err != nil {
			return nil, fmt.Errorf("EXPLAIN ANALYZE %s: %w", q.Name, err)
		}
		timing, err := timeQuery(ctx, db, q.SQL, runs)
		if err != nil {
			return nil, fmt.Errorf("time %s: %w", q.Name, err)
		}
		report.Name = q.Name
		report.SQL = q.SQL
		report.Timing = timing
		out = append(out, report)
	}
	return out, nil
}

func timeQuery(ctx context.Context, db *engine.DB, sql string, runs int) (*explain.Timing, error) {
	timing := &explain.Timing{Samples: runs}
	var total float64
	for i := range runs {
		start := time.Now()
		if _, err := db.Query(ctx, sql); err != nil {
			return nil, err
		}
		ms := float64(time.Since(start).Nanoseconds()) / 1_000_000.0
		if i == 0 {
			timing.FirstMs = ms
			timing.BestMs = ms
		}
		if ms < timing.BestMs {
			timing.BestMs = ms
		}
		total += ms
	}
	timing.AvgMs = total / float64(runs)
	return timing, nil
}
