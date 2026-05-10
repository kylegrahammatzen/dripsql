// Command bench is the DripSQL v3 workload benchmark driver.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/engine"
	"github.com/kylegrahammatzen/dripsql/internal/explain"
	humanfmt "github.com/kylegrahammatzen/dripsql/internal/format"
	"github.com/kylegrahammatzen/dripsql/internal/types"
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

var defaultBenchmarkRows = rowsList{
	1_000,
	10_000,
	100_000,
	500_000,
	1_000_000,
	5_000_000,
	10_000_000,
	25_000_000,
	50_000_000,
}

type options struct {
	rows        rowsList
	dir         string
	keep        bool
	runs        int
	profile     string
	mode        string
	json        bool
	baseline    string
	showAll     bool
	details     bool
	query       string
	segmentRows int
}

type query struct {
	Name string `json:"name"`
	SQL  string `json:"sql"`
}

const (
	benchModeSameProcess = "same-process"
	benchModeWarmReopen  = "warm-reopen"
	benchModeColdish     = "cold-ish"
)

func benchModesFor(mode string) ([]string, error) {
	switch mode {
	case benchModeSameProcess, benchModeWarmReopen, benchModeColdish:
		return []string{mode}, nil
	case "all":
		return []string{benchModeSameProcess, benchModeWarmReopen, benchModeColdish}, nil
	default:
		return nil, fmt.Errorf("unknown benchmark mode %q", mode)
	}
}

func effectiveMode(mode string) string {
	if mode == "" {
		return benchModeSameProcess
	}
	return mode
}

var baseRegisteredQueries = []query{
	{Name: "event checkout total", SQL: "SELECT count(*) FROM events WHERE event_type = 'checkout'"},
	{Name: "event counts", SQL: "SELECT event_type, count(*) FROM events GROUP BY event_type"},
	{Name: "status counts", SQL: "SELECT status, count(*) FROM events GROUP BY status"},
	{Name: "path checkout count", SQL: "SELECT count(*) FROM events WHERE path = '/checkout/confirm'"},
	{Name: "event aggregate summary", SQL: "SELECT count(*) AS events, sum(amount) AS amount, min(amount) AS min_amount, max(amount) AS max_amount FROM events"},
	{Name: "country aggregate summary", SQL: "SELECT country, count(*) AS events, sum(amount) AS amount FROM events GROUP BY country"},
	{Name: "event checkout for tenant", SQL: "SELECT count(*) FROM events WHERE tenant_id = 42 AND event_type = 'checkout'"},
	{Name: "absent tenant", SQL: "SELECT count(*) FROM events WHERE tenant_id = 999999"},
	{Name: "checkout amount for tenant", SQL: "SELECT sum(amount) FROM events WHERE tenant_id = 42 AND event_type = 'checkout'"},
	{Name: "checkout counts by country", SQL: "SELECT country, count(*) FROM events WHERE event_type = 'checkout' GROUP BY country"},
}

type profileLookupQueries struct {
	UserIDs []int64
	URLs    []string
	UUIDs   []string
	Created []int64
}

func buildBenchmarkQueries(profile profileSpec, rows int64) []query {
	lookups := profile.lookupValues(rows)
	queries := make([]query, 0, len(baseRegisteredQueries)+4)
	queries = append(queries, baseRegisteredQueries...)
	queries = append(queries, query{
		Name: "user id lookup",
		SQL:  fmt.Sprintf("SELECT count(*) FROM events WHERE user_id = %d", lookups.UserIDs[0]),
	})
	createdAt := createdAtForRow(benchLookupRow)
	if len(lookups.Created) > 0 {
		createdAt = lookups.Created[0]
	}
	queries = append(queries, query{
		Name: "created_at point lookup",
		SQL:  fmt.Sprintf("SELECT count(*) FROM events WHERE created_at = %d", createdAt),
	})
	queries = append(queries,
		query{Name: "uuid lookup", SQL: fmt.Sprintf("SELECT count(*) FROM events WHERE event_uuid = '%s'", lookups.UUIDs[0])},
		query{Name: "url lookup", SQL: fmt.Sprintf("SELECT count(*) FROM events WHERE url = '%s'", lookups.URLs[0])},
	)
	return queries
}

func benchmarkRowForLookup(rows int64) int64 {
	if rows <= 0 {
		return 0
	}
	row := benchLookupRow
	if row >= rows {
		row = rows - 1
	}
	if row < 0 {
		return 0
	}
	return row
}

type benchReport struct {
	Env      envInfo             `json:"env"`
	Dir      string              `json:"dir"`
	Profile  string              `json:"profile"`
	Mode     string              `json:"mode"`
	ModeNote string              `json:"mode_note,omitempty"`
	Rows     int64               `json:"rows"`
	Runs     int                 `json:"runs"`
	Load     loadStats           `json:"load"`
	Storage  engine.StorageStats `json:"storage"`
	Queries  []queryReport       `json:"queries"`
}

type queryReport struct {
	Name    string         `json:"query_name"`
	SQL     string         `json:"sql"`
	Columns []string       `json:"columns"`
	Values  [][]any        `json:"values"`
	Rows    int            `json:"rows"`
	Timing  explain.Timing `json:"timing"`
	Explain explain.Report `json:"explain"`
}

type loadStats struct {
	Rows       int64         `json:"rows"`
	Elapsed    time.Duration `json:"elapsed"`
	GenerateNs int64         `json:"generate_ns"`
	AppendNs   int64         `json:"append_ns"`
}

type envInfo struct {
	Commit string `json:"commit"`
	Dirty  bool   `json:"dirty"`
	Go     string `json:"go"`
	OS     string `json:"os"`
	Arch   string `json:"arch"`
	CPUs   int    `json:"cpus"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseOptions(args []string) (options, error) {
	opts := options{rows: append(rowsList(nil), defaultBenchmarkRows...), runs: 5, profile: "all", mode: benchModeSameProcess}
	fs := flag.NewFlagSet("bench", flag.ContinueOnError)
	fs.Var(&opts.rows, "rows", "comma-separated row counts to benchmark (defaults: "+fmt.Sprintf("%v", defaultBenchmarkRows)+")")
	fs.StringVar(&opts.dir, "dir", "", "database directory (default: tempdir)")
	fs.BoolVar(&opts.keep, "keep", false, "keep the database directory after exit")
	fs.IntVar(&opts.runs, "runs", opts.runs, "number of timing samples per query")
	fs.StringVar(&opts.profile, "profile", opts.profile, "data profile (structured|random|skewed|wide-text|sorted|all; all runs structured,random,skewed)")
	fs.StringVar(&opts.mode, "mode", opts.mode, "query mode (same-process|warm-reopen|cold-ish|all)")
	fs.BoolVar(&opts.json, "json", false, "emit machine-readable JSON output")
	fs.StringVar(&opts.baseline, "baseline", "", "compare against a previously emitted JSON report")
	fs.BoolVar(&opts.showAll, "show-all", false, "in baseline mode, show every query (not just regressions)")
	fs.BoolVar(&opts.details, "details", false, "in baseline mode, print full breakdowns for shown queries")
	fs.StringVar(&opts.query, "query", "", "run only queries whose name or SQL contains this string")
	fs.IntVar(&opts.segmentRows, "segment-rows", 0, "rows per sealed segment (0 = storage default 131072; larger = fewer seals = faster ingest)")
	if err := fs.Parse(args); err != nil {
		return options{}, err
	}
	if opts.runs <= 0 {
		return options{}, fmt.Errorf("-runs must be positive")
	}
	if _, err := profileSpecsFor(opts.profile); err != nil {
		return options{}, err
	}
	if _, err := benchModesFor(opts.mode); err != nil {
		return options{}, err
	}
	return opts, nil
}

func run(args []string, out io.Writer) error {
	opts, err := parseOptions(args)
	if err != nil {
		return err
	}
	profiles, err := profileSpecsFor(opts.profile)
	if err != nil {
		return err
	}
	modes, err := benchModesFor(opts.mode)
	if err != nil {
		return err
	}
	var baselines []benchReport
	if opts.baseline != "" {
		baselines, err = loadBaselines(opts.baseline, profileNames(profiles))
		if err != nil {
			return err
		}
	}
	total := len(opts.rows) * len(profiles) * len(modes)
	reports := make([]benchReport, 0, total)
	for _, rows := range opts.rows {
		for _, profile := range profiles {
			for _, mode := range modes {
				runOpts := opts
				runOpts.profile = profile.name
				runOpts.mode = mode
				if opts.dir != "" && total > 1 {
					runOpts.dir = filepath.Join(opts.dir, fmt.Sprintf("%d-%s-%s", rows, profile.name, mode))
				}
				report, err := runOne(runOpts, rows, profile)
				if err != nil {
					return err
				}
				reports = append(reports, report)
			}
		}
	}
	if opts.json {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		for _, report := range reports {
			if err := enc.Encode(report); err != nil {
				return err
			}
		}
		return nil
	}
	if opts.baseline != "" {
		return writeComparison(out, baselines, reports, opts.showAll, opts.details)
	}
	for i, report := range reports {
		if i > 0 {
			fmt.Fprintln(out)
		}
		emitText(out, report)
	}
	return nil
}

func profileNames(profiles []profileSpec) []string {
	names := make([]string, len(profiles))
	for i, profile := range profiles {
		names[i] = profile.name
	}
	return names
}

func runOne(opts options, rows int64, profile profileSpec) (benchReport, error) {
	dir := opts.dir
	cleanup := func() error { return nil }
	if dir == "" {
		tmp, err := os.MkdirTemp("", "dripsql-v3-bench-*")
		if err != nil {
			return benchReport{}, err
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
		return benchReport{}, err
	}
	defer func() {
		if db != nil {
			_ = db.Close()
		}
	}()
	if err := createBenchTable(ctx, db, profile, opts.segmentRows); err != nil {
		return benchReport{}, fmt.Errorf("create schema: %w", err)
	}
	loadStart := time.Now()
	load, err := loadRows(ctx, db, rows, profile)
	if err != nil {
		return benchReport{}, err
	}
	load.Elapsed = time.Since(loadStart)
	modeNote, err := prepareQueryMode(ctx, &db, dir, opts.mode)
	if err != nil {
		return benchReport{}, err
	}

	benchQueries := buildBenchmarkQueries(profile, rows)
	queries, err := runQuerySet(ctx, db, benchQueries, opts.runs, opts.query)
	if err != nil {
		return benchReport{}, err
	}
	storageStats, err := db.StorageStats(ctx, "events")
	if err != nil {
		return benchReport{}, err
	}
	return benchReport{Env: captureEnv(), Dir: dir, Profile: opts.profile, Mode: opts.mode, ModeNote: modeNote, Rows: rows, Runs: opts.runs, Load: load, Storage: storageStats, Queries: queries}, nil
}

func prepareQueryMode(ctx context.Context, db **engine.DB, dir string, mode string) (string, error) {
	switch effectiveMode(mode) {
	case benchModeSameProcess:
		return "queries run in the same process immediately after load", nil
	case benchModeWarmReopen:
		return "database closed and reopened before queries; OS file cache may remain warm", reopenBenchDB(ctx, db, dir)
	case benchModeColdish:
		runtime.GC()
		debug.FreeOSMemory()
		return "best-effort reopen after freeing Go memory; OS file cache is not cleared", reopenBenchDB(ctx, db, dir)
	}
	return "", fmt.Errorf("unknown benchmark mode %q", mode)
}

func reopenBenchDB(ctx context.Context, db **engine.DB, dir string) error {
	if *db != nil {
		if err := (*db).Close(); err != nil {
			return err
		}
	}
	reopened, err := engine.Open(ctx, dir, engine.Options{})
	if err != nil {
		return err
	}
	*db = reopened
	return nil
}

// loadRows generates batches in parallel and submits them in order.
// Worker w handles batch indices w, w+W, w+2W, ... so the consumer can
// round-robin across worker channels and naturally read batches in
// sequential order — no reorder buffer, all workers stay fed.
func loadRows(ctx context.Context, db *engine.DB, rows int64, profile profileSpec) (loadStats, error) {
	stats := loadStats{Rows: rows}
	batchSize := int64(types.StandardBatchRows)
	batchCount := int((rows + batchSize - 1) / batchSize)

	flushFinal := func() error {
		appendStart := time.Now()
		err := db.FlushBuffered(ctx, "events")
		stats.AppendNs += time.Since(appendStart).Nanoseconds()
		if err != nil {
			return fmt.Errorf("flush rows: %w", err)
		}
		return nil
	}
	if batchCount == 0 {
		return stats, flushFinal()
	}

	workers := min(runtime.GOMAXPROCS(0), batchCount)
	type job struct {
		batch types.Batch
		ns    int64
		err   error
	}
	chans := make([]chan job, workers)
	for w := 0; w < workers; w++ {
		ch := make(chan job, 2)
		chans[w] = ch
		go func(start int, ch chan<- job) {
			defer close(ch)
			for i := start; i < batchCount; i += workers {
				pos := int64(i) * batchSize
				n := batchSize
				if pos+n > rows {
					n = rows - pos
				}
				t := time.Now()
				batch, err := profile.buildBatch(pos, int(n))
				ch <- job{batch: batch, ns: time.Since(t).Nanoseconds(), err: err}
				if err != nil {
					return
				}
			}
		}(w, ch)
	}

	for i := 0; i < batchCount; i++ {
		j, ok := <-chans[i%workers]
		if !ok {
			return loadStats{}, fmt.Errorf("worker %d closed early at batch %d", i%workers, i)
		}
		if j.err != nil {
			return loadStats{}, j.err
		}
		stats.GenerateNs += j.ns
		appendStart := time.Now()
		if err := db.AppendBufferedOwned(ctx, "events", j.batch); err != nil {
			return loadStats{}, fmt.Errorf("append batch: %w", err)
		}
		stats.AppendNs += time.Since(appendStart).Nanoseconds()
	}
	return stats, flushFinal()
}

func runQuerySet(ctx context.Context, db *engine.DB, queries []query, runs int, queryFilter string) ([]queryReport, error) {
	out := make([]queryReport, 0, len(queries))
	for _, q := range queries {
		if !queryMatchesFilter(q, queryFilter) {
			continue
		}
		report, err := timeQuery(ctx, db, q, runs)
		if err != nil {
			return nil, err
		}
		out = append(out, report)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no benchmark queries matched %q", queryFilter)
	}
	return out, nil
}

func queryMatchesFilter(q query, filter string) bool {
	if filter == "" {
		return true
	}
	filter = strings.ToLower(filter)
	return strings.Contains(strings.ToLower(q.Name), filter) || strings.Contains(strings.ToLower(q.SQL), filter)
}

func timeQuery(ctx context.Context, db *engine.DB, q query, runs int) (queryReport, error) {
	report := queryReport{Name: q.Name, SQL: q.SQL, Timing: explain.Timing{Samples: runs}}
	var firstExplain explain.Report
	var totalNs int64
	samples := make([]int64, 0, runs)
	for i := range runs {
		var sampleNs int64
		for iterations := int64(0); iterations == 0 || (sampleNs == 0 && iterations < 10_000); iterations++ {
			start := time.Now()
			rows, runExplain, err := db.ExplainAnalyze(ctx, q.SQL)
			elapsed := time.Since(start)
			if err != nil {
				return queryReport{}, fmt.Errorf("query %s: %w", q.Name, err)
			}
			values := normalizeValues(rows.Values)
			if i == 0 && iterations == 0 {
				report.Columns = append([]string(nil), rows.Columns...)
				report.Values = values
				report.Rows = len(values)
				firstExplain = runExplain
			} else if !reflect.DeepEqual(rows.Columns, report.Columns) {
				return queryReport{}, fmt.Errorf("query %s returned unstable results", q.Name)
			} else if !reflect.DeepEqual(values, report.Values) {
				// Timing repeats should not hide benchmark numbers just because a
				// query returns rows in a different order or with nondeterministic ties.
			}
			sampleNs = (sampleNs*iterations + elapsed.Nanoseconds()) / (iterations + 1)
		}
		samples = append(samples, sampleNs)
		ms := float64(sampleNs) / float64(time.Millisecond)
		if i == 0 {
			report.Timing.FirstMs = ms
			report.Timing.BestMs = ms
			report.Timing.FirstNs = sampleNs
			report.Timing.BestNs = sampleNs
		}
		if sampleNs < report.Timing.BestNs {
			report.Timing.BestMs = ms
			report.Timing.BestNs = sampleNs
		}
		totalNs += sampleNs
	}
	report.Timing.AvgNs = totalNs / int64(runs)
	report.Timing.AvgMs = float64(report.Timing.AvgNs) / float64(time.Millisecond)
	report.Timing.P95Ns = percentileNearestRank(samples, 95)
	report.Timing.P95Ms = float64(report.Timing.P95Ns) / float64(time.Millisecond)
	firstExplain.Timing = report.Timing
	report.Explain = firstExplain
	return report, nil
}

func percentileNearestRank(samples []int64, percentile int) int64 {
	if len(samples) == 0 {
		return 0
	}
	ordered := append([]int64(nil), samples...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	if percentile <= 0 {
		return ordered[0]
	}
	if percentile >= 100 {
		return ordered[len(ordered)-1]
	}
	index := (percentile*len(ordered) + 99) / 100
	if index <= 0 {
		index = 1
	}
	return ordered[index-1]
}

func normalizeValues(values [][]any) [][]any {
	out := make([][]any, len(values))
	for i := range values {
		out[i] = append([]any(nil), values[i]...)
	}
	sort.Slice(out, func(i, j int) bool {
		left := out[i]
		right := out[j]
		for col := 0; col < len(left) && col < len(right); col++ {
			l := fmt.Sprint(left[col])
			r := fmt.Sprint(right[col])
			if l != r {
				return l < r
			}
		}
		return len(left) < len(right)
	})
	return out
}

func emitText(w io.Writer, report benchReport) {
	fmt.Fprintf(w, "DripSQL v3 Benchmark\n")
	fmt.Fprintf(w, "Directory:    %s\n", report.Dir)
	fmt.Fprintf(w, "Data:         %s\n", report.Profile)
	fmt.Fprintf(w, "Mode:         %s\n", effectiveMode(report.Mode))
	if report.ModeNote != "" {
		fmt.Fprintf(w, "Mode note:    %s\n", report.ModeNote)
	}
	fmt.Fprintf(w, "Rows:         %d\n", report.Rows)
	fmt.Fprintf(w, "Query runs:   %d\n\n", report.Runs)
	fmt.Fprintf(w, "Load Benchmark\n")
	fmt.Fprintf(w, "Elapsed:      %s\n", humanfmt.Duration(report.Load.Elapsed))
	fmt.Fprintf(w, "Generate:     %s\n", humanfmt.Duration(time.Duration(report.Load.GenerateNs)))
	fmt.Fprintf(w, "Append:       %s\n\n", humanfmt.Duration(time.Duration(report.Load.AppendNs)))
	fmt.Fprintf(w, "Storage\n")
	fmt.Fprintf(w, "Table size:        %s\n", formatBytes(report.Storage.TableBytes))
	fmt.Fprintf(w, "Column payload:    %s\n", formatBytes(report.Storage.ColumnPayloadBytes))
	fmt.Fprintf(w, "Storage overhead:  %s\n", formatBytes(report.Storage.StorageOverhead))
	fmt.Fprintf(w, "Plain estimate:    %s\n", formatBytes(report.Storage.PlainEstimate))
	fmt.Fprintf(w, "Column compression: %.2fx\n", report.Storage.ColumnCompression)
	fmt.Fprintf(w, "Table compression: %.2fx\n", report.Storage.TableCompression)
	fmt.Fprintf(w, "Bytes / row:       %.2f\n", report.Storage.BytesPerRow)
	if len(report.Storage.Columns) > 0 {
		fmt.Fprintln(w)
		writeColumnsTable(w, report.Storage.Columns)
	}
	fmt.Fprintln(w)
	fmt.Fprintf(w, "Query Benchmark\n")
	for _, q := range report.Queries {
		fmt.Fprintf(w, "\n%s\n", q.Name)
		fmt.Fprintf(w, "  %s\n", q.SQL)
		explain.RenderText(w, q.Explain, "  ")
	}
}

func writeColumnsTable(w io.Writer, cols []engine.ColumnStats) {
	fmt.Fprintln(w, "Columns")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "  Column\tType\tPlain\tStored\tRatio\tEncoding")
	for _, c := range cols {
		ratio := "—"
		if c.Compression > 0 {
			ratio = fmt.Sprintf("%.2fx", c.Compression)
		}
		encoding := strings.Join(c.Encodings, ", ")
		if encoding == "" {
			encoding = "—"
		}
		fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\t%s\t%s\n", c.Name, c.TypeName, formatBytes(c.PlainBytes), formatBytes(c.StoredBytes), ratio, encoding)
	}
	_ = tw.Flush()
}

func captureEnv() envInfo {
	env := envInfo{Go: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH, CPUs: runtime.NumCPU(), Commit: "unknown"}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return env
	}
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			if len(setting.Value) >= 8 {
				env.Commit = setting.Value[:8]
			} else if setting.Value != "" {
				env.Commit = setting.Value
			}
		case "vcs.modified":
			env.Dirty = setting.Value == "true"
		}
	}
	return env
}

func formatBytes(n int64) string {
	if n < 0 {
		return "-" + formatBytes(-n)
	}
	const unit = 1024
	if n < unit {
		return strconv.FormatInt(n, 10) + " B"
	}
	value := float64(n)
	units := []string{"KiB", "MiB", "GiB", "TiB"}
	for i, suffix := range units {
		value /= unit
		if value < unit || i == len(units)-1 {
			return strconv.FormatFloat(value, 'f', 2, 64) + " " + suffix
		}
	}
	return strconv.FormatInt(n, 10) + " B"
}

type profileSpec struct {
	name    string
	columns []columnSpec
	userFor int64Function
}

type columnSpec struct {
	name string
	typ  types.Type
	fill func(start int64, n int) types.Column
}

// createBenchTable goes through SQL when segmentRows is 0 (use storage default)
// or builds a TableSpec directly when a custom segment size is requested,
// since SQL CREATE TABLE doesn't expose physical options today.
func createBenchTable(ctx context.Context, db *engine.DB, profile profileSpec, segmentRows int) error {
	if segmentRows <= 0 {
		_, err := db.Exec(ctx, profile.createTableSQL("events"))
		return err
	}
	cols := make([]types.ColumnSpec, len(profile.columns))
	for i, c := range profile.columns {
		cols[i] = types.ColumnSpec{Name: c.name, Type: c.typ}
	}
	return db.CreateTable(ctx, types.TableSpec{
		Name:    "events",
		Columns: cols,
		Options: types.TableOptions{SegmentRows: types.SegmentRows(segmentRows)},
	})
}

func (p profileSpec) createTableSQL(table string) string {
	cols := make([]string, len(p.columns))
	for i, col := range p.columns {
		cols[i] = col.name + " " + col.typ.String() + " NOT NULL"
	}
	return "CREATE TABLE " + table + " (" + strings.Join(cols, ", ") + ")"
}

func (p profileSpec) lookupValues(rows int64) profileLookupQueries {
	lookupRow := benchmarkRowForLookup(rows)
	values := profileLookupQueries{
		UserIDs: []int64{p.userFor(lookupRow)},
		URLs:    []string{urlForRow(lookupRow)},
		UUIDs:   []string{uuidForRow(lookupRow)},
		Created: []int64{createdAtForRow(lookupRow)},
	}
	return values
}

type int64Function func(int64) int64

func (p profileSpec) buildBatch(start int64, n int) (types.Batch, error) {
	cols := make([]types.Column, len(p.columns))
	for i, col := range p.columns {
		cols[i] = col.fill(start, n)
	}
	batch, err := types.NewBatchNoClone(cols)
	if err != nil {
		return types.Batch{}, fmt.Errorf("build batch: %w", err)
	}
	return batch, nil
}

func profileSpecFor(profile string) (profileSpec, error) {
	switch profile {
	case "structured":
		return profileSpec{name: "structured", columns: baseProfileColumns(structuredValues), userFor: structuredValues.user}, nil
	case "random":
		return profileSpec{name: "random", columns: baseProfileColumns(randomValues), userFor: randomValues.user}, nil
	case "skewed":
		return profileSpec{name: "skewed", columns: baseProfileColumns(skewedValues), userFor: skewedValues.user}, nil
	case "wide-text":
		base := baseProfileColumns(randomValues)
		base = append(base, textColumnSpec("referrer_url", func(row int64) string { return urlForRow(row + 10_000_000) }))
		return profileSpec{name: "wide-text", columns: base, userFor: randomValues.user}, nil
	case "sorted":
		base := baseProfileColumns(structuredValues)
		base = append(base, int64ColumnSpec("cluster_key", func(row int64) int64 { return row % 10_000 }))
		return profileSpec{name: "sorted", columns: base, userFor: structuredValues.user}, nil
	}
	return profileSpec{}, fmt.Errorf("unknown profile %q", profile)
}

func profileSpecsFor(profile string) ([]profileSpec, error) {
	names := []string{profile}
	if profile == "all" {
		names = []string{"structured", "random", "skewed"}
	}
	out := make([]profileSpec, 0, len(names))
	for _, name := range names {
		spec, err := profileSpecFor(name)
		if err != nil {
			return nil, err
		}
		out = append(out, spec)
	}
	return out, nil
}

type profileValues struct {
	tenant  func(row int64) int64
	user    func(row int64) int64
	amount  func(row int64) int64
	event   func(row int64) string
	country func(row int64) string
	status  func(row int64) string
	path    func(row int64) string
}

func baseProfileColumns(values profileValues) []columnSpec {
	return []columnSpec{
		int64ColumnSpec("tenant_id", values.tenant),
		int64ColumnSpec("user_id", values.user),
		int64ColumnSpec("created_at", createdAtForRow),
		textColumnSpec("event_uuid", uuidForRow),
		textColumnSpec("status", values.status),
		textColumnSpec("path", values.path),
		textColumnSpec("url", urlForRow),
		int64ColumnSpec("amount", values.amount),
		textColumnSpec("event_type", values.event),
		textColumnSpec("country", values.country),
	}
}

func int64ColumnSpec(name string, value func(row int64) int64) columnSpec {
	return columnSpec{name: name, typ: types.Int64, fill: func(start int64, n int) types.Column {
		values := make([]int64, n)
		for i := range n {
			values[i] = value(start + int64(i))
		}
		return types.Column{Name: name, Type: types.Int64, V: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: n, I64: values}}
	}}
}

func textColumnSpec(name string, value func(row int64) string) columnSpec {
	return columnSpec{name: name, typ: types.Text, fill: func(start int64, n int) types.Column {
		dataBytes := 0
		strs := make([]string, n)
		for i := range n {
			strs[i] = value(start + int64(i))
			dataBytes += len(strs[i])
		}
		varbytes := types.NewVarBytes(n, dataBytes)
		for i, s := range strs {
			varbytes.AppendString(i, s)
		}
		return types.Column{Name: name, Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: n, Var: varbytes}}
	}}
}

var structuredValues = profileValues{
	tenant:  func(row int64) int64 { return (row % 1024) + 1 },
	user:    func(row int64) int64 { return row + 1 },
	amount:  func(row int64) int64 { return row % 100 },
	event:   func(row int64) string { return eventTypes[row%int64(len(eventTypes))] },
	country: func(row int64) string { return countries[row%int64(len(countries))] },
	status:  func(row int64) string { return statuses[row%int64(len(statuses))] },
	path:    func(row int64) string { return paths[row%int64(len(paths))] },
}

var randomValues = profileValues{
	tenant:  func(row int64) int64 { return int64(mix64(row, saltTenant)%4096) + 1 },
	user:    func(row int64) int64 { return int64(mix64(row, saltUser) % 50_000_000) },
	amount:  func(row int64) int64 { return int64(mix64(row, saltAmount) % 1000) },
	event:   func(row int64) string { return eventTypes[mix64(row, saltEvent)%uint64(len(eventTypes))] },
	country: func(row int64) string { return countries[mix64(row, saltCountry)%uint64(len(countries))] },
	status:  func(row int64) string { return statuses[mix64(row, saltStatus)%uint64(len(statuses))] },
	path:    func(row int64) string { return paths[mix64(row, saltPath)%uint64(len(paths))] },
}

var skewedValues = profileValues{
	tenant: func(row int64) int64 {
		r := mix64(row, saltTenant) % 100
		switch {
		case r < 80:
			return int64(mix64(row, saltTenant+1)%50) + 1
		case r < 95:
			return int64(mix64(row, saltTenant+2)%500) + 51
		default:
			return int64(mix64(row, saltTenant+3)%3500) + 551
		}
	},
	user:    func(row int64) int64 { return int64(mix64(row, saltUser) % 5_000_000) },
	amount:  func(row int64) int64 { return int64(mix64(row, saltAmount) % 1000) },
	event:   func(row int64) string { return pickWeighted(row, saltEvent, eventTypesSkewed) },
	country: func(row int64) string { return pickWeighted(row, saltCountry, countriesSkewed) },
	status:  func(row int64) string { return pickWeighted(row, saltStatus, statusesSkewed) },
	path:    func(row int64) string { return pickWeighted(row, saltPath, pathsSkewed) },
}

func createdAtForRow(row int64) int64 {
	const baseUnixMillis int64 = 1_700_000_000_000
	return baseUnixMillis + row*1000
}

func urlForRow(row int64) string {
	return "/users/" + strconv.FormatInt(row%100_000, 10) + "/events/" + strconv.FormatInt(row, 10) + "/" + strconv.FormatUint(mix64(row, saltURL), 36)
}

func uuidForRow(row int64) string {
	left := mix64(row, saltUUID)
	right := mix64(row, saltUUID+1)
	const hex = "0123456789abcdef"
	var b [36]byte
	put := func(off int, v uint64, n int) {
		for i := n - 1; i >= 0; i-- {
			b[off+i] = hex[v&0xf]
			v >>= 4
		}
	}
	put(0, left>>32, 8)
	b[8] = '-'
	put(9, left>>16, 4)
	b[13] = '-'
	put(14, left, 4)
	b[18] = '-'
	put(19, right>>48, 4)
	b[23] = '-'
	put(24, right&0xffffffffffff, 12)
	return string(b[:])
}

func pickWeighted(row int64, salt uint64, weighted []weightedString) string {
	r := mix64(row, salt) % 100
	var acc uint64
	for _, w := range weighted {
		acc += w.weight
		if r < acc {
			return w.value
		}
	}
	return weighted[len(weighted)-1].value
}

type weightedString struct {
	value  string
	weight uint64
}

func mix64(row int64, salt uint64) uint64 {
	x := uint64(row) ^ salt
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

const (
	saltTenant  uint64 = 0x9e3779b97f4a7c15
	saltUser    uint64 = 0xbf58476d1ce4e5b9
	saltAmount  uint64 = 0x94d049bb133111eb
	saltEvent   uint64 = 0xff51afd7ed558ccd
	saltCountry uint64 = 0xc4ceb9fe1a85ec53
	saltStatus  uint64 = 0x165667b19e3779f9
	saltPath    uint64 = 0xd6e8feb86659fd93
	saltURL     uint64 = 0x27d4eb2f165667c5
	saltUUID    uint64 = 0x319642b2d24d8ec3

	benchLookupRow int64 = 777
)

var eventTypes = []string{"signup", "checkout", "page_view", "cancel"}
var countries = []string{"US", "CA", "GB", "DE", "FR", "JP", "BR", "AU"}
var statuses = []string{"new", "queued", "paid", "failed", "refunded", "archived"}
var paths = []string{"/", "/products", "/products/detail", "/cart", "/checkout/start", "/checkout/confirm", "/account", "/support", "/pricing", "/search", "/docs", "/logout"}

var eventTypesSkewed = []weightedString{{"checkout", 50}, {"page_view", 30}, {"signup", 15}, {"cancel", 5}}
var countriesSkewed = []weightedString{{"US", 35}, {"CA", 20}, {"GB", 15}, {"DE", 10}, {"FR", 8}, {"JP", 6}, {"BR", 4}, {"AU", 2}}
var statusesSkewed = []weightedString{{"paid", 55}, {"new", 20}, {"queued", 10}, {"failed", 8}, {"refunded", 5}, {"archived", 2}}
var pathsSkewed = []weightedString{{"/products", 22}, {"/products/detail", 18}, {"/checkout/start", 15}, {"/checkout/confirm", 12}, {"/cart", 10}, {"/search", 8}, {"/", 5}, {"/account", 4}, {"/support", 3}, {"/pricing", 2}, {"/docs", 1}}
