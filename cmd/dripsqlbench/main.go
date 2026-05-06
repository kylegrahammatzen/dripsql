package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/table"
)

const minBenchmarkRuns = 3

var defaultBenchmarkRows = []int64{100_000, 500_000, 5_000_000}

type benchOptions struct {
	rows         int64
	rowSizes     []int64
	dir          string
	keep         bool
	tenant       int64
	event        string
	runs         int
	workers      int
	data         string
	seed         uint64
	openExisting bool
}

func main() {
	opts := parseOptions()
	for runIndex, rows := range opts.rowSizes {
		runOpts := opts
		runOpts.rows = rows
		if !opts.openExisting && len(opts.rowSizes) > 1 && opts.dir != "" {
			runOpts.dir = filepath.Join(opts.dir, fmt.Sprintf("%d-rows", rows))
		}
		if len(opts.rowSizes) > 1 {
			printBenchmarkRun(runIndex+1, len(opts.rowSizes), rows)
		}
		runBenchmark(runOpts)
	}
}

func runBenchmark(opts benchOptions) {
	cleanup := prepareDirectory(&opts)
	if cleanup != nil {
		defer cleanup()
	}

	tbl, loadElapsed, loadStats, segmentRows, segmentRowsSource := openOrLoad(&opts)
	results, groups, err := runBenchmarks(tbl, opts)
	if err != nil {
		log.Fatal(err)
	}

	codecStats, err := tbl.ColumnStorageStats()
	if err != nil {
		log.Fatal(err)
	}

	mode := "load"
	if opts.openExisting {
		mode = "open"
	}
	printSummary(opts.dir, mode, opts.rows, segmentRows, segmentRowsSource, opts.workers, opts.data, opts.seed, tbl.Segments())
	if !opts.openExisting {
		printLoadBenchmark(opts.rows, tbl.Bytes(), loadElapsed, loadStats)
	}
	printStorage(opts.rows, tbl.Bytes(), codecStats)
	printColumns(codecStats)
	printQueries(results)
	printGroups(groups)
}

func parseOptions() benchOptions {
	var opts benchOptions
	flag.Int64Var(&opts.rows, "rows", 0, "synthetic rows to load; omitted runs 100k, 500k, and 5M")
	flag.StringVar(&opts.dir, "dir", "", "table directory; defaults to a temporary directory")
	flag.BoolVar(&opts.keep, "keep", false, "keep the generated table directory")
	flag.Int64Var(&opts.tenant, "tenant", 7, "tenant_id value to count")
	flag.StringVar(&opts.event, "event", "checkout", "event_type value to count")
	flag.IntVar(&opts.runs, "runs", minBenchmarkRuns, "query repetitions after load/open; first run is reported separately; minimum 3")
	flag.IntVar(&opts.workers, "workers", 1, "parallel workers for count queries")
	flag.StringVar(&opts.data, "data", string(dataModeRandom), "synthetic data mode: random, structured, or skewed")
	flag.Uint64Var(&opts.seed, "seed", 1, "deterministic seed for random/skewed synthetic data")
	flag.BoolVar(&opts.openExisting, "open", false, "open an existing table directory and skip loading")
	flag.Parse()
	rowsSet := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "rows" {
			rowsSet = true
		}
	})
	if err := validateDataMode(opts.data); err != nil {
		log.Fatal(err)
	}

	switch {
	case rowsSet && opts.rows < 0:
		log.Fatalf("rows must be non-negative: %d", opts.rows)
	case opts.workers <= 0:
		log.Fatalf("workers must be positive: %d", opts.workers)
	case opts.openExisting && opts.dir == "":
		log.Fatal("-open requires -dir")
	}
	if opts.runs < minBenchmarkRuns {
		opts.runs = minBenchmarkRuns
	}
	if opts.openExisting || rowsSet {
		opts.rowSizes = []int64{opts.rows}
	} else {
		opts.rowSizes = append([]int64(nil), defaultBenchmarkRows...)
	}
	return opts
}

func prepareDirectory(opts *benchOptions) func() {
	if opts.dir != "" {
		return nil
	}
	tempDir, err := os.MkdirTemp("", "dripsqlbench-*")
	if err != nil {
		log.Fatal(err)
	}
	opts.dir = tempDir
	if opts.keep {
		return nil
	}
	return func() { _ = os.RemoveAll(tempDir) }
}

func openOrLoad(opts *benchOptions) (*table.Table, time.Duration, loadStats, int, string) {
	if opts.openExisting {
		tbl, err := table.Open(opts.dir)
		if err != nil {
			log.Fatal(err)
		}
		opts.rows = int64(tbl.Rows())
		return tbl, 0, loadStats{}, table.AverageSegmentRows(opts.rows, tbl.Segments()), "avg"
	}

	segmentRows := table.RecommendedSegmentRows(opts.rows)
	tbl, err := table.Create(opts.dir, benchmarkSchema())
	if err != nil {
		log.Fatal(err)
	}
	start := time.Now()
	loadStats, err := loadSyntheticEvents(tbl, opts.rows, segmentRows, opts.dataProfile(), newLoadProgress(opts.rows))
	if err != nil {
		log.Fatal(err)
	}
	return tbl, time.Since(start), loadStats, segmentRows, "auto"
}

func (opts benchOptions) dataProfile() dataProfile {
	return dataProfile{mode: dataMode(opts.data), seed: opts.seed}
}
