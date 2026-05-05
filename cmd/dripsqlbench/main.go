package main

import (
	"flag"
	"log"
	"os"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/table"
)

type benchOptions struct {
	rows         int64
	dir          string
	keep         bool
	tenant       int64
	event        string
	runs         int
	workers      int
	openExisting bool
}

func main() {
	opts := parseOptions()
	cleanup := prepareDirectory(&opts)
	if cleanup != nil {
		defer cleanup()
	}

	tbl, loadElapsed, segmentRows, segmentRowsSource := openOrLoad(&opts)
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
	printSummary(opts.dir, mode, opts.rows, segmentRows, segmentRowsSource, opts.workers, tbl.Segments())
	if !opts.openExisting {
		printLoadBenchmark(opts.rows, tbl.Bytes(), loadElapsed)
	}
	printStorage(opts.rows, tbl.Bytes(), codecStats)
	printColumns(codecStats)
	printQueries(results, opts.rows)
	printGroups(groups)
}

func parseOptions() benchOptions {
	var opts benchOptions
	flag.Int64Var(&opts.rows, "rows", 1_000_000, "synthetic rows to load")
	flag.StringVar(&opts.dir, "dir", "", "table directory; defaults to a temporary directory")
	flag.BoolVar(&opts.keep, "keep", false, "keep the generated table directory")
	flag.Int64Var(&opts.tenant, "tenant", 7, "tenant_id value to count")
	flag.StringVar(&opts.event, "event", "checkout", "event_type value to count")
	flag.IntVar(&opts.runs, "runs", 5, "query repetitions after load/open; first run is reported separately")
	flag.IntVar(&opts.workers, "workers", 1, "parallel workers for count queries")
	flag.BoolVar(&opts.openExisting, "open", false, "open an existing table directory and skip loading")
	flag.Parse()

	switch {
	case opts.rows < 0:
		log.Fatalf("rows must be non-negative: %d", opts.rows)
	case opts.runs <= 0:
		log.Fatalf("runs must be positive: %d", opts.runs)
	case opts.workers <= 0:
		log.Fatalf("workers must be positive: %d", opts.workers)
	case opts.openExisting && opts.dir == "":
		log.Fatal("-open requires -dir")
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

func openOrLoad(opts *benchOptions) (*table.Table, time.Duration, int, string) {
	if opts.openExisting {
		tbl, err := table.Open(opts.dir)
		if err != nil {
			log.Fatal(err)
		}
		opts.rows = int64(tbl.Rows())
		return tbl, 0, table.AverageSegmentRows(opts.rows, tbl.Segments()), "avg"
	}

	segmentRows := table.RecommendedSegmentRows(opts.rows)
	tbl, err := table.Create(opts.dir, benchmarkSchema())
	if err != nil {
		log.Fatal(err)
	}
	start := time.Now()
	if err := loadSyntheticEvents(tbl, opts.rows, segmentRows, newLoadProgress(opts.rows)); err != nil {
		log.Fatal(err)
	}
	return tbl, time.Since(start), segmentRows, "auto"
}
