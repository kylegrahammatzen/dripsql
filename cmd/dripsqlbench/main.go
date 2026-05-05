package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"slices"
	"text/tabwriter"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/table"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type benchResult struct {
	Name  string
	Count int
	First time.Duration
	Best  time.Duration
	Avg   time.Duration
	Stats table.ScanStats
}

func main() {
	var (
		rows         int64
		segmentRows  int
		dir          string
		keep         bool
		tenant       int64
		event        string
		runs         int
		openExisting bool
	)

	flag.Int64Var(&rows, "rows", 1_000_000, "synthetic rows to load; set 100000000 for the 100M benchmark")
	flag.IntVar(&segmentRows, "segment-rows", 100_000, "rows per immutable segment")
	flag.StringVar(&dir, "dir", "", "table directory; defaults to a temporary directory")
	flag.BoolVar(&keep, "keep", false, "keep the generated table directory")
	flag.Int64Var(&tenant, "tenant", 7, "tenant_id value to count")
	flag.StringVar(&event, "event", "checkout", "event_type value to count")
	flag.IntVar(&runs, "runs", 5, "query repetitions after load/open; first run is reported separately")
	flag.BoolVar(&openExisting, "open", false, "open an existing table directory and skip loading")
	flag.Parse()

	switch {
	case rows < 0:
		log.Fatalf("rows must be non-negative: %d", rows)
	case segmentRows <= 0:
		log.Fatalf("segment-rows must be positive: %d", segmentRows)
	case runs <= 0:
		log.Fatalf("runs must be positive: %d", runs)
	case openExisting && dir == "":
		log.Fatal("-open requires -dir")
	}

	if dir == "" {
		tempDir, err := os.MkdirTemp("", "dripsqlbench-*")
		if err != nil {
			log.Fatal(err)
		}

		dir = tempDir

		if !keep {
			defer os.RemoveAll(dir)
		}
	}

	var (
		tbl         *table.Table
		loadElapsed time.Duration
		err         error
	)

	if openExisting {
		tbl, err = table.Open(dir)
		if err != nil {
			log.Fatal(err)
		}

		rows = int64(tbl.Rows())
	} else {
		tbl, err = table.Create(dir, []table.Column{
			{Name: "tenant_id", Kind: vector.KindInt64},
			{Name: "event_type", Kind: vector.KindString},
		})
		if err != nil {
			log.Fatal(err)
		}

		start := time.Now()

		if err := loadSyntheticEvents(tbl, rows, segmentRows); err != nil {
			log.Fatal(err)
		}

		loadElapsed = time.Since(start)
	}

	scanner := tbl.NewScanner()
	defer scanner.Close()
	results := make([]benchResult, 0, 3)

	bench := func(name string, fn func() (int, error)) {
		var result benchResult
		var total time.Duration

		result.Name = name

		for run := range runs {
			start := time.Now()

			count, err := fn()
			if err != nil {
				log.Fatal(err)
			}

			elapsed := time.Since(start)

			if run == 0 {
				result.Count = count
				result.First = elapsed
				result.Best = elapsed
			} else {
				if count != result.Count {
					log.Fatalf("run %d count %d does not match first count %d", run+1, count, result.Count)
				}

				if elapsed < result.Best {
					result.Best = elapsed
				}
			}

			total += elapsed
		}

		result.Avg = total / time.Duration(runs)
		result.Stats = scanner.Stats()

		results = append(results, result)
	}

	bench(fmt.Sprintf("tenant_id = %d", tenant), func() (int, error) {
		return scanner.CountInt64Equal("tenant_id", tenant)
	})

	bench(fmt.Sprintf("event_type = %q", event), func() (int, error) {
		return scanner.CountStringEqual("event_type", event)
	})

	groups := make(map[string]int, 8)

	bench("GROUP BY event_type", func() (int, error) {
		clear(groups)

		counts, err := scanner.GroupStringCountsInto("event_type", groups)
		if err != nil {
			return 0, err
		}

		return len(counts), nil
	})

	mode := "load"
	if openExisting {
		mode = "open"
	}

	printSummary(dir, mode, rows, segmentRows, tbl.Bytes(), tbl.Segments(), loadElapsed, openExisting)
	printQueries(results, rows)
	printGroups(groups)
	printScanStats(results)
}

func loadSyntheticEvents(tbl *table.Table, rows int64, segmentRows int) error {
	choices := []string{"signup", "checkout", "page_view", "cancel"}
	appender, err := tbl.NewAppender()
	if err != nil {
		return err
	}

	for start := int64(0); start < rows; start += int64(segmentRows) {
		count := segmentRows
		if remaining := rows - start; remaining < int64(count) {
			count = int(remaining)
		}

		tenants := make([]int64, count)
		events := make([]string, count)

		for i := range count {
			row := start + int64(i)

			tenants[i] = row % 1024
			events[i] = choices[row%int64(len(choices))]
		}

		batch, err := vector.NewBatch(
			vector.Column{Name: "tenant_id", Vector: vector.FromInt64(tenants)},
			vector.Column{Name: "event_type", Vector: vector.FromString(events)},
		)
		if err != nil {
			if closeErr := appender.Close(); closeErr != nil {
				return fmt.Errorf("create batch: %w; close appender: %v", err, closeErr)
			}
			return err
		}

		if err := appender.Append(batch); err != nil {
			if closeErr := appender.Close(); closeErr != nil {
				return fmt.Errorf("append batch: %w; close appender: %v", err, closeErr)
			}
			return err
		}
	}

	return appender.Close()
}

func printSummary(
	dir string,
	mode string,
	rows int64,
	segmentRows int,
	tableBytes int64,
	segments int,
	loadElapsed time.Duration,
	openExisting bool,
) {
	const mib = 1024 * 1024

	bytesPerRow := 0.0
	if rows > 0 {
		bytesPerRow = float64(tableBytes) / float64(rows)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	fmt.Fprintln(w, "DripSQL Benchmark")
	fmt.Fprintln(w)
	fmt.Fprintf(w, "Directory:\t%s\n", dir)
	fmt.Fprintf(w, "Mode:\t%s\n", mode)
	fmt.Fprintf(w, "Rows:\t%s\n", commas(rows))
	fmt.Fprintf(w, "Segments:\t%s\n", commas(segments))
	fmt.Fprintf(w, "Segment rows:\t%s\n", commas(segmentRows))
	fmt.Fprintf(w, "Table size:\t%.2f MiB\n", float64(tableBytes)/mib)
	fmt.Fprintf(w, "Bytes / row:\t%.2f\n", bytesPerRow)

	if !openExisting {
		rowsPerSec := 0.0
		if loadElapsed > 0 {
			rowsPerSec = float64(rows) / loadElapsed.Seconds()
		}

		fmt.Fprintln(w)
		fmt.Fprintln(w, "Load")
		fmt.Fprintf(w, "Elapsed:\t%s\n", loadElapsed)
		fmt.Fprintf(w, "Throughput:\t%s rows/sec\n", commas(int64(rowsPerSec)))
	}

	w.Flush()
}

func printQueries(results []benchResult, rows int64) {
	const mib = 1024 * 1024

	fmt.Println()
	fmt.Println("Queries")

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	fmt.Fprintln(w, "Query\tResult\tFirst\tBest\tAvg\tRows/sec\tScan")

	for _, result := range results {
		rowsScanned := result.Stats.RowsScanned
		if rowsScanned == 0 && result.Stats.RowsSkipped == 0 {
			rowsScanned = rows
		}

		rowsPerSec := 0.0
		if result.Avg > 0 {
			rowsPerSec = float64(rowsScanned) / result.Avg.Seconds()
		}

		fmt.Fprintf(
			w,
			"%s\t%s\t%s\t%s\t%s\t%s\t%.2f MiB\n",
			result.Name,
			commas(result.Count),
			formatDuration(result.First),
			formatDuration(result.Best),
			formatDuration(result.Avg),
			commas(int64(rowsPerSec)),
			float64(result.Stats.BytesScanned)/mib,
		)
	}

	w.Flush()
}

func formatDuration(duration time.Duration) string {
	if duration == 0 {
		return "<1us"
	}
	return duration.String()
}

func printGroups(groups map[string]int) {
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}

	slices.Sort(keys)

	fmt.Println()
	fmt.Println("Group Results")

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	for _, key := range keys {
		fmt.Fprintf(w, "%s:\t%s\n", key, commas(groups[key]))
	}

	w.Flush()
}

func printScanStats(results []benchResult) {
	fmt.Println()
	fmt.Println("Scan Stats")

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	fmt.Fprintln(w, "Query\tSegments\tRows scanned\tRows skipped")

	for _, result := range results {
		fmt.Fprintf(
			w,
			"%s\t%d/%d\t%s\t%s\n",
			result.Name,
			result.Stats.SegmentsScanned,
			result.Stats.SegmentsTotal,
			commas(result.Stats.RowsScanned),
			commas(result.Stats.RowsSkipped),
		)
	}

	w.Flush()
}

func commas[T ~int | ~int64](n T) string {
	s := fmt.Sprint(n)

	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}

	return s
}
