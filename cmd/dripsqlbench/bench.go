package main

import (
	"fmt"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/table"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

const (
	minQueryTiming    = 2 * time.Millisecond
	maxQueryTimingOps = 1_000_000
)

type benchResult struct {
	Name    string
	Count   int
	Samples int
	First   time.Duration
	Best    time.Duration
	Avg     time.Duration
	Stats   table.ScanStats
}

type groupResult struct {
	Name   string
	Counts map[string]int
}

type queryFunc func() (int, error)

type benchmarkRunner struct {
	scanner *table.Scanner
	runs    int
	results []benchResult
}

func runBenchmarks(tbl *table.Table, opts benchOptions) ([]benchResult, []groupResult, error) {
	scanner := tbl.NewScanner()
	defer scanner.Close()

	runner := benchmarkRunner{scanner: scanner, runs: opts.runs, results: make([]benchResult, 0, 10)}
	groups := make([]groupResult, 0, 2)
	schema := tbl.Schema()
	targetRow := int64(0)
	if opts.rows > 0 {
		targetRow = min(opts.rows-1, int64(12_345))
	}

	countInt64 := func(column string, value int64) (int, error) {
		if opts.workers > 1 {
			return scanner.CountInt64EqualParallel(column, value, opts.workers)
		}
		return scanner.CountInt64Equal(column, value)
	}
	countString := func(column string, value string) (int, error) {
		if opts.workers > 1 {
			return scanner.CountStringEqualParallel(column, value, opts.workers)
		}
		return scanner.CountStringEqual(column, value)
	}

	if hasColumn(schema, "tenant_id", vector.KindInt64) {
		if err := runner.run(fmt.Sprintf("tenant_id = %d", opts.tenant), func() (int, error) { return countInt64("tenant_id", opts.tenant) }); err != nil {
			return nil, nil, err
		}
		if err := runner.run("tenant_id = -1", func() (int, error) { return countInt64("tenant_id", -1) }); err != nil {
			return nil, nil, err
		}
	}
	if hasColumn(schema, "user_id", vector.KindInt64) {
		userID := syntheticUserID(targetRow)
		if err := runner.run(fmt.Sprintf("user_id = %d", userID), func() (int, error) { return countInt64("user_id", userID) }); err != nil {
			return nil, nil, err
		}
	}
	if hasColumn(schema, "created_at", vector.KindInt64) {
		createdAt := syntheticCreatedAt(targetRow)
		if err := runner.run(fmt.Sprintf("created_at = %d", createdAt), func() (int, error) { return countInt64("created_at", createdAt) }); err != nil {
			return nil, nil, err
		}
	}
	if hasColumn(schema, "event_type", vector.KindString) {
		if err := runner.run(fmt.Sprintf("event_type = %q", opts.event), func() (int, error) { return countString("event_type", opts.event) }); err != nil {
			return nil, nil, err
		}
		if err := runner.run("event_type = \"missing\"", func() (int, error) { return countString("event_type", "missing") }); err != nil {
			return nil, nil, err
		}
		eventGroups := make(map[string]int, 8)
		if err := runner.run("GROUP BY event_type", groupQuery(scanner, "event_type", eventGroups)); err != nil {
			return nil, nil, err
		}
		groups = append(groups, groupResult{Name: "event_type", Counts: eventGroups})
	}
	if hasColumn(schema, "country", vector.KindString) {
		if err := runner.run("country = \"US\"", func() (int, error) { return countString("country", "US") }); err != nil {
			return nil, nil, err
		}
		countryGroups := make(map[string]int, 16)
		if err := runner.run("GROUP BY country", groupQuery(scanner, "country", countryGroups)); err != nil {
			return nil, nil, err
		}
		groups = append(groups, groupResult{Name: "country", Counts: countryGroups})
	}
	if hasColumn(schema, "url", vector.KindString) {
		url := syntheticURLValue(targetRow)
		if err := runner.run(fmt.Sprintf("url = %q", url), func() (int, error) { return countString("url", url) }); err != nil {
			return nil, nil, err
		}
	}
	if hasColumn(schema, "email", vector.KindString) {
		email := syntheticEmailValue(targetRow)
		if err := runner.run(fmt.Sprintf("email = %q", email), func() (int, error) { return countString("email", email) }); err != nil {
			return nil, nil, err
		}
	}
	return runner.results, groups, nil
}

func groupQuery(scanner *table.Scanner, column string, counts map[string]int) queryFunc {
	return func() (int, error) {
		clear(counts)
		got, err := scanner.GroupStringCountsInto(column, counts)
		if err != nil {
			return 0, err
		}
		return len(got), nil
	}
}

func (r *benchmarkRunner) run(name string, fn queryFunc) error {
	result := benchResult{Name: name}
	var totalElapsed time.Duration
	var totalSamples int

	for run := range r.runs {
		count, elapsed, samples, err := measureQueryRun(fn)
		if err != nil {
			return err
		}
		if run == 0 {
			result.Count = count
			result.First = elapsed
			result.Best = elapsed
		} else if count != result.Count {
			return fmt.Errorf("run %d count %d does not match first count %d", run+1, count, result.Count)
		} else if elapsed < result.Best {
			result.Best = elapsed
		}
		totalElapsed += elapsed * time.Duration(samples)
		totalSamples += samples
	}

	result.Samples = totalSamples
	if totalSamples > 0 {
		result.Avg = totalElapsed / time.Duration(totalSamples)
	}
	result.Stats = r.scanner.Stats()
	r.results = append(r.results, result)
	return nil
}

func measureQueryRun(fn queryFunc) (int, time.Duration, int, error) {
	start := time.Now()
	count, err := fn()
	if err != nil {
		return 0, 0, 0, err
	}
	samples := 1
	elapsed := time.Since(start)
	for elapsed < minQueryTiming && samples < maxQueryTimingOps {
		next, err := fn()
		if err != nil {
			return 0, 0, 0, err
		}
		if next != count {
			return 0, 0, 0, fmt.Errorf("sample %d count %d does not match first count %d", samples+1, next, count)
		}
		samples++
		elapsed = time.Since(start)
	}
	perQuery := elapsed / time.Duration(samples)
	if perQuery == 0 && elapsed > 0 {
		perQuery = time.Nanosecond
	}
	return count, perQuery, samples, nil
}
