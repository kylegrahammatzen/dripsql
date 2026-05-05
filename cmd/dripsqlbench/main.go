package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/table"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func main() {
	var rows int64
	var segmentRows int
	var dir string
	var keep bool
	var tenant int64
	var event string

	flag.Int64Var(&rows, "rows", 1_000_000, "synthetic rows to load; set 100000000 for the 100M benchmark")
	flag.IntVar(&segmentRows, "segment-rows", 100_000, "rows per immutable segment")
	flag.StringVar(&dir, "dir", "", "table directory; defaults to a temporary directory")
	flag.BoolVar(&keep, "keep", false, "keep the generated table directory")
	flag.Int64Var(&tenant, "tenant", 7, "tenant_id value to count")
	flag.StringVar(&event, "event", "checkout", "event_type value to count")
	flag.Parse()

	if rows < 0 {
		log.Fatalf("rows must be non-negative: %d", rows)
	}
	if segmentRows <= 0 {
		log.Fatalf("segment-rows must be positive: %d", segmentRows)
	}

	if dir == "" {
		var err error
		dir, err = os.MkdirTemp("", "dripsqlbench-*")
		if err != nil {
			log.Fatal(err)
		}
		if !keep {
			defer os.RemoveAll(dir)
		}
	}

	schema := []table.Column{
		{Name: "tenant_id", Kind: vector.KindInt64},
		{Name: "event_type", Kind: vector.KindString},
	}
	tbl, err := table.Create(dir, schema)
	if err != nil {
		log.Fatal(err)
	}

	loadStart := time.Now()
	if err := loadSyntheticEvents(tbl, rows, segmentRows); err != nil {
		log.Fatal(err)
	}
	loadElapsed := time.Since(loadStart)

	scanner := tbl.NewScanner()
	tenantStart := time.Now()
	tenantCount, err := scanner.CountInt64Equal("tenant_id", tenant)
	if err != nil {
		log.Fatal(err)
	}
	tenantElapsed := time.Since(tenantStart)

	eventStart := time.Now()
	eventCount, err := scanner.CountStringEqual("event_type", event)
	if err != nil {
		log.Fatal(err)
	}
	eventElapsed := time.Since(eventStart)

	groupStart := time.Now()
	groups, err := scanner.GroupStringCounts("event_type")
	if err != nil {
		log.Fatal(err)
	}
	groupElapsed := time.Since(groupStart)

	fmt.Printf("dir=%s\n", dir)
	fmt.Printf("rows=%d segments=%d segment_rows=%d\n", rows, tbl.Segments(), segmentRows)
	fmt.Printf("load=%s rows_per_sec=%.0f\n", loadElapsed, perSecond(rows, loadElapsed))
	fmt.Printf("count tenant_id=%d count=%d elapsed=%s rows_per_sec=%.0f\n", tenant, tenantCount, tenantElapsed, perSecond(rows, tenantElapsed))
	fmt.Printf("count event_type=%q count=%d elapsed=%s rows_per_sec=%.0f\n", event, eventCount, eventElapsed, perSecond(rows, eventElapsed))
	fmt.Printf("group event_type groups=%v elapsed=%s rows_per_sec=%.0f\n", groups, groupElapsed, perSecond(rows, groupElapsed))
}

func loadSyntheticEvents(tbl *table.Table, rows int64, segmentRows int) error {
	choices := []string{"signup", "checkout", "page_view", "cancel"}
	for start := int64(0); start < rows; start += int64(segmentRows) {
		count := segmentRows
		if remaining := rows - start; remaining < int64(count) {
			count = int(remaining)
		}
		tenants := make([]int64, count)
		events := make([]string, count)
		for row := range count {
			global := start + int64(row)
			tenants[row] = global % 1024
			events[row] = choices[global%int64(len(choices))]
		}

		batch, err := vector.NewBatch(
			vector.Column{Name: "tenant_id", Vector: vector.FromInt64(tenants)},
			vector.Column{Name: "event_type", Vector: vector.FromString(events)},
		)
		if err != nil {
			return err
		}
		if err := tbl.Append(batch); err != nil {
			return err
		}
	}
	return nil
}

func perSecond(rows int64, elapsed time.Duration) float64 {
	if elapsed <= 0 {
		return 0
	}
	return float64(rows) / elapsed.Seconds()
}
