// Dataset generators and the query catalog. Each generator fills a fresh DB with a deterministic synthetic schema covering users, TPC-H lineitem, and ClickBench hits.
// Each query targets one schema and carries a name used as the benchstat label.
package main

import (
	"context"
	"fmt"
	"math/rand/v2"

	"github.com/kylegrahammatzen/dripsql/cmd/bench/ingest"
	"github.com/kylegrahammatzen/dripsql/internal/engine"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type dataset struct {
	name  string
	setup func(ctx context.Context, db *engine.DB, rows int, segmentRows int) error
}

type query struct {
	name    string
	dataset string
	sql     func(rows int) string
}

var datasets = map[string]dataset{
	"users":    {name: "users", setup: setupUsers},
	"lineitem": {name: "lineitem", setup: setupLineitem},
	"hits":     {name: "hits", setup: setupHits},
}

var queries = map[string]query{
	"count":            {name: "count", dataset: "users", sql: func(int) string { return "SELECT count(*) FROM users" }},
	"id_lookup":        {name: "id_lookup", dataset: "users", sql: func(rows int) string { return fmt.Sprintf("SELECT id, name FROM users WHERE id = %d", rows/2) }},
	"category_groupby": {name: "category_groupby", dataset: "users", sql: func(int) string { return "SELECT category, count(id), sum(age) FROM users GROUP BY category" }},
	"category_count":   {name: "category_count", dataset: "users", sql: func(int) string { return "SELECT category, count(*) FROM users GROUP BY category" }},
	"top_age":          {name: "top_age", dataset: "users", sql: func(int) string { return "SELECT id, age FROM users ORDER BY age DESC LIMIT 10" }},
	"cat_eq":           {name: "cat_eq", dataset: "users", sql: func(int) string { return "SELECT id FROM users WHERE category = 'alpha'" }},
	"age_eq":           {name: "age_eq", dataset: "users", sql: func(int) string { return "SELECT id FROM users WHERE age = 50" }},
	"select_star":      {name: "select_star", dataset: "users", sql: func(rows int) string { return fmt.Sprintf("SELECT * FROM users WHERE id < %d", rows/100) }},
	"price_sum":        {name: "price_sum", dataset: "users", sql: func(int) string { return "SELECT sum(price) FROM users" }},
	"price_avg":        {name: "price_avg", dataset: "users", sql: func(int) string { return "SELECT avg(price) FROM users" }},
	"tpch_q1": {name: "tpch_q1", dataset: "lineitem", sql: func(int) string {
		return fmt.Sprintf("SELECT l_returnflag, l_linestatus, sum(l_quantity) AS s_qty, sum(l_extprice) AS s_ext, sum(l_disc_rev) AS s_rev, count(*) AS c FROM lineitem WHERE l_shipdate <= %d GROUP BY l_returnflag, l_linestatus", tpchDayQ1Cutoff)
	}},
	"tpch_q1_expr": {name: "tpch_q1_expr", dataset: "lineitem", sql: func(int) string {
		return fmt.Sprintf("SELECT l_returnflag, l_linestatus, sum(l_quantity) AS s_qty, sum(l_extprice * (1.0 - l_discount)) AS s_rev, count(*) AS c FROM lineitem WHERE l_shipdate <= %d GROUP BY l_returnflag, l_linestatus", tpchDayQ1Cutoff)
	}},
	"tpch_q6": {name: "tpch_q6", dataset: "lineitem", sql: func(int) string {
		return fmt.Sprintf("SELECT sum(l_disc_price) FROM lineitem WHERE l_shipdate >= %d AND l_shipdate < %d AND l_discount >= 0.05 AND l_discount <= 0.07 AND l_quantity < 24", tpchDayQ6Lo, tpchDayQ6Hi)
	}},
	"clickbench_q1": {name: "clickbench_q1", dataset: "hits", sql: func(int) string { return "SELECT count(*) FROM hits" }},
	"clickbench_q4": {name: "clickbench_q4", dataset: "hits", sql: func(int) string { return "SELECT count(*) FROM hits WHERE AdvEngineID <> 0" }},
	"clickbench_q5": {name: "clickbench_q5", dataset: "hits", sql: func(int) string { return "SELECT count(*) FROM hits WHERE RegionID = 1" }},
	"clickbench_q7": {name: "clickbench_q7", dataset: "hits", sql: func(int) string {
		return "SELECT SearchEngineID, count(*) AS c FROM hits WHERE SearchEngineID <> 0 GROUP BY SearchEngineID ORDER BY c DESC LIMIT 5"
	}},
	"clickbench_q9": {name: "clickbench_q9", dataset: "hits", sql: func(int) string {
		return "SELECT RegionID, count(*) AS c FROM hits GROUP BY RegionID ORDER BY c DESC LIMIT 10"
	}},
}

// Each segment seals up to bulkPagesPerSegment pages of segmentRows rows via Ingest, and 128 pages of 2048 rows makes ~262k-row segments so scans amortize per-segment open and sidecar cost.
const bulkPagesPerSegment = 128

// seedTable drives the chunked seed loop shared by every dataset and calls fill once per page with the base row and row count.
func seedTable(ctx context.Context, db *engine.DB, table string, rows, segmentRows int, fill func(b *ingest.BatchBuilder, base, n int)) error {
	if segmentRows <= 0 || segmentRows > vector.StandardBatchRows {
		segmentRows = vector.StandardBatchRows
	}
	def, err := db.BoundTableByName(table)
	if err != nil {
		return err
	}
	builder := ingest.NewBatchBuilder(def)
	pending := make([]vector.Batch, 0, bulkPagesPerSegment)
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		if _, err := db.Ingest(ctx, engine.IngestConfig{Table: table, Batches: pending}); err != nil {
			return err
		}
		pending = pending[:0]
		return nil
	}
	for id := 0; id < rows; {
		end := min(id+segmentRows, rows)
		n := end - id
		builder.Reset(n)
		fill(builder, id, n)
		batch, err := builder.Build()
		if err != nil {
			return err
		}
		pending = append(pending, batch)
		if len(pending) >= bulkPagesPerSegment {
			if err := flush(); err != nil {
				return fmt.Errorf("seed flush ending at row %d: %w", end, err)
			}
		}
		id = end
	}
	return flush()
}

func setupUsers(ctx context.Context, db *engine.DB, rows int, segmentRows int) error {
	if _, err := db.Exec(ctx, "CREATE TABLE users (id int64 NOT NULL, name text NOT NULL, age int64 NOT NULL, category text NOT NULL, price float64 NOT NULL)"); err != nil {
		return err
	}
	cats := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	r := rand.New(rand.NewPCG(1, 2))
	return seedTable(ctx, db, "users", rows, segmentRows, func(b *ingest.BatchBuilder, base, n int) {
		ids := make([]int64, n)
		names := make([]string, n)
		ages := make([]int64, n)
		catsSlice := make([]string, n)
		prices := make([]float64, n)
		for i := range n {
			row := base + i
			cents := r.IntN(100000)
			ids[i] = int64(row)
			names[i] = fmt.Sprintf("user%d", row)
			ages[i] = int64(r.IntN(100))
			catsSlice[i] = cats[r.IntN(len(cats))]
			prices[i] = float64(cents) / 100.0
		}
		b.Int64("id", ids).Text("name", names).Int64("age", ages).Text("category", catsSlice).Float64("price", prices)
	})
}

// Days since 1992-01-01 for the date windows TPC-H Q1 and Q6 reference.
const (
	tpchDayQ1Cutoff = 2526
	tpchDayQ6Lo     = 731
	tpchDayQ6Hi     = 1096
)

func setupLineitem(ctx context.Context, db *engine.DB, rows int, segmentRows int) error {
	ddl := "CREATE TABLE lineitem (" +
		"l_orderkey int64 NOT NULL, " +
		"l_quantity int64 NOT NULL, " +
		"l_extprice float64 NOT NULL, " +
		"l_discount float64 NOT NULL, " +
		"l_disc_rev float64 NOT NULL, " +
		"l_disc_price float64 NOT NULL, " +
		"l_returnflag text NOT NULL, " +
		"l_linestatus text NOT NULL, " +
		"l_shipdate int64 NOT NULL)"
	if _, err := db.Exec(ctx, ddl); err != nil {
		return err
	}
	flags := []string{"A", "N", "R"}
	stats := []string{"F", "O"}
	r := rand.New(rand.NewPCG(42, 7))
	return seedTable(ctx, db, "lineitem", rows, segmentRows, func(b *ingest.BatchBuilder, base, n int) {
		orderkeys := make([]int64, n)
		quantities := make([]int64, n)
		extprices := make([]float64, n)
		discounts := make([]float64, n)
		discRevs := make([]float64, n)
		discPrices := make([]float64, n)
		returnFlags := make([]string, n)
		lineStatuses := make([]string, n)
		shipdates := make([]int64, n)
		for i := range n {
			row := base + i
			qty := int64(r.IntN(50) + 1)
			priceCents := int64(r.IntN(10_000_000) + 100)
			ext := float64(priceCents) / 100.0
			disc := float64(r.IntN(11)) / 100.0
			orderkeys[i] = int64(row)
			quantities[i] = qty
			extprices[i] = ext
			discounts[i] = disc
			discRevs[i] = ext * (1.0 - disc)
			discPrices[i] = ext * disc
			returnFlags[i] = flags[r.IntN(len(flags))]
			lineStatuses[i] = stats[r.IntN(len(stats))]
			shipdates[i] = int64(r.IntN(2557))
		}
		b.Int64("l_orderkey", orderkeys).
			Int64("l_quantity", quantities).
			Float64("l_extprice", extprices).
			Float64("l_discount", discounts).
			Float64("l_disc_rev", discRevs).
			Float64("l_disc_price", discPrices).
			Text("l_returnflag", returnFlags).
			Text("l_linestatus", lineStatuses).
			Int64("l_shipdate", shipdates)
	})
}

func setupHits(ctx context.Context, db *engine.DB, rows int, segmentRows int) error {
	ddl := "CREATE TABLE hits (" +
		"WatchID int64 NOT NULL, " +
		"UserID int64 NOT NULL, " +
		"EventTime int64 NOT NULL, " +
		"URL text NOT NULL, " +
		"Title text NOT NULL, " +
		"RegionID int64 NOT NULL, " +
		"SearchEngineID int64 NOT NULL, " +
		"AdvEngineID int64 NOT NULL, " +
		"SearchPhrase text NOT NULL)"
	if _, err := db.Exec(ctx, ddl); err != nil {
		return err
	}
	urls := []string{"/", "/index", "/home", "/search", "/cart", "/product", "/about", "/contact"}
	phrases := []string{"", "buy", "sale", "review", "best", "cheap", "near me", "tutorial"}
	r := rand.New(rand.NewPCG(11, 13))
	return seedTable(ctx, db, "hits", rows, segmentRows, func(b *ingest.BatchBuilder, base, n int) {
		watchIDs := make([]int64, n)
		userIDs := make([]int64, n)
		eventTimes := make([]int64, n)
		urlSlice := make([]string, n)
		titleSlice := make([]string, n)
		regionIDs := make([]int64, n)
		searchEngineIDs := make([]int64, n)
		advEngineIDs := make([]int64, n)
		searchPhrases := make([]string, n)
		for i := range n {
			watchIDs[i] = int64(base + i)
			userIDs[i] = int64(r.IntN(1_000_000))
			eventTimes[i] = int64(1_500_000_000 + r.IntN(86400*365))
			urlSlice[i] = urls[r.IntN(len(urls))]
			titleSlice[i] = fmt.Sprintf("Page %d", r.IntN(10000))
			regionIDs[i] = int64(r.IntN(100))
			searchEngineIDs[i] = int64(r.IntN(10))
			advEngineIDs[i] = int64(r.IntN(5))
			searchPhrases[i] = phrases[r.IntN(len(phrases))]
		}
		b.Int64("WatchID", watchIDs).
			Int64("UserID", userIDs).
			Int64("EventTime", eventTimes).
			Text("URL", urlSlice).
			Text("Title", titleSlice).
			Int64("RegionID", regionIDs).
			Int64("SearchEngineID", searchEngineIDs).
			Int64("AdvEngineID", advEngineIDs).
			Text("SearchPhrase", searchPhrases)
	})
}
