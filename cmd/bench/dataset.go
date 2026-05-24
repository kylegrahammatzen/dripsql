// Dataset generators and query catalog. Each generator fills a fresh DB with a deterministic synthetic schema.
// Each query targets one schema and carries a name used as the benchstat label.
package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/engine"
	"github.com/kylegrahammatzen/dripsql/internal/types"
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

// Per-segment page target. Each segment is built from up to bulkPagesPerSegment
// INSERTs of segmentRows rows each, sealed via BulkInsert into one multi-page file.
const bulkPagesPerSegment = 16

func setupUsers(ctx context.Context, db *engine.DB, rows int, segmentRows int) error {
	if _, err := db.Exec(ctx, "CREATE TABLE users (id int64 NOT NULL, name text NOT NULL, age int64 NOT NULL, category text NOT NULL, price float64 NOT NULL)"); err != nil {
		return err
	}
	if segmentRows <= 0 || segmentRows > types.StandardBatchRows {
		segmentRows = types.StandardBatchRows
	}
	cats := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	r := rand.New(rand.NewPCG(1, 2))
	id := 0
	pending := make([]string, 0, bulkPagesPerSegment)
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		if _, err := db.BulkInsert(ctx, pending); err != nil {
			return err
		}
		pending = pending[:0]
		return nil
	}
	for id < rows {
		end := min(id+segmentRows, rows)
		var b strings.Builder
		b.WriteString("INSERT INTO users (id, name, age, category, price) VALUES ")
		for i := id; i < end; i++ {
			if i > id {
				b.WriteByte(',')
			}
			cents := r.IntN(100000)
			fmt.Fprintf(&b, "(%d, 'user%d', %d, '%s', %d.%02d)", i, i, r.IntN(100), cats[r.IntN(len(cats))], cents/100, cents%100)
		}
		pending = append(pending, b.String())
		if len(pending) >= bulkPagesPerSegment {
			if err := flush(); err != nil {
				return fmt.Errorf("seed flush ending at row %d: %w", end, err)
			}
		}
		id = end
	}
	return flush()
}
