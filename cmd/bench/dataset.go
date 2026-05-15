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
	"users": {name: "users", setup: setupUsers},
}

var queries = map[string]query{
	"count":            {name: "count", dataset: "users", sql: func(int) string { return "SELECT count(*) FROM users" }},
	"id_lookup":        {name: "id_lookup", dataset: "users", sql: func(rows int) string { return fmt.Sprintf("SELECT id, name FROM users WHERE id = %d", rows/2) }},
	"category_groupby": {name: "category_groupby", dataset: "users", sql: func(int) string { return "SELECT category, count(id), sum(age) FROM users GROUP BY category" }},
	"top_age":          {name: "top_age", dataset: "users", sql: func(int) string { return "SELECT id, age FROM users ORDER BY age DESC LIMIT 10" }},
}

func setupUsers(ctx context.Context, db *engine.DB, rows int, segmentRows int) error {
	if _, err := db.Exec(ctx, "CREATE TABLE users (id int64 NOT NULL, name text NOT NULL, age int64 NOT NULL, category text NOT NULL)"); err != nil {
		return err
	}
	if segmentRows <= 0 || segmentRows > types.StandardBatchRows {
		segmentRows = types.StandardBatchRows
	}
	cats := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	r := rand.New(rand.NewPCG(1, 2))
	id := 0
	for id < rows {
		end := id + segmentRows
		if end > rows {
			end = rows
		}
		var b strings.Builder
		b.WriteString("INSERT INTO users (id, name, age, category) VALUES ")
		for i := id; i < end; i++ {
			if i > id {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, "(%d, 'user%d', %d, '%s')", i, i, r.IntN(100), cats[r.IntN(len(cats))])
		}
		if _, err := db.Exec(ctx, b.String()); err != nil {
			return fmt.Errorf("seed batch %d-%d: %w", id, end, err)
		}
		id = end
	}
	return nil
}
