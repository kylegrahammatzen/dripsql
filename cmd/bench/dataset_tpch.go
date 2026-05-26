// TPC-H-shaped synthetic dataset and query catalog entries built from a lineitem-only subset.
// Date columns are int64 days since 1992-01-01 to fit current types.
package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/engine"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// Days since 1992-01-01 for the date windows TPC-H Q1 and Q6 reference.
const (
	tpchDayQ1Cutoff = 2526 // 1998-12-01 minus 90d delta == 1998-09-02 - 1992-01-01
	tpchDayQ6Lo     = 731  // 1994-01-01 - 1992-01-01
	tpchDayQ6Hi     = 1096 // 1995-01-01 - 1992-01-01
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
	if segmentRows <= 0 || segmentRows > vector.StandardBatchRows {
		segmentRows = vector.StandardBatchRows
	}
	flags := []string{"A", "N", "R"}
	stats := []string{"F", "O"}
	r := rand.New(rand.NewPCG(42, 7))
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
		b.WriteString("INSERT INTO lineitem (l_orderkey, l_quantity, l_extprice, l_discount, l_disc_rev, l_disc_price, l_returnflag, l_linestatus, l_shipdate) VALUES ")
		for i := id; i < end; i++ {
			if i > id {
				b.WriteByte(',')
			}
			qty := int64(r.IntN(50) + 1)
			priceCents := int64(r.IntN(10_000_000) + 100)
			ext := float64(priceCents) / 100.0
			disc := float64(r.IntN(11)) / 100.0
			discRev := ext * (1.0 - disc)
			discPrice := ext * disc
			rf := flags[r.IntN(len(flags))]
			ls := stats[r.IntN(len(stats))]
			ship := int64(r.IntN(2557))
			fmt.Fprintf(&b, "(%d, %d, %.2f, %.2f, %.4f, %.4f, '%s', '%s', %d)",
				int64(i), qty, ext, disc, discRev, discPrice, rf, ls, ship)
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
