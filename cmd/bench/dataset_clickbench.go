// ClickBench-shaped synthetic dataset and query catalog entries.
// Uses a 9-column subset of the full hits schema with deterministic generation.
package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/engine"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

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
	if segmentRows <= 0 || segmentRows > types.StandardBatchRows {
		segmentRows = types.StandardBatchRows
	}
	urls := []string{"/", "/index", "/home", "/search", "/cart", "/product", "/about", "/contact"}
	phrases := []string{"", "buy", "sale", "review", "best", "cheap", "near me", "tutorial"}
	r := rand.New(rand.NewPCG(11, 13))
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
		b.WriteString("INSERT INTO hits (WatchID, UserID, EventTime, URL, Title, RegionID, SearchEngineID, AdvEngineID, SearchPhrase) VALUES ")
		for i := id; i < end; i++ {
			if i > id {
				b.WriteByte(',')
			}
			watch := int64(i)
			user := int64(r.IntN(1_000_000))
			event := int64(1_500_000_000 + r.IntN(86400*365))
			url := urls[r.IntN(len(urls))]
			title := fmt.Sprintf("Page %d", r.IntN(10000))
			region := int64(r.IntN(100))
			seid := int64(r.IntN(10))
			adv := int64(r.IntN(5))
			ph := phrases[r.IntN(len(phrases))]
			fmt.Fprintf(&b, "(%d, %d, %d, '%s', '%s', %d, %d, %d, '%s')",
				watch, user, event, url, title, region, seid, adv, ph)
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
