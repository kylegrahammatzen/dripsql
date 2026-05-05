package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/table"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

const loadProgressInterval = time.Second

var (
	eventChoices     = []string{"signup", "checkout", "page_view", "cancel"}
	countryChoices   = []string{"US", "CA", "GB", "DE", "FR", "JP", "BR", "AU"}
	statusChoices    = []string{"ok", "retry", "error"}
	pathChoices      = []string{"/", "/pricing", "/docs", "/search", "/cart", "/checkout", "/account", "/support", "/blog", "/settings", "/api", "/download", "/products", "/teams", "/billing", "/logout"}
	userAgentChoices = []string{"Chrome/Windows", "Safari/iOS", "Firefox/Linux", "Edge/Windows", "Chrome/Android", "Safari/macOS"}
)

func loadSyntheticEvents(tbl *table.Table, rows int64, segmentRows int, progress func(int64)) error {
	appender, err := tbl.NewAppenderForRows(rows)
	if err != nil {
		return err
	}

	for start := int64(0); start < rows; start += int64(segmentRows) {
		count := segmentRows
		if remaining := rows - start; remaining < int64(count) {
			count = int(remaining)
		}
		batch, err := syntheticBatch(start, count)
		if err != nil {
			return closeAppenderWithError(appender, "create batch", err)
		}
		if err := appender.Append(batch); err != nil {
			return closeAppenderWithError(appender, "append batch", err)
		}
		if progress != nil {
			progress(start + int64(count))
		}
	}
	return appender.Close()
}

func closeAppenderWithError(appender *table.Appender, action string, err error) error {
	if closeErr := appender.Close(); closeErr != nil {
		return fmt.Errorf("%s: %w; close appender: %v", action, err, closeErr)
	}
	return err
}

type loadProgress struct {
	rows    int64
	started time.Time
	next    time.Time
	printed bool
}

func newLoadProgress(rows int64) func(int64) {
	if rows <= 0 {
		return nil
	}
	now := time.Now()
	progress := &loadProgress{rows: rows, started: now, next: now.Add(loadProgressInterval)}
	return progress.update
}

func (p *loadProgress) update(loaded int64) {
	now := time.Now()
	done := loaded >= p.rows
	elapsed := now.Sub(p.started)
	if !done && now.Before(p.next) {
		return
	}
	if done && !p.printed && elapsed < loadProgressInterval {
		return
	}

	p.printed = true
	p.next = now.Add(loadProgressInterval)
	rowsPerSec := 0.0
	if elapsed > 0 {
		rowsPerSec = float64(loaded) / elapsed.Seconds()
	}
	percent := float64(loaded) / float64(p.rows) * 100
	fmt.Fprintf(os.Stderr, "Load progress: %s/%s rows (%.1f%%, %s rows/sec)\n", commas(loaded), commas(p.rows), percent, commas(int64(rowsPerSec)))
}

func syntheticBatch(start int64, count int) (vector.Batch, error) {
	cols := syntheticColumns(start, count)
	urls, err := makeGeneratedStrings(start, count, appendSyntheticURL)
	if err != nil {
		return vector.Batch{}, err
	}
	emails, err := makeGeneratedStrings(start, count, appendSyntheticEmail)
	if err != nil {
		return vector.Batch{}, err
	}
	payloads, err := makeGeneratedStrings(start, count, appendSyntheticPayload)
	if err != nil {
		return vector.Batch{}, err
	}

	return vector.NewBatch(
		vector.Column{Name: "tenant_id", Vector: vector.FromInt64(cols.tenants)},
		vector.Column{Name: "user_id", Vector: vector.FromInt64(cols.users)},
		vector.Column{Name: "created_at", Vector: vector.FromInt64(cols.createdAt)},
		vector.Column{Name: "event_type", Vector: vector.FromString(cols.events)},
		vector.Column{Name: "country", Vector: vector.FromString(cols.countries)},
		vector.Column{Name: "status", Vector: vector.FromString(cols.statuses)},
		vector.Column{Name: "path", Vector: vector.FromString(cols.paths)},
		vector.Column{Name: "user_agent", Vector: vector.FromString(cols.userAgents)},
		vector.Column{Name: "url", Vector: urls},
		vector.Column{Name: "email", Vector: emails},
		vector.Column{Name: "payload", Vector: payloads},
	)
}

type syntheticColumnData struct {
	tenants    []int64
	users      []int64
	createdAt  []int64
	events     []string
	countries  []string
	statuses   []string
	paths      []string
	userAgents []string
}

func syntheticColumns(start int64, count int) syntheticColumnData {
	cols := syntheticColumnData{
		tenants:    make([]int64, count),
		users:      make([]int64, count),
		createdAt:  make([]int64, count),
		events:     make([]string, count),
		countries:  make([]string, count),
		statuses:   make([]string, count),
		paths:      make([]string, count),
		userAgents: make([]string, count),
	}
	for i := range count {
		row := start + int64(i)
		cols.tenants[i] = row % 1024
		cols.users[i] = syntheticUserID(row)
		cols.createdAt[i] = syntheticCreatedAt(row)
		cols.events[i] = eventChoices[row%int64(len(eventChoices))]
		cols.countries[i] = countryChoices[(row/7)%int64(len(countryChoices))]
		cols.statuses[i] = statusChoices[(row/13)%int64(len(statusChoices))]
		cols.paths[i] = pathChoices[(row/17)%int64(len(pathChoices))]
		cols.userAgents[i] = userAgentChoices[(row/19)%int64(len(userAgentChoices))]
	}
	return cols
}

func syntheticUserID(row int64) int64 {
	return (row * 13) % 10_000_000
}

func syntheticCreatedAt(row int64) int64 {
	return 1_700_000_000 + row
}

func makeGeneratedStrings(start int64, count int, build func([]byte, int64) []byte) (vector.String, error) {
	data := make([]byte, 0, count*24)
	ranges := make([]uint64, count)
	for i := range count {
		row := start + int64(i)
		begin := len(data)
		data = build(data, row)
		length := len(data) - begin
		if uint64(begin) > uint64(^uint32(0)) || uint64(length) > uint64(^uint32(0)) {
			return vector.String{}, fmt.Errorf("generated string column exceeds 32-bit string range")
		}
		ranges[i] = vector.StringRange(uint32(begin), uint32(length))
	}
	return vector.FromStringDataUnsafe(data, ranges), nil
}

func syntheticURLValue(row int64) string {
	return string(appendSyntheticURL(nil, row))
}

func appendSyntheticURL(buf []byte, row int64) []byte {
	buf = append(buf, "/item/"...)
	return strconv.AppendInt(buf, row%10_000_000, 36)
}

func syntheticEmailValue(row int64) string {
	return string(appendSyntheticEmail(nil, row))
}

func appendSyntheticEmail(buf []byte, row int64) []byte {
	buf = append(buf, 'u')
	buf = strconv.AppendInt(buf, (row*17)%50_000_000, 36)
	return append(buf, "@drip.test"...)
}

func appendSyntheticPayload(buf []byte, row int64) []byte {
	buf = append(buf, "id="...)
	buf = strconv.AppendInt(buf, row%10_000_000, 36)
	buf = append(buf, ";bucket="...)
	return strconv.AppendInt(buf, row%97, 10)
}
