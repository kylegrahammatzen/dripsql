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

type dataMode string

const (
	dataModeRandom     dataMode = "random"
	dataModeStructured dataMode = "structured"
	dataModeSkewed     dataMode = "skewed"
)

type dataProfile struct {
	mode dataMode
	seed uint64
}

type loadStats struct {
	Rows        int64
	Segments    int
	Generate    time.Duration
	Append      time.Duration
	Close       time.Duration
	MaxGenerate time.Duration
	MaxAppend   time.Duration
}

var (
	eventChoices     = []string{"signup", "checkout", "page_view", "cancel"}
	countryChoices   = []string{"US", "CA", "GB", "DE", "FR", "JP", "BR", "AU"}
	statusChoices    = []string{"ok", "retry", "error"}
	pathChoices      = []string{"/", "/pricing", "/docs", "/search", "/cart", "/checkout", "/account", "/support", "/blog", "/settings", "/api", "/download", "/products", "/teams", "/billing", "/logout"}
	userAgentChoices = []string{"Chrome/Windows", "Safari/iOS", "Firefox/Linux", "Edge/Windows", "Chrome/Android", "Safari/macOS"}
	payloadShapes    = []string{"id", "trace", "job"}
)

func validateDataMode(value string) error {
	switch dataMode(value) {
	case dataModeRandom, dataModeStructured, dataModeSkewed:
		return nil
	default:
		return fmt.Errorf("unsupported -data %q (want random, structured, or skewed)", value)
	}
}

func loadSyntheticEvents(tbl *table.Table, rows int64, segmentRows int, profile dataProfile, progress func(int64)) (loadStats, error) {
	appender, err := tbl.NewAppenderForRows(rows)
	if err != nil {
		return loadStats{}, err
	}

	var stats loadStats
	for start := int64(0); start < rows; start += int64(segmentRows) {
		count := segmentRows
		if remaining := rows - start; remaining < int64(count) {
			count = int(remaining)
		}
		generateStart := time.Now()
		batch, err := syntheticBatch(start, count, profile)
		generateElapsed := time.Since(generateStart)
		stats.Generate += generateElapsed
		if generateElapsed > stats.MaxGenerate {
			stats.MaxGenerate = generateElapsed
		}
		if err != nil {
			return stats, closeAppenderWithError(appender, "create batch", err)
		}
		appendStart := time.Now()
		if err := appender.Append(batch); err != nil {
			return stats, closeAppenderWithError(appender, "append batch", err)
		}
		appendElapsed := time.Since(appendStart)
		stats.Append += appendElapsed
		if appendElapsed > stats.MaxAppend {
			stats.MaxAppend = appendElapsed
		}
		stats.Rows += int64(count)
		stats.Segments++
		if progress != nil {
			progress(start + int64(count))
		}
	}
	closeStart := time.Now()
	err = appender.Close()
	stats.Close = time.Since(closeStart)
	return stats, err
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
	last    time.Time
	lastRow int64
	next    time.Time
	printed bool
}

func newLoadProgress(rows int64) func(int64) {
	if rows <= 0 {
		return nil
	}
	now := time.Now()
	progress := &loadProgress{rows: rows, started: now, last: now, next: now.Add(loadProgressInterval)}
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
	intervalRowsPerSec := 0.0
	intervalElapsed := now.Sub(p.last)
	if intervalElapsed > 0 && loaded >= p.lastRow {
		intervalRowsPerSec = float64(loaded-p.lastRow) / intervalElapsed.Seconds()
	} else if elapsed > 0 {
		intervalRowsPerSec = float64(loaded) / elapsed.Seconds()
	}
	p.last = now
	p.lastRow = loaded
	percent := float64(loaded) / float64(p.rows) * 100
	fmt.Fprintf(os.Stderr, "Load progress: %s/%s rows (%.1f%%, %s rows/sec)\n", commas(loaded), commas(p.rows), percent, commas(int64(intervalRowsPerSec)))
}

func syntheticBatch(start int64, count int, profile dataProfile) (vector.Batch, error) {
	cols := syntheticColumns(start, count, profile)
	urls, err := makeGeneratedStrings(start, count, profile, appendSyntheticURL)
	if err != nil {
		return vector.Batch{}, err
	}
	emails, err := makeGeneratedStrings(start, count, profile, appendSyntheticEmail)
	if err != nil {
		return vector.Batch{}, err
	}
	payloads, err := makeGeneratedStrings(start, count, profile, appendSyntheticPayload)
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

func syntheticColumns(start int64, count int, profile dataProfile) syntheticColumnData {
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
		cols.tenants[i] = syntheticTenantID(row, profile)
		cols.users[i] = syntheticUserID(row, profile)
		cols.createdAt[i] = syntheticCreatedAt(row, profile)
		cols.events[i] = syntheticEventType(row, profile)
		cols.countries[i] = syntheticCountry(row, profile)
		cols.statuses[i] = syntheticStatus(row, profile)
		cols.paths[i] = syntheticPath(row, profile)
		cols.userAgents[i] = syntheticUserAgent(row, profile)
	}
	return cols
}

func syntheticTenantID(row int64, profile dataProfile) int64 {
	if profile.mode == dataModeStructured {
		return row % 1024
	}
	if profile.mode == dataModeSkewed {
		return int64(weightedBucket(profile.hash(row, 0x10), []uint32{50, 20, 12, 8, 5, 3, 1, 1}))
	}
	return int64(profile.hash(row, 0x10) % 4096)
}

func syntheticUserID(row int64, profile dataProfile) int64 {
	if profile.mode == dataModeStructured {
		return (row * 13) % 10_000_000
	}
	modulus := uint64(50_000_000)
	if profile.mode == dataModeSkewed {
		modulus = 5_000_000
	}
	return int64(profile.hash(row, 0x20) % modulus)
}

func syntheticCreatedAt(row int64, profile dataProfile) int64 {
	if profile.mode == dataModeStructured {
		return 1_700_000_000 + row
	}
	jitter := int64(profile.hash(row, 0x30)%31) - 15
	if profile.mode == dataModeSkewed {
		jitter = int64(profile.hash(row, 0x30)%301) - 150
	}
	return 1_700_000_000 + row*60 + jitter
}

func syntheticEventType(row int64, profile dataProfile) string {
	if profile.mode == dataModeStructured {
		return eventChoices[row%int64(len(eventChoices))]
	}
	if profile.mode == dataModeSkewed {
		return chooseWeighted(profile.hash(row, 0x40), eventChoices, []uint32{45, 30, 20, 5})
	}
	return chooseWeighted(profile.hash(row, 0x40), eventChoices, []uint32{35, 30, 25, 10})
}

func syntheticCountry(row int64, profile dataProfile) string {
	if profile.mode == dataModeStructured {
		return countryChoices[(row/7)%int64(len(countryChoices))]
	}
	if profile.mode == dataModeSkewed {
		return chooseWeighted(profile.hash(row, 0x50), countryChoices, []uint32{38, 18, 14, 10, 8, 5, 4, 3})
	}
	return chooseWeighted(profile.hash(row, 0x50), countryChoices, []uint32{24, 18, 15, 13, 11, 8, 6, 5})
}

func syntheticStatus(row int64, profile dataProfile) string {
	if profile.mode == dataModeStructured {
		return statusChoices[(row/13)%int64(len(statusChoices))]
	}
	if profile.mode == dataModeSkewed {
		return chooseWeighted(profile.hash(row, 0x60), statusChoices, []uint32{92, 7, 1})
	}
	return chooseWeighted(profile.hash(row, 0x60), statusChoices, []uint32{80, 15, 5})
}

func syntheticPath(row int64, profile dataProfile) string {
	if profile.mode == dataModeStructured {
		return pathChoices[(row/17)%int64(len(pathChoices))]
	}
	if profile.mode == dataModeSkewed {
		return chooseWeighted(profile.hash(row, 0x70), pathChoices, []uint32{20, 16, 13, 10, 8, 7, 6, 5, 4, 3, 2, 2, 1, 1, 1, 1})
	}
	return chooseWeighted(profile.hash(row, 0x70), pathChoices, []uint32{14, 13, 11, 10, 9, 8, 7, 6, 5, 4, 3, 3, 2, 2, 2, 1})
}

func syntheticUserAgent(row int64, profile dataProfile) string {
	if profile.mode == dataModeStructured {
		return userAgentChoices[(row/19)%int64(len(userAgentChoices))]
	}
	if profile.mode == dataModeSkewed {
		return chooseWeighted(profile.hash(row, 0x80), userAgentChoices, []uint32{42, 24, 12, 9, 8, 5})
	}
	return chooseWeighted(profile.hash(row, 0x80), userAgentChoices, []uint32{34, 24, 16, 11, 9, 6})
}

func makeGeneratedStrings(start int64, count int, profile dataProfile, build func([]byte, int64, dataProfile) []byte) (vector.String, error) {
	data := make([]byte, 0, count*24)
	ranges := make([]uint64, count)
	for i := range count {
		row := start + int64(i)
		begin := len(data)
		data = build(data, row, profile)
		length := len(data) - begin
		if uint64(begin) > uint64(^uint32(0)) || uint64(length) > uint64(^uint32(0)) {
			return vector.String{}, fmt.Errorf("generated string column exceeds 32-bit string range")
		}
		ranges[i] = vector.StringRange(uint32(begin), uint32(length))
	}
	return vector.FromStringDataUnsafe(data, ranges), nil
}

func syntheticURLValue(row int64, profile dataProfile) string {
	return string(appendSyntheticURL(nil, row, profile))
}

func appendSyntheticURL(buf []byte, row int64, profile dataProfile) []byte {
	if profile.mode == dataModeRandom {
		prefix := chooseWeighted(profile.hash(row, 0x90), []string{"/item/", "/search?q=", "/docs/"}, []uint32{75, 15, 10})
		buf = append(buf, prefix...)
		return strconv.AppendInt(buf, int64(profile.hash(row, 0x91)%50_000_000), 36)
	}
	if profile.mode == dataModeSkewed {
		prefix := chooseWeighted(profile.hash(row, 0x90), []string{"/item/", "/search?q=", "/docs/", "/cart/"}, []uint32{70, 15, 10, 5})
		buf = append(buf, prefix...)
		return strconv.AppendInt(buf, int64(profile.hash(row, 0x91)%50_000_000), 36)
	}
	buf = append(buf, "/item/"...)
	if profile.mode == dataModeStructured {
		return strconv.AppendInt(buf, row%10_000_000, 36)
	}
	return strconv.AppendInt(buf, int64(profile.hash(row, 0x91)%50_000_000), 36)
}

func syntheticEmailValue(row int64, profile dataProfile) string {
	return string(appendSyntheticEmail(nil, row, profile))
}

func appendSyntheticEmail(buf []byte, row int64, profile dataProfile) []byte {
	buf = append(buf, 'u')
	if profile.mode == dataModeStructured {
		buf = strconv.AppendInt(buf, (row*17)%50_000_000, 36)
		return append(buf, "@drip.test"...)
	}
	buf = strconv.AppendInt(buf, int64(profile.hash(row, 0xa0)%100_000_000), 36)
	if profile.mode == dataModeRandom {
		domain := chooseWeighted(profile.hash(row, 0xa1), []string{"@drip.test", "@example.dev", "@mail.local"}, []uint32{80, 15, 5})
		return append(buf, domain...)
	}
	if profile.mode == dataModeSkewed {
		domain := chooseWeighted(profile.hash(row, 0xa1), []string{"@drip.test", "@example.dev", "@mail.local"}, []uint32{70, 20, 10})
		return append(buf, domain...)
	}
	return append(buf, "@drip.test"...)
}

func appendSyntheticPayload(buf []byte, row int64, profile dataProfile) []byte {
	if profile.mode == dataModeRandom {
		shape := chooseWeighted(profile.hash(row, 0xb0), payloadShapes, []uint32{80, 15, 5})
		buf = append(buf, shape...)
		buf = append(buf, '=')
		buf = strconv.AppendInt(buf, int64(profile.hash(row, 0xb1)%100_000_000), 36)
		buf = append(buf, ";bucket="...)
		return strconv.AppendInt(buf, int64(profile.hash(row, 0xb2)%997), 10)
	}
	if profile.mode == dataModeSkewed {
		shape := chooseWeighted(profile.hash(row, 0xb0), payloadShapes, []uint32{70, 20, 10})
		buf = append(buf, shape...)
		buf = append(buf, '=')
		buf = strconv.AppendInt(buf, int64(profile.hash(row, 0xb1)%100_000_000), 36)
		buf = append(buf, ";bucket="...)
		return strconv.AppendInt(buf, int64(profile.hash(row, 0xb2)%997), 10)
	}
	buf = append(buf, "id="...)
	if profile.mode == dataModeStructured {
		buf = strconv.AppendInt(buf, row%10_000_000, 36)
		buf = append(buf, ";bucket="...)
		return strconv.AppendInt(buf, row%97, 10)
	}
	buf = strconv.AppendInt(buf, int64(profile.hash(row, 0xb1)%100_000_000), 36)
	buf = append(buf, ";bucket="...)
	return strconv.AppendInt(buf, int64(profile.hash(row, 0xb2)%997), 10)
}

func chooseUniform(hash uint64, choices []string) string {
	return choices[hash%uint64(len(choices))]
}

func chooseWeighted(hash uint64, choices []string, weights []uint32) string {
	return choices[weightedBucket(hash, weights)]
}

func weightedBucket(hash uint64, weights []uint32) int {
	total := uint32(0)
	for _, weight := range weights {
		total += weight
	}
	if total == 0 {
		return 0
	}
	pick := uint32(hash % uint64(total))
	for i, weight := range weights {
		if pick < weight {
			return i
		}
		pick -= weight
	}
	return len(weights) - 1
}

func (p dataProfile) hash(row int64, salt uint64) uint64 {
	value := uint64(row) + salt + p.seed*0x9e3779b97f4a7c15
	return mix64(value)
}

func mix64(value uint64) uint64 {
	value ^= value >> 30
	value *= 0xbf58476d1ce4e5b9
	value ^= value >> 27
	value *= 0x94d049bb133111eb
	value ^= value >> 31
	return value
}
