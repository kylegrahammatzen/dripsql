package main

import (
	"context"
	"fmt"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/engine"
	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

var schemaColumns = []schema.ColumnSpec{
	{Name: "tenant_id", Type: sqltype.Int64},
	{Name: "user_id", Type: sqltype.Int64},
	{Name: "amount", Type: sqltype.Int64},
	{Name: "event_type", Type: sqltype.Text},
	{Name: "country", Type: sqltype.Text},
}

func createSchema(ctx context.Context, db *engine.DB) error {
	return db.CreateTable(ctx, schema.TableSpec{
		Name:        "events",
		IfNotExists: true,
		Columns:     schemaColumns,
		Options:     schema.TableOptions{Storage: schema.StorageColumnar},
	})
}

type loadStats struct {
	Rows        int64
	Segments    int
	Elapsed     time.Duration
	GenerateNs  int64
	AppendNs    int64
	BytesPerRow float64
	BytesTotal  int64
}

// rowBuilder fills batch column slices for rows in [start, start+n). Each
// profile (structured, random, skewed) is one rowBuilder; the caller turns
// the slices into a vector.Batch.
type rowBuilder func(start int64, n int) batchData

type batchData struct {
	tenant  []int64
	user    []int64
	amount  []int64
	event   []string
	country []string
}

// load runs the load phase for one profile. Each profile differs only in its
// rowBuilder; load timing, segment publication, and stats accounting are
// shared.
func load(ctx context.Context, store *storage.Store, def catalog.TableDef, rows int64, builder rowBuilder) (loadStats, error) {
	stats := loadStats{Rows: rows}
	const batchRows = int64(vector.StandardBatchRows)
	pos := int64(0)
	for pos < rows {
		n := batchRows
		if pos+n > rows {
			n = rows - pos
		}
		genStart := time.Now()
		data := builder(pos, int(n))
		batch, err := buildBatch(data, int(n))
		if err != nil {
			return loadStats{}, err
		}
		stats.GenerateNs += time.Since(genStart).Nanoseconds()

		appendStart := time.Now()
		if err := store.AppendBuffered(ctx, def, batch); err != nil {
			return loadStats{}, err
		}
		stats.AppendNs += time.Since(appendStart).Nanoseconds()
		pos += n
	}
	return stats, nil
}

// builderFor returns the row builder for one of the supported profile names.
func builderFor(profile string) (rowBuilder, error) {
	switch profile {
	case "structured":
		return structuredBuilder, nil
	case "random":
		return randomBuilder, nil
	case "skewed":
		return skewedBuilder, nil
	}
	return nil, fmt.Errorf("unknown profile %q", profile)
}

// structuredBuilder uses deterministic modular arithmetic so every segment
// contains every distinct value (tenant_id 1..1024, event_type/country
// rotated). Good for measuring metadata-prune fast-paths on absent values
// and stable, predictable storage layouts; bad for showing segment-level
// min/max prune on matched values because no segment can be excluded.
func structuredBuilder(start int64, n int) batchData {
	d := newBatchData(n)
	for i := range n {
		row := start + int64(i)
		d.tenant[i] = (row % 1024) + 1
		d.user[i] = row + 1
		d.amount[i] = row % 100
		d.event[i] = eventTypes[row%int64(len(eventTypes))]
		d.country[i] = countries[row%int64(len(countries))]
	}
	return d
}

// randomBuilder generates deterministic seeded uniform-random data via
// maphash. tenant_id covers 1..4096 uniformly, user_id covers a 50M-row
// range, and event_type / country are uniform across their dictionaries.
// Reproducible across runs because every value is a pure function of (row,
// column salt).
func randomBuilder(start int64, n int) batchData {
	d := newBatchData(n)
	for i := range n {
		row := start + int64(i)
		d.tenant[i] = int64(mix64(row, saltTenant)%4096) + 1
		d.user[i] = int64(mix64(row, saltUser) % 50_000_000)
		d.amount[i] = int64(mix64(row, saltAmount) % 1000)
		d.event[i] = eventTypes[mix64(row, saltEvent)%uint64(len(eventTypes))]
		d.country[i] = countries[mix64(row, saltCountry)%uint64(len(countries))]
	}
	return d
}

// skewedBuilder produces a heavy-tailed distribution: 80% of rows hit a
// small range of tenants (1..50), 'checkout' is the dominant event_type
// (50%), and US dominates country (35%). user_id stays high-cardinality.
// This is the profile most representative of real event-analytics data and
// the one where dictionary encoding has the most measurable win.
func skewedBuilder(start int64, n int) batchData {
	d := newBatchData(n)
	for i := range n {
		row := start + int64(i)
		r := mix64(row, saltTenant) % 100
		switch {
		case r < 80:
			d.tenant[i] = int64(mix64(row, saltTenant+1)%50) + 1
		case r < 95:
			d.tenant[i] = int64(mix64(row, saltTenant+2)%500) + 51
		default:
			d.tenant[i] = int64(mix64(row, saltTenant+3)%3500) + 551
		}
		d.user[i] = int64(mix64(row, saltUser) % 5_000_000)
		d.amount[i] = int64(mix64(row, saltAmount) % 1000)
		d.event[i] = pickWeighted(row, saltEvent, eventTypesSkewed)
		d.country[i] = pickWeighted(row, saltCountry, countriesSkewed)
	}
	return d
}

func pickWeighted(row int64, salt uint64, weighted []weightedString) string {
	r := mix64(row, salt) % 100
	var acc uint64
	for _, w := range weighted {
		acc += w.weight
		if r < acc {
			return w.value
		}
	}
	return weighted[len(weighted)-1].value
}

type weightedString struct {
	value  string
	weight uint64
}

var eventTypesSkewed = []weightedString{
	{value: "checkout", weight: 50},
	{value: "page_view", weight: 30},
	{value: "signup", weight: 15},
	{value: "cancel", weight: 5},
}

var countriesSkewed = []weightedString{
	{value: "US", weight: 35},
	{value: "CA", weight: 20},
	{value: "GB", weight: 15},
	{value: "DE", weight: 10},
	{value: "FR", weight: 8},
	{value: "JP", weight: 6},
	{value: "BR", weight: 4},
	{value: "AU", weight: 2},
}

func newBatchData(n int) batchData {
	return batchData{
		tenant:  make([]int64, n),
		user:    make([]int64, n),
		amount:  make([]int64, n),
		event:   make([]string, n),
		country: make([]string, n),
	}
}

func buildBatch(d batchData, n int) (vector.Batch, error) {
	cols := []vector.Column{
		{Name: "tenant_id", Type: sqltype.Int64, V: vector.Vec{Kind: vector.Int64, Len: n, I64: d.tenant}},
		{Name: "user_id", Type: sqltype.Int64, V: vector.Vec{Kind: vector.Int64, Len: n, I64: d.user}},
		{Name: "amount", Type: sqltype.Int64, V: vector.Vec{Kind: vector.Int64, Len: n, I64: d.amount}},
		buildTextColumn("event_type", n, d.event),
		buildTextColumn("country", n, d.country),
	}
	batch, err := vector.NewBatch(cols)
	if err != nil {
		return vector.Batch{}, fmt.Errorf("build batch: %w", err)
	}
	return batch, nil
}

func buildTextColumn(name string, rows int, values []string) vector.Column {
	approx := 0
	for _, v := range values {
		approx += len(v)
	}
	varbytes := vector.NewVarBytes(rows, approx)
	for row, value := range values {
		varbytes.Data = append(varbytes.Data, value...)
		varbytes.Offsets[row+1] = uint32(len(varbytes.Data))
	}
	return vector.Column{Name: name, Type: sqltype.Text, V: vector.Vec{Kind: vector.Text, Len: rows, Var: varbytes}}
}

// mix64 maps (row, salt) to a uint64 via a splitmix64 finalizer. Pure
// function with no global state, so output is reproducible across runs and
// machines without needing maphash's randomized seeds.
func mix64(row int64, salt uint64) uint64 {
	x := uint64(row) ^ salt
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

const (
	saltTenant  uint64 = 0x9e3779b97f4a7c15
	saltUser    uint64 = 0xbf58476d1ce4e5b9
	saltAmount  uint64 = 0x94d049bb133111eb
	saltEvent   uint64 = 0xff51afd7ed558ccd
	saltCountry uint64 = 0xc4ceb9fe1a85ec53
)

var eventTypes = []string{"signup", "checkout", "page_view", "cancel"}
var countries = []string{"US", "CA", "GB", "DE", "FR", "JP", "BR", "AU"}
