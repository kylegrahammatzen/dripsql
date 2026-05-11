package engine

import (
	"context"
	"reflect"
	"runtime"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/explain"
	v3sql "github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestEngineCreateInsertAndAggregateQueries(t *testing.T) {
	ctx := context.Background()
	db := openEventsDB(t, ctx)

	assertQueryRows(t, ctx, db,
		"SELECT count(*) FROM events",
		[]string{"count"},
		[][]any{{int64(5)}},
	)
	assertQueryRows(t, ctx, db,
		"SELECT count(*) FROM events WHERE tenant_id = 42 AND event_type = 'checkout'",
		[]string{"count"},
		[][]any{{int64(2)}},
	)
	assertQueryRows(t, ctx, db,
		"SELECT count(*) FROM events WHERE tenant_id = 999999",
		[]string{"count"},
		[][]any{{int64(0)}},
	)
	assertQueryRows(t, ctx, db,
		"SELECT sum(amount) FROM events WHERE tenant_id = 42 AND event_type = 'checkout'",
		[]string{"sum"},
		[][]any{{int64(125)}},
	)
	assertQueryRows(t, ctx, db,
		"SELECT count(*) AS n, sum(amount) AS total, min(amount) AS min_amount, max(amount) AS max_amount FROM events",
		[]string{"n", "total", "min_amount", "max_amount"},
		[][]any{{int64(5), int64(200), int64(5), int64(100)}},
	)
}

func TestEngineComputedWhereFallback(t *testing.T) {
	ctx := context.Background()
	db := openEventsDB(t, ctx)

	assertQueryRows(t, ctx, db,
		"SELECT amount FROM events WHERE amount * 2 >= 100",
		[]string{"amount"},
		[][]any{{int64(100)}, {int64(50)}},
	)
	assertQueryRows(t, ctx, db,
		"SELECT tenant_id FROM events WHERE lower(event_type) = 'checkout'",
		[]string{"tenant_id"},
		[][]any{{int64(42)}, {int64(42)}, {int64(7)}},
	)
	assertQueryRows(t, ctx, db,
		"SELECT sum(amount) FROM events WHERE tenant_id + 1 = 43",
		[]string{"sum"},
		[][]any{{int64(130)}},
	)
}

func TestEngineHavingQueries(t *testing.T) {
	ctx := context.Background()
	db := openEventsDB(t, ctx)

	assertQueryRows(t, ctx, db,
		"SELECT sum(amount) AS total FROM events HAVING total >= 200",
		[]string{"total"},
		[][]any{{int64(200)}},
	)
	assertQueryRows(t, ctx, db,
		"SELECT count(*) FROM events HAVING sum(amount) = 200",
		[]string{"count"},
		[][]any{{int64(5)}},
	)
	assertQueryRows(t, ctx, db,
		"SELECT country, count(*) AS total FROM events GROUP BY country HAVING total > 1",
		[]string{"country", "total"},
		[][]any{{"US", int64(3)}},
	)
	assertQueryRows(t, ctx, db,
		"SELECT event_type AS kind, count(*) FROM events GROUP BY event_type HAVING lower(kind) != 'login'",
		[]string{"kind", "count"},
		[][]any{{"checkout", int64(3)}, {"signup", int64(1)}},
	)

	rows, err := db.Query(ctx, "SELECT sum(amount) AS total FROM events HAVING total > 200")
	if err != nil {
		t.Fatalf("Query HAVING no match: %v", err)
	}
	if !reflect.DeepEqual(rows.Columns, []string{"total"}) || len(rows.Values) != 0 {
		t.Fatalf("HAVING no match rows = %#v", rows)
	}
}

func TestEngineOrderByExpressionQueries(t *testing.T) {
	ctx := context.Background()
	db := openEventsDB(t, ctx)

	assertQueryRows(t, ctx, db,
		"SELECT tenant_id, amount FROM events ORDER BY amount * 2 DESC",
		[]string{"tenant_id", "amount"},
		[][]any{{int64(42), int64(100)}, {int64(7), int64(50)}, {int64(42), int64(25)}, {int64(8), int64(20)}, {int64(42), int64(5)}},
	)
	assertQueryRows(t, ctx, db,
		"SELECT tenant_id, event_type FROM events ORDER BY tenant_id + 1, event_type DESC",
		[]string{"tenant_id", "event_type"},
		[][]any{{int64(7), "checkout"}, {int64(8), "signup"}, {int64(42), "login"}, {int64(42), "checkout"}, {int64(42), "checkout"}},
	)
	assertQueryRows(t, ctx, db,
		"SELECT tenant_id, event_type FROM events ORDER BY lower(event_type), amount DESC",
		[]string{"tenant_id", "event_type"},
		[][]any{{int64(42), "checkout"}, {int64(7), "checkout"}, {int64(42), "checkout"}, {int64(42), "login"}, {int64(8), "signup"}},
	)
}

func TestEngineSplitsPushableAndComputedWhere(t *testing.T) {
	ctx := context.Background()
	db := openEventsDB(t, ctx)

	assertQueryRows(t, ctx, db,
		"SELECT sum(amount) FROM events WHERE tenant_id = 42 AND amount * 2 > 10",
		[]string{"sum"},
		[][]any{{int64(125)}},
	)
}

func TestEngineParallelScalarAggregateAcrossSegments(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, t.TempDir(), Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
	if _, err := db.Exec(ctx, `CREATE TABLE events (tenant_id INT64 NOT NULL, amount INT64 NOT NULL)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO events VALUES (42, 10), (7, 100)`); err != nil {
		t.Fatalf("INSERT first: %v", err)
	}
	if err := db.FlushBuffered(ctx, "events"); err != nil {
		t.Fatalf("Flush first: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO events VALUES (42, 15), (42, 20)`); err != nil {
		t.Fatalf("INSERT second: %v", err)
	}
	if err := db.FlushBuffered(ctx, "events"); err != nil {
		t.Fatalf("Flush second: %v", err)
	}

	assertQueryRows(t, ctx, db,
		"SELECT sum(amount) FROM events WHERE tenant_id = 42",
		[]string{"sum"},
		[][]any{{int64(45)}},
	)
}

func TestEngineCountStarUsesMetadataWhenEligible(t *testing.T) {
	ctx := context.Background()
	db := openEventsDB(t, ctx)

	result, report, err := db.ExplainAnalyze(ctx, "SELECT count(*) FROM events")
	if err != nil {
		t.Fatalf("ExplainAnalyze count all: %v", err)
	}
	if !reflect.DeepEqual(result.Values, [][]any{{int64(5)}}) {
		t.Fatalf("count all result = %#v", result)
	}
	if report.Reduction.Rows.Scanned != 5 || report.Reduction.Rows.Candidate != 5 || report.Reduction.Rows.Matched != 5 {
		t.Fatalf("count all reduction = %#v", report.Reduction.Rows)
	}
	if report.Read.PayloadBytes != 0 {
		t.Fatalf("count all payload bytes = %d, want 0", report.Read.PayloadBytes)
	}

	result, report, err = db.ExplainAnalyze(ctx, "SELECT count(*) FROM events WHERE tenant_id = 999999")
	if err != nil {
		t.Fatalf("ExplainAnalyze absent count: %v", err)
	}
	if !reflect.DeepEqual(result.Values, [][]any{{int64(0)}}) {
		t.Fatalf("absent count result = %#v", result)
	}
	if report.Reduction.Rows.Scanned != 5 || report.Reduction.Rows.Candidate != 0 || report.Reduction.Rows.Matched != 0 {
		t.Fatalf("absent count reduction = %#v", report.Reduction.Rows)
	}
	if report.Read.PayloadBytes != 0 {
		t.Fatalf("absent count payload bytes = %d, want 0", report.Read.PayloadBytes)
	}

	result, report, err = db.ExplainAnalyze(ctx, "SELECT count(*) FROM events WHERE event_type = 'checkout'")
	if err != nil {
		t.Fatalf("ExplainAnalyze text count: %v", err)
	}
	if !reflect.DeepEqual(result.Values, [][]any{{int64(3)}}) {
		t.Fatalf("text count result = %#v", result)
	}
	if report.Reduction.Rows.Scanned != 5 || report.Reduction.Rows.Matched != 3 {
		t.Fatalf("text count reduction = %#v", report.Reduction.Rows)
	}
	if report.Read.PayloadBytes != 0 {
		t.Fatalf("text count payload bytes = %d, want 0", report.Read.PayloadBytes)
	}

	result, report, err = db.ExplainAnalyze(ctx, "SELECT count(*) FROM events WHERE event_type IN ('checkout', 'signup')")
	if err != nil {
		t.Fatalf("ExplainAnalyze text IN count: %v", err)
	}
	if !reflect.DeepEqual(result.Values, [][]any{{int64(4)}}) {
		t.Fatalf("text IN count result = %#v", result)
	}
	if report.Reduction.Rows.Scanned != 5 || report.Reduction.Rows.Matched != 4 {
		t.Fatalf("text IN count reduction = %#v", report.Reduction.Rows)
	}
	if report.Read.PayloadBytes != 0 {
		t.Fatalf("text IN count payload bytes = %d, want 0", report.Read.PayloadBytes)
	}
}

func TestEngineCountStarUsesIntValuePagePruning(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, t.TempDir(), Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
	if _, err := db.Exec(ctx, `CREATE TABLE events (tenant_id INT64 NOT NULL, amount INT64 NOT NULL)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO events VALUES (1, 10), (3, 30)`); err != nil {
		t.Fatalf("INSERT first: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO events VALUES (5, 50), (7, 70)`); err != nil {
		t.Fatalf("INSERT second: %v", err)
	}
	if err := db.FlushBuffered(ctx, "events"); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	result, report, err := db.ExplainAnalyze(ctx, "SELECT count(*) FROM events WHERE tenant_id = 2")
	if err != nil {
		t.Fatalf("ExplainAnalyze: %v", err)
	}
	if !reflect.DeepEqual(result.Values, [][]any{{int64(0)}}) {
		t.Fatalf("result = %#v", result)
	}
	if report.Reduction.Pages.Scanned != 2 || report.Reduction.Pages.Candidate != 0 {
		t.Fatalf("page reduction = %#v", report.Reduction.Pages)
	}
	if report.Reduction.Rows.Scanned != 4 || report.Reduction.Rows.Candidate != 0 || report.Reduction.Rows.Matched != 0 {
		t.Fatalf("row reduction = %#v", report.Reduction.Rows)
	}
	if report.Read.PayloadBytes != 0 {
		t.Fatalf("payload bytes = %d, want 0", report.Read.PayloadBytes)
	}
}

func TestEngineScalarMultiAggregateUsesMetadata(t *testing.T) {
	ctx := context.Background()
	db := openEventsDB(t, ctx)

	result, report, err := db.ExplainAnalyze(ctx, "SELECT count(*) AS n, count(amount) AS amounts, sum(amount) AS total, min(amount) AS min_amount, max(amount) AS max_amount FROM events")
	if err != nil {
		t.Fatalf("ExplainAnalyze: %v", err)
	}
	if !reflect.DeepEqual(result.Values, [][]any{{int64(5), int64(5), int64(200), int64(5), int64(100)}}) {
		t.Fatalf("result = %#v", result)
	}
	if report.Reduction.Rows.Scanned != 5 || report.Reduction.Rows.Candidate != 5 || report.Reduction.Rows.Matched != 5 {
		t.Fatalf("row reduction = %#v", report.Reduction.Rows)
	}
	if report.Read.PayloadBytes != 0 {
		t.Fatalf("payload bytes = %d, want 0", report.Read.PayloadBytes)
	}
	for _, access := range []struct {
		name     string
		strategy string
	}{
		{name: "count(*)", strategy: "metadata count"},
		{name: "count(amount)", strategy: "metadata count"},
		{name: "sum(amount)", strategy: "metadata sum"},
		{name: "min(amount)", strategy: "metadata min"},
		{name: "max(amount)", strategy: "metadata max"},
	} {
		if !hasAccess(report.Access, access.name, access.strategy, "") {
			t.Fatalf("missing access %q/%q in %#v", access.name, access.strategy, report.Access)
		}
	}
}

func TestEngineScalarMetadataAggregatesInt32AndNullable(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, t.TempDir(), Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
	if _, err := db.Exec(ctx, `CREATE TABLE metrics (name TEXT NOT NULL, score32 INT32, score64 INT64)`); err != nil {
		t.Fatalf("CREATE TABLE metrics: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO metrics VALUES
		('a', 1, 10),
		('b', NULL, 20),
		('c', 3, NULL),
		('d', NULL, NULL),
		('e', 2, 5)`); err != nil {
		t.Fatalf("INSERT metrics: %v", err)
	}
	if err := db.FlushBuffered(ctx, "metrics"); err != nil {
		t.Fatalf("FlushBuffered metrics: %v", err)
	}

	result, report, err := db.ExplainAnalyze(ctx, `SELECT
		count(*) AS rows,
		count(score32) AS count32,
		sum(score32) AS sum32,
		min(score32) AS min32,
		max(score32) AS max32,
		count(score64) AS count64,
		sum(score64) AS sum64,
		min(score64) AS min64,
		max(score64) AS max64
		FROM metrics`)
	if err != nil {
		t.Fatalf("ExplainAnalyze metrics: %v", err)
	}
	want := [][]any{{int64(5), int64(3), int64(6), int64(1), int64(3), int64(3), int64(35), int64(5), int64(20)}}
	if !reflect.DeepEqual(result.Values, want) {
		t.Fatalf("metrics result = %#v, want %#v", result.Values, want)
	}
	if report.Read.PayloadBytes != 0 {
		t.Fatalf("metrics payload bytes = %d, want 0", report.Read.PayloadBytes)
	}

	if _, err := db.Exec(ctx, `CREATE TABLE all_null_metrics (score32 INT32, score64 INT64)`); err != nil {
		t.Fatalf("CREATE TABLE all_null_metrics: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO all_null_metrics VALUES (NULL, NULL), (NULL, NULL)`); err != nil {
		t.Fatalf("INSERT all_null_metrics: %v", err)
	}
	if err := db.FlushBuffered(ctx, "all_null_metrics"); err != nil {
		t.Fatalf("FlushBuffered all_null_metrics: %v", err)
	}
	result, report, err = db.ExplainAnalyze(ctx, `SELECT
		count(score32) AS count32,
		sum(score32) AS sum32,
		min(score32) AS min32,
		max(score32) AS max32,
		count(score64) AS count64,
		sum(score64) AS sum64,
		min(score64) AS min64,
		max(score64) AS max64
		FROM all_null_metrics`)
	if err != nil {
		t.Fatalf("ExplainAnalyze all-null metrics: %v", err)
	}
	want = [][]any{{int64(0), int64(0), nil, nil, int64(0), int64(0), nil, nil}}
	if !reflect.DeepEqual(result.Values, want) {
		t.Fatalf("all-null result = %#v, want %#v", result.Values, want)
	}
	if report.Read.PayloadBytes != 0 {
		t.Fatalf("all-null payload bytes = %d, want 0", report.Read.PayloadBytes)
	}
}

func TestEngineParallelGroupedCountWithPredicateAcrossSegments(t *testing.T) {
	ctx := context.Background()
	oldProcs := runtime.GOMAXPROCS(2)
	t.Cleanup(func() { runtime.GOMAXPROCS(oldProcs) })

	db, err := Open(ctx, t.TempDir(), Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
	if _, err := db.Exec(ctx, `CREATE TABLE events (tenant_id INT64 NOT NULL, event_type TEXT NOT NULL, country TEXT NOT NULL)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	for segment := 0; segment < 8; segment++ {
		if _, err := db.Exec(ctx, `INSERT INTO events VALUES
			(1, 'checkout', 'US'),
			(2, 'checkout', 'CA'),
			(3, 'login', 'US'),
			(4, 'checkout', 'US')`); err != nil {
			t.Fatalf("INSERT segment %d: %v", segment, err)
		}
		if err := db.FlushBuffered(ctx, "events"); err != nil {
			t.Fatalf("Flush segment %d: %v", segment, err)
		}
	}

	assertQueryRows(t, ctx, db,
		"SELECT country, count(*) FROM events WHERE event_type = 'checkout' GROUP BY country",
		[]string{"country", "count"},
		[][]any{{"CA", int64(8)}, {"US", int64(16)}},
	)
}

func TestEngineSumUsesMetadataWhenEligible(t *testing.T) {
	ctx := context.Background()
	db := openEventsDB(t, ctx)

	result, report, err := db.ExplainAnalyze(ctx, "SELECT sum(amount) FROM events")
	if err != nil {
		t.Fatalf("ExplainAnalyze sum all: %v", err)
	}
	if !reflect.DeepEqual(result.Values, [][]any{{int64(200)}}) {
		t.Fatalf("sum all result = %#v", result)
	}
	if report.Reduction.Rows.Scanned != 5 || report.Reduction.Rows.Candidate != 5 || report.Reduction.Rows.Matched != 5 {
		t.Fatalf("sum all reduction = %#v", report.Reduction.Rows)
	}
	if report.Read.PayloadBytes != 0 {
		t.Fatalf("sum all payload bytes = %d, want 0", report.Read.PayloadBytes)
	}

	result, report, err = db.ExplainAnalyze(ctx, "SELECT sum(amount) FROM events WHERE tenant_id = 999999")
	if err != nil {
		t.Fatalf("ExplainAnalyze absent sum: %v", err)
	}
	if !reflect.DeepEqual(result.Values, [][]any{{int64(0)}}) {
		t.Fatalf("absent sum result = %#v", result)
	}
	if report.Reduction.Rows.Scanned != 5 || report.Reduction.Rows.Candidate != 0 || report.Reduction.Rows.Matched != 0 {
		t.Fatalf("absent sum reduction = %#v", report.Reduction.Rows)
	}
	if report.Read.PayloadBytes != 0 {
		t.Fatalf("absent sum payload bytes = %d, want 0", report.Read.PayloadBytes)
	}
}

func TestEngineGroupedCountQuery(t *testing.T) {
	ctx := context.Background()
	db := openEventsDB(t, ctx)

	assertQueryRows(t, ctx, db,
		"SELECT country, count(*) FROM events WHERE event_type = 'checkout' GROUP BY country",
		[]string{"country", "count"},
		[][]any{{"CA", int64(1)}, {"US", int64(2)}},
	)
	assertQueryRows(t, ctx, db,
		"SELECT tenant_id, count(*) FROM events WHERE event_type = 'checkout' GROUP BY tenant_id",
		[]string{"tenant_id", "count"},
		[][]any{{int64(7), int64(1)}, {int64(42), int64(2)}},
	)

	result, report, err := db.ExplainAnalyze(ctx, "SELECT event_type, count(*) FROM events GROUP BY event_type")
	if err != nil {
		t.Fatalf("ExplainAnalyze grouped text count: %v", err)
	}
	if !reflect.DeepEqual(result.Values, [][]any{{"checkout", int64(3)}, {"login", int64(1)}, {"signup", int64(1)}}) {
		t.Fatalf("grouped text count result = %#v", result)
	}
	if report.Read.PayloadBytes != 0 {
		t.Fatalf("grouped text count payload bytes = %d, want 0", report.Read.PayloadBytes)
	}
	if !hasAccess(report.Access, "events", "metadata scan", "no payload") {
		t.Fatalf("grouped text count access = %#v", report.Access)
	}
}

func TestEngineGroupedAggregateQueries(t *testing.T) {
	ctx := context.Background()
	db := openEventsDB(t, ctx)

	assertQueryRows(t, ctx, db,
		"SELECT country, sum(amount) FROM events GROUP BY country",
		[]string{"country", "sum"},
		[][]any{{"CA", int64(50)}, {"GB", int64(20)}, {"US", int64(130)}},
	)
	assertQueryRows(t, ctx, db,
		"SELECT country, count(*) AS n, sum(amount) AS total FROM events GROUP BY country",
		[]string{"country", "n", "total"},
		[][]any{{"CA", int64(1), int64(50)}, {"GB", int64(1), int64(20)}, {"US", int64(3), int64(130)}},
	)
	assertQueryRows(t, ctx, db,
		"SELECT country, count(*) AS n, sum(amount) AS total FROM events GROUP BY country HAVING sum(amount) >= 50",
		[]string{"country", "n", "total"},
		[][]any{{"CA", int64(1), int64(50)}, {"US", int64(3), int64(130)}},
	)
	assertQueryRows(t, ctx, db,
		"SELECT event_type, min(amount) FROM events GROUP BY event_type",
		[]string{"event_type", "min"},
		[][]any{{"checkout", int64(25)}, {"login", int64(5)}, {"signup", int64(20)}},
	)
	assertQueryRows(t, ctx, db,
		"SELECT event_type, max(amount) FROM events GROUP BY event_type",
		[]string{"event_type", "max"},
		[][]any{{"checkout", int64(100)}, {"login", int64(5)}, {"signup", int64(20)}},
	)
	assertQueryRows(t, ctx, db,
		"SELECT country, count(amount) FROM events GROUP BY country",
		[]string{"country", "count"},
		[][]any{{"CA", int64(1)}, {"GB", int64(1)}, {"US", int64(3)}},
	)
	assertQueryRows(t, ctx, db,
		"SELECT country, count(*) FROM events GROUP BY country HAVING sum(amount) > 60",
		[]string{"country", "count"},
		[][]any{{"US", int64(3)}},
	)
	assertQueryRows(t, ctx, db,
		"SELECT country, sum(amount) AS total FROM events GROUP BY country HAVING total >= 50",
		[]string{"country", "total"},
		[][]any{{"CA", int64(50)}, {"US", int64(130)}},
	)
}

func TestEngineGroupedTextCountSumNullableQuery(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, t.TempDir(), Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
	if _, err := db.Exec(ctx, `CREATE TABLE events (country TEXT NOT NULL, amount INT64)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO events VALUES
		('US', 10),
		('US', NULL),
		('CA', 5),
		('CA', NULL),
		('GB', NULL),
		('US', 7)`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}

	assertQueryRows(t, ctx, db,
		"SELECT country, count(*) AS n, sum(amount) AS total FROM events GROUP BY country",
		[]string{"country", "n", "total"},
		[][]any{{"CA", int64(2), int64(5)}, {"GB", int64(1), int64(0)}, {"US", int64(3), int64(17)}},
	)
}

func TestTextGroupCountSumCollectorDictionaryNullableInt64(t *testing.T) {
	group := testDictionaryTextColumn("country", []string{"US", "CA", "GB"}, []uint8{0, 0, 1, 1, 2, 0}, nil)
	sumValid := types.NewValidity(6)
	types.SetInvalid(sumValid, 1)
	types.SetInvalid(sumValid, 4)
	sum := types.Column{Name: "amount", Type: types.Int64, V: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: 6, Valid: sumValid, I64: []int64{10, 0, 5, 2, 0, 7}}}
	batch, err := types.NewBatch([]types.Column{group, sum})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	collector := &textGroupCountSumCollector{group: "country", sumCol: "amount", groups: make(map[string]textGroupCountSumState)}
	if err := collector.Push(batch, sel); err != nil {
		t.Fatalf("Push: %v", err)
	}
	want := map[string]textGroupCountSumState{
		"CA": {count: 2, sum: 7},
		"GB": {count: 1, sum: 0},
		"US": {count: 3, sum: 17},
	}
	if !reflect.DeepEqual(collector.groups, want) {
		t.Fatalf("groups = %#v, want %#v", collector.groups, want)
	}
}

func TestTextGroupCountSumCollectorFlatSelectionAndInt32(t *testing.T) {
	groupValid := types.NewValidity(5)
	types.SetInvalid(groupValid, 3)
	group := testFlatTextColumn("country", []string{"US", "CA", "US", "GB", "CA"}, groupValid)
	sumValid := types.NewValidity(5)
	types.SetInvalid(sumValid, 2)
	sum := types.Column{Name: "amount32", Type: types.Int32, V: types.Vec{Kind: types.VecInt32, Encoding: types.EncodingFlat, Len: 5, Valid: sumValid, I32: []int32{1, 2, 3, 4, 5}}}
	batch, err := types.NewBatch([]types.Column{group, sum})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	sel := types.NewSelectionMask(batch.Len)
	sel.Set(0)
	sel.Set(2)
	sel.Set(3)
	sel.Set(4)
	collector := &textGroupCountSumCollector{group: "country", sumCol: "amount32", groups: make(map[string]textGroupCountSumState)}
	if err := collector.Push(batch, sel); err != nil {
		t.Fatalf("Push: %v", err)
	}
	want := map[string]textGroupCountSumState{
		"CA": {count: 1, sum: 5},
		"US": {count: 2, sum: 1},
	}
	if !reflect.DeepEqual(collector.groups, want) {
		t.Fatalf("groups = %#v, want %#v", collector.groups, want)
	}
}

func TestEngineGroupedEnumCountQuery(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, t.TempDir(), Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})
	if _, err := db.Exec(ctx, `CREATE TYPE status AS ENUM ('new', 'done')`); err != nil {
		t.Fatalf("CREATE TYPE: %v", err)
	}
	if _, err := db.Exec(ctx, `CREATE TABLE events (status status NOT NULL, amount INT64 NOT NULL)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO events VALUES ('done', 10), ('new', 20), ('done', 30)`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	assertQueryRows(t, ctx, db,
		"SELECT status, count(*) FROM events GROUP BY status",
		[]string{"status", "count"},
		[][]any{{"new", int64(1)}, {"done", int64(2)}},
	)
}

func TestEngineExplainAnalyzeReportsExecutionStats(t *testing.T) {
	ctx := context.Background()
	db := openEventsDB(t, ctx)
	if err := db.FlushBuffered(ctx, "events"); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}

	result, report, err := db.ExplainAnalyze(ctx, "SELECT count(*) FROM events WHERE tenant_id = 42 AND event_type = 'checkout'")
	if err != nil {
		t.Fatalf("ExplainAnalyze: %v", err)
	}
	if !reflect.DeepEqual(result.Columns, []string{"count"}) || !reflect.DeepEqual(result.Values, [][]any{{int64(2)}}) {
		t.Fatalf("result = %#v", result)
	}
	if report.Reduction.Rows.Scanned != 5 || report.Reduction.Rows.Candidate != 5 || report.Reduction.Rows.Matched != 2 {
		t.Fatalf("reduction rows = %#v", report.Reduction.Rows)
	}
	if !report.Selectivity.Valid || report.Selectivity.Matched != 2 || report.Selectivity.Total != 5 {
		t.Fatalf("selectivity = %#v", report.Selectivity)
	}
	if !report.BytesPerMatch.Valid || report.Read.PayloadBytes <= 0 {
		t.Fatalf("read/bytes per match = %#v %#v", report.Read, report.BytesPerMatch)
	}
	if report.Output.Kept != 1 || report.Timing.Samples != 1 {
		t.Fatalf("output/timing = %#v %#v", report.Output, report.Timing)
	}
	if len(report.Access) < 3 {
		t.Fatalf("access = %#v", report.Access)
	}

	explained, err := db.Query(ctx, "EXPLAIN ANALYZE SELECT count(*) FROM events WHERE tenant_id = 42 AND event_type = 'checkout'")
	if err != nil {
		t.Fatalf("EXPLAIN ANALYZE query: %v", err)
	}
	for _, section := range []string{"Plan", "Reduction", "Read", "Access", "Timing"} {
		if !hasExplainSection(explained, section) {
			t.Fatalf("missing section %q in %#v", section, explained.Values)
		}
	}
}

func TestEngineExplainReportsUUIDSummaryPrune(t *testing.T) {
	for _, tc := range []struct {
		name     string
		expr     v3sql.BoundExpr
		eligible bool
	}{
		{name: "equal", expr: v3sql.BoundExpr{Kind: v3sql.BoundExprBinary, Op: v3sql.BoundOpEqual}, eligible: true},
		{name: "not equal", expr: v3sql.BoundExpr{Kind: v3sql.BoundExprBinary, Op: v3sql.BoundOpNotEqual}},
		{name: "in", expr: v3sql.BoundExpr{Kind: v3sql.BoundExprIn}, eligible: true},
		{name: "not in", expr: v3sql.BoundExpr{Kind: v3sql.BoundExprIn, Not: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			strategy, eligible := pruneStrategy(tc.expr, types.KindUUID)
			if strategy != "uuid summary prune" || eligible != tc.eligible {
				t.Fatalf("pruneStrategy = %q, %v; want uuid summary prune, %v", strategy, eligible, tc.eligible)
			}
		})
	}
}

func TestEngineReopensCatalogAndSegments(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	db, err := Open(ctx, dir, Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := db.Exec(ctx, `CREATE TYPE status AS ENUM ('new', 'done')`); err != nil {
		t.Fatalf("CREATE TYPE: %v", err)
	}
	if _, err := db.Exec(ctx, `CREATE TABLE events (tenant_id INT64 NOT NULL, status status NOT NULL, amount INT64 NOT NULL)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	if _, err := db.Exec(ctx, `INSERT INTO events VALUES (42, 'new', 10), (42, 'done', 20)`); err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(ctx, dir, Options{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Fatalf("Close reopened: %v", err)
		}
	})
	assertQueryRows(t, ctx, reopened,
		"SELECT sum(amount) FROM events WHERE tenant_id = 42",
		[]string{"sum"},
		[][]any{{int64(30)}},
	)
}

func openEventsDB(t *testing.T, ctx context.Context) *DB {
	t.Helper()
	db, err := Open(ctx, t.TempDir(), Options{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	})

	if _, err := db.Exec(ctx, `CREATE TABLE events (tenant_id INT64 NOT NULL, event_type TEXT NOT NULL, amount INT64 NOT NULL, country TEXT NOT NULL)`); err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	result, err := db.Exec(ctx, `INSERT INTO events VALUES
		(42, 'checkout', 100, 'US'),
		(42, 'checkout', 25, 'US'),
		(42, 'login', 5, 'US'),
		(7, 'checkout', 50, 'CA'),
		(8, 'signup', 20, 'GB')`)
	if err != nil {
		t.Fatalf("INSERT: %v", err)
	}
	if err := db.FlushBuffered(ctx, "events"); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	if result.Statements != 1 || result.RowsAffected != 5 {
		t.Fatalf("INSERT result = %#v, want 1 statement and 5 rows", result)
	}
	return db
}

func assertQueryRows(t *testing.T, ctx context.Context, db *DB, query string, columns []string, values [][]any) {
	t.Helper()
	rows, err := db.Query(ctx, query)
	if err != nil {
		t.Fatalf("Query %q: %v", query, err)
	}
	if !reflect.DeepEqual(rows.Columns, columns) {
		t.Fatalf("Query %q columns = %#v, want %#v", query, rows.Columns, columns)
	}
	if !reflect.DeepEqual(rows.Values, values) {
		t.Fatalf("Query %q values = %#v, want %#v", query, rows.Values, values)
	}
}

func testDictionaryTextColumn(name string, values []string, ids []uint8, valid types.Validity) types.Column {
	return types.Column{Name: name, Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingDictionary, Len: len(ids), Valid: valid, DictIDs: ids, DictValues: testVarBytes(values)}}
}

func testFlatTextColumn(name string, values []string, valid types.Validity) types.Column {
	return types.Column{Name: name, Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: len(values), Valid: valid, Var: testVarBytes(values)}}
}

func testVarBytes(values []string) types.VarBytes {
	dataBytes := 0
	for _, value := range values {
		dataBytes += len(value)
	}
	varBytes := types.NewVarBytes(len(values), dataBytes)
	for row, value := range values {
		varBytes.AppendString(row, value)
	}
	return varBytes
}

func hasExplainSection(rows *Rows, section string) bool {
	if rows == nil {
		return false
	}
	for _, row := range rows.Values {
		if len(row) != 0 && row[0] == section {
			return true
		}
	}
	return false
}

func hasAccess(access []explain.Access, name, strategy, effect string) bool {
	for _, entry := range access {
		if entry.Name == name && entry.Strategy == strategy && entry.Effect == effect {
			return true
		}
	}
	return false
}
