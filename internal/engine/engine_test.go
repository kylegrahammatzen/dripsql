package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
)

func mustOpen(t *testing.T, dir string) *DB {
	t.Helper()
	db, err := Open(context.Background(), dir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func mustExec(t *testing.T, db *DB, sql string) Result {
	t.Helper()
	result, err := db.Exec(context.Background(), sql)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestCreateTypeAndTable(t *testing.T) {
	ctx := context.Background()
	db := mustOpen(t, t.TempDir())

	if err := db.CreateType(ctx, schema.TypeSpec{Name: "event_status", EnumLabels: []string{"new", "done"}}); err != nil {
		t.Fatal(err)
	}
	if _, ok := db.Type("event_status"); !ok {
		t.Fatal("created type not found")
	}

	spec := schema.TableSpec{
		Name:        "all_types",
		IfNotExists: true,
		Columns:     allBuiltInColumns(),
		Options:     schema.TableOptions{Storage: schema.StorageColumnar, Profile: schema.ProfileEventAnalytics, Compression: schema.CompressionAuto},
	}
	if err := db.CreateTable(ctx, spec); err != nil {
		t.Fatal(err)
	}
	def, ok := db.Table("all_types")
	if !ok {
		t.Fatal("created table not found")
	}
	if len(def.Columns) != len(spec.Columns) {
		t.Fatalf("column count = %d, want %d", len(def.Columns), len(spec.Columns))
	}
	if def.Options.Storage != schema.StorageColumnar || def.Options.Profile != schema.ProfileEventAnalytics {
		t.Fatalf("options not preserved: %#v", def.Options)
	}
	if err := db.CreateTable(ctx, spec); err != nil {
		t.Fatal("CreateTable if not exists:", err)
	}
}

func TestExecCreateDDL(t *testing.T) {
	db := mustOpen(t, t.TempDir())

	result := mustExec(t, db, `
		CREATE TYPE IF NOT EXISTS event_status AS ENUM ('new', 'done');
		CREATE TABLE IF NOT EXISTS all_types (
			b BOOL NOT NULL,
			i16 INT16,
			i32 INT32,
			i64 INT64,
			f32 FLOAT32,
			f64 FLOAT64,
			dec DECIMAL,
			txt TEXT,
			bin BYTES,
			id UUID,
			ts TIMESTAMP,
			tm TIME,
			d DATE,
			j JSON,
			status event_status
		) WITH (
			storage = columnar,
			profile = event_analytics,
			segment_rows = auto,
			compression = auto,
			sort_by = 'i64, txt',
			time_column = ts
		);
	`)
	if result.Statements != 2 || result.RowsAffected != 0 {
		t.Fatalf("result = %#v", result)
	}
	if _, ok := db.Type("event_status"); !ok {
		t.Fatal("type not found")
	}
	def, ok := db.Table("all_types")
	if !ok || len(def.Columns) != 15 {
		t.Fatalf("table = %#v ok=%v", def, ok)
	}
	if def.Columns[0].Nullable || def.Columns[0].Type != sqltype.Bool {
		t.Fatalf("bool column = %#v", def.Columns[0])
	}
}

func TestExecInsertAndQueryCount(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, event_type TEXT NOT NULL)`)

	result := mustExec(t, db, `INSERT INTO events VALUES (1, 'signup'), (2, 'checkout'), (1, 'login')`)
	if result.RowsAffected != 3 {
		t.Fatalf("RowsAffected = %d, want 3", result.RowsAffected)
	}

	rows, err := db.Query(context.Background(), `SELECT count(*) FROM events`)
	if err != nil {
		t.Fatalf("Query count: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(3) {
		t.Fatalf("count = %#v, want 3", got)
	}

	rows, err = db.Query(context.Background(), `SELECT count(*) FROM events WHERE tenant_id = 1`)
	if err != nil {
		t.Fatalf("Query count where: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(2) {
		t.Fatalf("count where = %#v, want 2", got)
	}

	rows, err = db.Query(context.Background(), `SELECT count(*) FROM events WHERE tenant_id = 1 AND event_type = 'signup'`)
	if err != nil {
		t.Fatalf("Query count where and: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(1) {
		t.Fatalf("count where and = %#v, want 1", got)
	}

	rows, err = db.Query(context.Background(), `SELECT count(*) FROM events WHERE tenant_id = 2 OR event_type = 'login'`)
	if err != nil {
		t.Fatalf("Query count where or: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(2) {
		t.Fatalf("count where or = %#v, want 2", got)
	}

	rows, err = db.Query(context.Background(), `SELECT count(*) FROM events WHERE NOT (tenant_id = 1 AND event_type = 'login')`)
	if err != nil {
		t.Fatalf("Query count where not: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(2) {
		t.Fatalf("count where not = %#v, want 2", got)
	}

	rows, err = db.Query(context.Background(), `SELECT count(*) FROM events WHERE tenant_id BETWEEN 1 AND 2`)
	if err != nil {
		t.Fatalf("Query count between: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(3) {
		t.Fatalf("count between = %#v, want 3", got)
	}

	rows, err = db.Query(context.Background(), `SELECT count(*) FROM events HAVING count(*) > 0`)
	if err != nil {
		t.Fatalf("Query scalar having count: %v", err)
	}
	if got := len(rows.Values); got != 1 {
		t.Fatalf("scalar having count rows = %d, want 1 (%#v)", got, rows.Values)
	}
	if got := rows.Values[0][0]; got != uint64(3) {
		t.Fatalf("scalar having count = %#v, want 3", got)
	}

	rows, err = db.Query(context.Background(), `SELECT count(*) FROM events HAVING count(*) > 99`)
	if err != nil {
		t.Fatalf("Query scalar having count empty: %v", err)
	}
	if got := len(rows.Values); got != 0 {
		t.Fatalf("scalar having empty rows = %d, want 0 (%#v)", got, rows.Values)
	}
}

func TestQueryCountColumn(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, event_type TEXT)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, 'signup'), (2, NULL), (1, 'login')`)
	def, ok := db.Table("events")
	if !ok {
		t.Fatal("events table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO events VALUES (1, NULL), (2, 'checkout')`)

	rows, err := db.Query(context.Background(), `SELECT count(event_type) FROM events`)
	if err != nil {
		t.Fatalf("Query count column: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(3) {
		t.Fatalf("count column = %#v, want 3", got)
	}

	rows, err = db.Query(context.Background(), `SELECT count(event_type) FROM events WHERE tenant_id = 1`)
	if err != nil {
		t.Fatalf("Query count column where: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(2) {
		t.Fatalf("count column where = %#v, want 2", got)
	}

	rows, err = db.Query(context.Background(), `SELECT count(event_type) FROM events WHERE tenant_id = 1 AND event_type = 'signup'`)
	if err != nil {
		t.Fatalf("Query count column where and: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(1) {
		t.Fatalf("count column where and = %#v, want 1", got)
	}
}

func TestQuerySumColumn(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, score INT64)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, 10), (2, NULL), (1, 5)`)
	def, ok := db.Table("events")
	if !ok {
		t.Fatal("events table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO events VALUES (1, 7), (2, 3)`)

	rows, err := db.Query(context.Background(), `SELECT sum(score) FROM events`)
	if err != nil {
		t.Fatalf("Query sum column: %v", err)
	}
	if got := rows.Values[0][0]; got != int64(25) {
		t.Fatalf("sum column = %#v, want 25", got)
	}

	rows, err = db.Query(context.Background(), `SELECT sum(score) FROM events WHERE tenant_id = 1`)
	if err != nil {
		t.Fatalf("Query sum column where: %v", err)
	}
	if got := rows.Values[0][0]; got != int64(22) {
		t.Fatalf("sum column where = %#v, want 22", got)
	}

	rows, err = db.Query(context.Background(), `SELECT sum(score) FROM events WHERE tenant_id = 1 AND score > 6`)
	if err != nil {
		t.Fatalf("Query sum column where and: %v", err)
	}
	if got := rows.Values[0][0]; got != int64(17) {
		t.Fatalf("sum column where and = %#v, want 17", got)
	}

	rows, err = db.Query(context.Background(), `SELECT sum(score) FROM events WHERE tenant_id = 2 OR score = 5`)
	if err != nil {
		t.Fatalf("Query sum column where or: %v", err)
	}
	if got := rows.Values[0][0]; got != int64(8) {
		t.Fatalf("sum column where or = %#v, want 8", got)
	}

	rows, err = db.Query(context.Background(), `SELECT sum(score) total FROM events HAVING total >= 25`)
	if err != nil {
		t.Fatalf("Query scalar having sum: %v", err)
	}
	if got := len(rows.Values); got != 1 {
		t.Fatalf("scalar having sum rows = %d, want 1 (%#v)", got, rows.Values)
	}
	if got := rows.Values[0][0]; got != int64(25) {
		t.Fatalf("scalar having sum = %#v, want 25", got)
	}
}

func TestQueryAggregateAliases(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, score INT64, event_type TEXT)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, 10, 'signup'), (2, 20, 'checkout'), (1, 5, 'signup')`)

	rows, err := db.Query(context.Background(), `SELECT sum(score) AS total FROM events`)
	if err != nil {
		t.Fatalf("Query alias: %v", err)
	}
	if got, want := rows.Columns, []string{"total"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}

	rows, err = db.Query(context.Background(), `SELECT sum(score) total FROM events`)
	if err != nil {
		t.Fatalf("Query implicit alias: %v", err)
	}
	if got, want := rows.Columns, []string{"total"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}

	rows, err = db.Query(context.Background(), `SELECT event_type kind, sum(score) total FROM events GROUP BY event_type`)
	if err != nil {
		t.Fatalf("Query group alias: %v", err)
	}
	if got, want := rows.Columns, []string{"kind", "total"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
}

func TestQueryMinMaxColumn(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, score INT64)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, 10), (2, NULL), (1, 5)`)
	def, ok := db.Table("events")
	if !ok {
		t.Fatal("events table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO events VALUES (1, 7), (2, 3)`)

	rows, err := db.Query(context.Background(), `SELECT min(score) FROM events`)
	if err != nil {
		t.Fatalf("Query min column: %v", err)
	}
	if got := rows.Values[0][0]; got != int64(3) {
		t.Fatalf("min column = %#v, want 3", got)
	}

	rows, err = db.Query(context.Background(), `SELECT max(score) FROM events WHERE tenant_id = 1`)
	if err != nil {
		t.Fatalf("Query max column where: %v", err)
	}
	if got := rows.Values[0][0]; got != int64(10) {
		t.Fatalf("max column where = %#v, want 10", got)
	}

	rows, err = db.Query(context.Background(), `SELECT min(score) FROM events WHERE tenant_id = 1 AND score > 6`)
	if err != nil {
		t.Fatalf("Query min column where and: %v", err)
	}
	if got := rows.Values[0][0]; got != int64(7) {
		t.Fatalf("min column where and = %#v, want 7", got)
	}
}

func TestQueryCountReadsHotBufferWithoutPublishing(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, event_type TEXT NOT NULL)`)
	result := mustExec(t, db, `INSERT INTO events VALUES (1, 'signup'), (2, 'checkout'), (1, 'login')`)
	if result.RowsAffected != 3 {
		t.Fatalf("RowsAffected = %d, want 3", result.RowsAffected)
	}
	def, ok := db.Table("events")
	if !ok {
		t.Fatal("events table not found")
	}
	segments, err := db.data.Segments(context.Background(), def)
	if err != nil {
		t.Fatalf("Segments before query: %v", err)
	}
	if len(segments) != 0 {
		t.Fatalf("segments before query = %d, want 0", len(segments))
	}
	rows, err := db.Query(context.Background(), `SELECT count(*) FROM events WHERE tenant_id = 1`)
	if err != nil {
		t.Fatalf("Query count where: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(2) {
		t.Fatalf("count where = %#v, want 2", got)
	}
	segments, err = db.data.Segments(context.Background(), def)
	if err != nil {
		t.Fatalf("Segments after query: %v", err)
	}
	if len(segments) != 0 {
		t.Fatalf("segments after query = %d, want 0", len(segments))
	}
}

func TestExecInsertInt32AndQueryCount(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT32 NOT NULL, event_type TEXT NOT NULL)`)
	result := mustExec(t, db, `INSERT INTO events VALUES (1, 'signup'), (2, 'checkout'), (1, 'login')`)
	if result.RowsAffected != 3 {
		t.Fatalf("RowsAffected = %d, want 3", result.RowsAffected)
	}
	rows, err := db.Query(context.Background(), `SELECT count(*) FROM events`)
	if err != nil {
		t.Fatalf("Query count: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(3) {
		t.Fatalf("count = %#v, want 3", got)
	}

	rows, err = db.Query(context.Background(), `SELECT count(*) FROM events WHERE tenant_id = 1`)
	if err != nil {
		t.Fatalf("Query count where: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(2) {
		t.Fatalf("count where = %#v, want 2", got)
	}

	rows, err = db.Query(context.Background(), `SELECT count(*) FROM events WHERE tenant_id BETWEEN 1 AND 2`)
	if err != nil {
		t.Fatalf("Query count between: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(3) {
		t.Fatalf("count between = %#v, want 3", got)
	}
}

func TestExecInsertBoolAndQueryCount(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (active BOOL NOT NULL, event_type TEXT NOT NULL)`)
	result := mustExec(t, db, `INSERT INTO events VALUES (true, 'signup'), (false, 'checkout'), (true, 'login')`)
	if result.RowsAffected != 3 {
		t.Fatalf("RowsAffected = %d, want 3", result.RowsAffected)
	}
	rows, err := db.Query(context.Background(), `SELECT count(*) FROM events`)
	if err != nil {
		t.Fatalf("Query count: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(3) {
		t.Fatalf("count = %#v, want 3", got)
	}

	rows, err = db.Query(context.Background(), `SELECT count(*) FROM events WHERE active = true`)
	if err != nil {
		t.Fatalf("Query count true: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(2) {
		t.Fatalf("count true = %#v, want 2", got)
	}

	rows, err = db.Query(context.Background(), `SELECT count(*) FROM events WHERE active = false`)
	if err != nil {
		t.Fatalf("Query count false: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(1) {
		t.Fatalf("count false = %#v, want 1", got)
	}
}

func TestExecInsertTimestampDateAndQueryCount(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (created_at TIMESTAMP NOT NULL, event_date DATE NOT NULL, event_type TEXT NOT NULL)`)
	result := mustExec(t, db, `INSERT INTO events VALUES ('2026-05-07T12:30:00.000000123Z', '2026-05-07', 'signup'), ('2026-05-08T00:00:00Z', '2026-05-08', 'checkout')`)
	if result.RowsAffected != 2 {
		t.Fatalf("RowsAffected = %d, want 2", result.RowsAffected)
	}
	rows, err := db.Query(context.Background(), `SELECT count(*) FROM events`)
	if err != nil {
		t.Fatalf("Query count: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(2) {
		t.Fatalf("count = %#v, want 2", got)
	}

	rows, err = db.Query(context.Background(), `SELECT created_at FROM events WHERE created_at >= '2026-05-08T00:00:00Z' ORDER BY created_at`)
	if err != nil {
		t.Fatalf("Query timestamp scan: %v", err)
	}
	if got, want := rows.Columns, []string{"created_at"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
	wantRows := [][]any{{"2026-05-08T00:00:00.000000000Z"}}
	if got := len(rows.Values); got != len(wantRows) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(wantRows), rows.Values)
	}
	for i := range wantRows {
		for j := range wantRows[i] {
			if rows.Values[i][j] != wantRows[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], wantRows[i][j], rows.Values)
			}
		}
	}

	rows, err = db.Query(context.Background(), `SELECT count(*) FROM events WHERE created_at = '2026-05-07T08:30:00.000000123-04:00'`)
	if err != nil {
		t.Fatalf("Query timestamp offset count: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(1) {
		t.Fatalf("timestamp offset count = %#v, want 1", got)
	}

	rows, err = db.Query(context.Background(), `SELECT count(*) FROM events WHERE created_at BETWEEN '2026-05-07T12:00:00Z' AND '2026-05-07T13:00:00Z'`)
	if err != nil {
		t.Fatalf("Query timestamp between count: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(1) {
		t.Fatalf("timestamp between count = %#v, want 1", got)
	}

	rows, err = db.Query(context.Background(), `SELECT event_type FROM events WHERE created_at IN ('2026-05-07T08:30:00.000000123-04:00')`)
	if err != nil {
		t.Fatalf("Query timestamp in scan: %v", err)
	}
	wantRows = [][]any{{"signup"}}
	if got := len(rows.Values); got != len(wantRows) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(wantRows), rows.Values)
	}
	for i := range wantRows {
		for j := range wantRows[i] {
			if rows.Values[i][j] != wantRows[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], wantRows[i][j], rows.Values)
			}
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_date FROM events WHERE event_date >= '2026-05-08' ORDER BY event_date`)
	if err != nil {
		t.Fatalf("Query date scan: %v", err)
	}
	if got, want := rows.Columns, []string{"event_date"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
	wantRows = [][]any{{"2026-05-08"}}
	if got := len(rows.Values); got != len(wantRows) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(wantRows), rows.Values)
	}
	for i := range wantRows {
		for j := range wantRows[i] {
			if rows.Values[i][j] != wantRows[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], wantRows[i][j], rows.Values)
			}
		}
	}

	rows, err = db.Query(context.Background(), `SELECT count(*) FROM events WHERE event_date BETWEEN '2026-05-07' AND '2026-05-07'`)
	if err != nil {
		t.Fatalf("Query date between count: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(1) {
		t.Fatalf("date between count = %#v, want 1", got)
	}

	rows, err = db.Query(context.Background(), `SELECT count(*) FROM events WHERE event_date NOT IN ('2026-05-08')`)
	if err != nil {
		t.Fatalf("Query date not in count: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(1) {
		t.Fatalf("date not in count = %#v, want 1", got)
	}

	rows, err = db.Query(context.Background(), `SELECT event_type FROM events WHERE event_date IN ('2026-05-08')`)
	if err != nil {
		t.Fatalf("Query date in scan: %v", err)
	}
	wantRows = [][]any{{"checkout"}}
	if got := len(rows.Values); got != len(wantRows) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(wantRows), rows.Values)
	}
	for i := range wantRows {
		for j := range wantRows[i] {
			if rows.Values[i][j] != wantRows[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], wantRows[i][j], rows.Values)
			}
		}
	}
}

func TestQueryTemporalGroupBy(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (created_at TIMESTAMP NOT NULL, event_date DATE NOT NULL, score INT64 NOT NULL)`)
	mustExec(t, db, `INSERT INTO events VALUES
		('2026-05-07T12:30:00.000000123Z', '2026-05-07', 10),
		('2026-05-08T00:00:00Z', '2026-05-08', 5),
		('2026-05-07T12:30:00.000000123Z', '2026-05-07', 7)`)
	def, ok := db.Table("events")
	if !ok {
		t.Fatal("events table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO events VALUES
		('2026-05-08T00:00:00Z', '2026-05-08', 3),
		('2026-05-09T01:02:03.000000004Z', '2026-05-09', 11)`)

	rows, err := db.Query(context.Background(), `SELECT event_date, count(*) FROM events GROUP BY event_date ORDER BY event_date`)
	if err != nil {
		t.Fatalf("Query date group count: %v", err)
	}
	want := [][]any{{"2026-05-07", uint64(2)}, {"2026-05-08", uint64(2)}, {"2026-05-09", uint64(1)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_date, count(*) FROM events GROUP BY event_date HAVING event_date BETWEEN '2026-05-07' AND '2026-05-08' ORDER BY event_date`)
	if err != nil {
		t.Fatalf("Query date group having: %v", err)
	}
	want = [][]any{{"2026-05-07", uint64(2)}, {"2026-05-08", uint64(2)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT created_at, sum(score) FROM events GROUP BY created_at ORDER BY created_at`)
	if err != nil {
		t.Fatalf("Query timestamp group sum: %v", err)
	}
	want = [][]any{{"2026-05-07T12:30:00.000000123Z", int64(17)}, {"2026-05-08T00:00:00.000000000Z", int64(8)}, {"2026-05-09T01:02:03.000000004Z", int64(11)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT created_at, sum(score) FROM events GROUP BY created_at HAVING created_at IN ('2026-05-07T12:30:00.000000123Z', '2026-05-08T00:00:00Z') ORDER BY created_at`)
	if err != nil {
		t.Fatalf("Query timestamp group having: %v", err)
	}
	want = [][]any{{"2026-05-07T12:30:00.000000123Z", int64(17)}, {"2026-05-08T00:00:00.000000000Z", int64(8)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

}

func TestExecInsertInt16AndQuery(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (small INT16 NOT NULL, score INT64 NOT NULL, event_type TEXT NOT NULL)`)
	mustExec(t, db, `INSERT INTO events VALUES (3, 10, 'persisted'), (1, 5, 'low')`)
	def, ok := db.Table("events")
	if !ok {
		t.Fatal("events table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO events VALUES (7, 1, 'buffered')`)

	rows, err := db.Query(context.Background(), `SELECT small FROM events ORDER BY small`)
	if err != nil {
		t.Fatalf("Query int16 scan: %v", err)
	}
	if got, want := rows.Columns, []string{"small"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
	wantRows := [][]any{{int16(1)}, {int16(3)}, {int16(7)}}
	if got := len(rows.Values); got != len(wantRows) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(wantRows), rows.Values)
	}
	for i := range wantRows {
		for j := range wantRows[i] {
			if rows.Values[i][j] != wantRows[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], wantRows[i][j], rows.Values)
			}
		}
	}

	rows, err = db.Query(context.Background(), `SELECT small, event_type FROM events WHERE small BETWEEN 2 AND 7 ORDER BY small`)
	if err != nil {
		t.Fatalf("Query int16 between scan: %v", err)
	}
	wantRows = [][]any{{int16(3), "persisted"}, {int16(7), "buffered"}}
	if got := len(rows.Values); got != len(wantRows) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(wantRows), rows.Values)
	}
	for i := range wantRows {
		for j := range wantRows[i] {
			if rows.Values[i][j] != wantRows[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], wantRows[i][j], rows.Values)
			}
		}
	}

	rows, err = db.Query(context.Background(), `SELECT count(*) FROM events WHERE small = 1`)
	if err != nil {
		t.Fatalf("Query int16 count: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(1) {
		t.Fatalf("int16 count = %#v, want 1", got)
	}

	rows, err = db.Query(context.Background(), `SELECT count(*) FROM events WHERE small IN (1, 7)`)
	if err != nil {
		t.Fatalf("Query int16 in count: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(2) {
		t.Fatalf("int16 in count = %#v, want 2", got)
	}

	rows, err = db.Query(context.Background(), `SELECT event_type, small + score AS total FROM events WHERE small + score >= 8 ORDER BY total`)
	if err != nil {
		t.Fatalf("Query int16 arithmetic scan: %v", err)
	}
	wantRows = [][]any{{"buffered", int64(8)}, {"persisted", int64(13)}}
	if got := len(rows.Values); got != len(wantRows) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(wantRows), rows.Values)
	}
	for i := range wantRows {
		for j := range wantRows[i] {
			if rows.Values[i][j] != wantRows[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], wantRows[i][j], rows.Values)
			}
		}
	}
}

func TestOpenRestoresInsertedRows(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir)
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, event_type TEXT NOT NULL)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, 'signup'), (2, 'checkout'), (1, 'login')`)
	_ = db.Close()

	reopened := mustOpen(t, dir)
	rows, err := reopened.Query(context.Background(), `SELECT count(*) FROM events WHERE tenant_id = 1`)
	if err != nil {
		t.Fatalf("Query count where after reopen: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(2) {
		t.Fatalf("count after reopen = %#v, want 2", got)
	}
}

func TestQueryCountTextPredicate(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, event_type TEXT NOT NULL)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, 'signup'), (2, 'checkout'), (3, 'signup')`)

	rows, err := db.Query(context.Background(), `SELECT count(*) FROM events WHERE event_type = 'signup'`)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(2) {
		t.Fatalf("count = %#v, want 2", got)
	}
}

func TestQueryCountInPredicates(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, active BOOL NOT NULL, event_type TEXT NOT NULL)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, true, 'signup'), (2, false, 'checkout'), (3, true, 'login')`)
	def, ok := db.Table("events")
	if !ok {
		t.Fatal("events table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO events VALUES (4, false, 'signup'), (5, true, 'checkout')`)

	tests := []struct {
		name string
		sql  string
		want uint64
	}{
		{name: "int64", sql: `SELECT count(*) FROM events WHERE tenant_id IN (1, 4, 5)`, want: 3},
		{name: "bool", sql: `SELECT count(*) FROM events WHERE active IN (false)`, want: 2},
		{name: "text", sql: `SELECT count(*) FROM events WHERE event_type IN ('signup', 'login')`, want: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(context.Background(), tt.sql)
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			if got := rows.Values[0][0]; got != tt.want {
				t.Fatalf("count = %#v, want %d", got, tt.want)
			}
		})
	}
}

func TestQueryCountNotInPredicates(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, active BOOL NOT NULL, event_type TEXT NOT NULL)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, true, 'signup'), (2, false, 'checkout'), (3, true, 'login')`)
	def, ok := db.Table("events")
	if !ok {
		t.Fatal("events table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO events VALUES (4, false, 'signup'), (5, true, 'checkout')`)

	tests := []struct {
		name string
		sql  string
		want uint64
	}{
		{name: "int64", sql: `SELECT count(*) FROM events WHERE tenant_id NOT IN (1, 4, 5)`, want: 2},
		{name: "bool", sql: `SELECT count(*) FROM events WHERE active NOT IN (false)`, want: 3},
		{name: "text", sql: `SELECT count(*) FROM events WHERE event_type NOT IN ('signup', 'login')`, want: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(context.Background(), tt.sql)
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			if got := rows.Values[0][0]; got != tt.want {
				t.Fatalf("count = %#v, want %d", got, tt.want)
			}
		})
	}
}

func TestQueryNotEqualPredicates(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, active BOOL NOT NULL, event_type TEXT NOT NULL)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, true, 'signup'), (2, false, 'checkout'), (3, true, 'login')`)
	def, ok := db.Table("events")
	if !ok {
		t.Fatal("events table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO events VALUES (4, false, 'signup'), (5, true, 'checkout')`)

	tests := []struct {
		name string
		sql  string
		want uint64
	}{
		{name: "int64", sql: `SELECT count(*) FROM events WHERE tenant_id != 1`, want: 4},
		{name: "int64 alternate", sql: `SELECT count(*) FROM events WHERE tenant_id <> 5`, want: 4},
		{name: "bool", sql: `SELECT count(*) FROM events WHERE active != true`, want: 2},
		{name: "text", sql: `SELECT count(*) FROM events WHERE event_type != 'signup'`, want: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(context.Background(), tt.sql)
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			if got := rows.Values[0][0]; got != tt.want {
				t.Fatalf("count = %#v, want %d", got, tt.want)
			}
		})
	}
}

func TestQueryComparisonPredicates(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, score INT64, event_type TEXT NOT NULL)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, 10, 'signup'), (2, 20, 'checkout'), (3, 5, 'login')`)
	def, ok := db.Table("events")
	if !ok {
		t.Fatal("events table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO events VALUES (4, 7, 'signup'), (5, 3, 'checkout')`)

	tests := []struct {
		name string
		sql  string
		want uint64
	}{
		{name: "less", sql: `SELECT count(*) FROM events WHERE tenant_id < 3`, want: 2},
		{name: "less equal", sql: `SELECT count(*) FROM events WHERE tenant_id <= 3`, want: 3},
		{name: "greater", sql: `SELECT count(*) FROM events WHERE tenant_id > 3`, want: 2},
		{name: "greater equal", sql: `SELECT count(*) FROM events WHERE tenant_id >= 3`, want: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(context.Background(), tt.sql)
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			if got := rows.Values[0][0]; got != tt.want {
				t.Fatalf("count = %#v, want %d", got, tt.want)
			}
		})
	}

	rows, err := db.Query(context.Background(), `SELECT sum(score) FROM events WHERE tenant_id >= 3`)
	if err != nil {
		t.Fatalf("Query sum comparison: %v", err)
	}
	if got := rows.Values[0][0]; got != int64(15) {
		t.Fatalf("sum = %#v, want 15", got)
	}

	rows, err = db.Query(context.Background(), `SELECT event_type, max(score) FROM events WHERE tenant_id <= 4 GROUP BY event_type`)
	if err != nil {
		t.Fatalf("Query group comparison: %v", err)
	}
	want := [][]any{{"checkout", int64(20)}, {"login", int64(5)}, {"signup", int64(10)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type, count(*) FROM events WHERE tenant_id > 1 AND event_type != 'gamma' GROUP BY event_type`)
	if err != nil {
		t.Fatalf("Query group count where and: %v", err)
	}
	want = [][]any{{"checkout", uint64(2)}, {"login", uint64(1)}, {"signup", uint64(1)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

}

func TestQueryAggregateInPredicates(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, score INT64, event_type TEXT NOT NULL)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, 10, 'signup'), (2, 20, 'checkout'), (3, 5, 'login')`)
	def, ok := db.Table("events")
	if !ok {
		t.Fatal("events table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO events VALUES (4, 7, 'signup'), (5, 3, 'checkout')`)

	rows, err := db.Query(context.Background(), `SELECT sum(score) FROM events WHERE tenant_id IN (1, 4, 5)`)
	if err != nil {
		t.Fatalf("Query sum in: %v", err)
	}
	if got := rows.Values[0][0]; got != int64(20) {
		t.Fatalf("sum = %#v, want 20", got)
	}

	rows, err = db.Query(context.Background(), `SELECT event_type, max(score) FROM events WHERE tenant_id IN (1, 2, 4) GROUP BY event_type`)
	if err != nil {
		t.Fatalf("Query group max in: %v", err)
	}
	want := [][]any{{"checkout", int64(20)}, {"signup", int64(10)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type, count(score) FROM events WHERE tenant_id > 1 AND score > 4 GROUP BY event_type`)
	if err != nil {
		t.Fatalf("Query group count column where and: %v", err)
	}
	want = [][]any{{"checkout", uint64(1)}, {"login", uint64(1)}, {"signup", uint64(1)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

}

func TestQueryAggregateNotInPredicates(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, score INT64, event_type TEXT NOT NULL)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, 10, 'signup'), (2, 20, 'checkout'), (3, 5, 'login')`)
	def, ok := db.Table("events")
	if !ok {
		t.Fatal("events table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO events VALUES (4, 7, 'signup'), (5, 3, 'checkout')`)

	rows, err := db.Query(context.Background(), `SELECT sum(score) FROM events WHERE tenant_id NOT IN (2, 3)`)
	if err != nil {
		t.Fatalf("Query sum not in: %v", err)
	}
	if got := rows.Values[0][0]; got != int64(20) {
		t.Fatalf("sum = %#v, want 20", got)
	}

	rows, err = db.Query(context.Background(), `SELECT event_type, max(score) FROM events WHERE tenant_id NOT IN (2, 5) GROUP BY event_type`)
	if err != nil {
		t.Fatalf("Query group max not in: %v", err)
	}
	want := [][]any{{"login", int64(5)}, {"signup", int64(10)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type, sum(score) FROM events WHERE tenant_id = 1 AND score > 6 GROUP BY event_type`)
	if err != nil {
		t.Fatalf("Query group sum where and: %v", err)
	}
	want = [][]any{{"signup", int64(10)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

}

func TestQueryAggregateNotEqualPredicates(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, score INT64, event_type TEXT NOT NULL)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, 10, 'signup'), (2, 20, 'checkout'), (3, 5, 'login')`)
	def, ok := db.Table("events")
	if !ok {
		t.Fatal("events table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO events VALUES (4, 7, 'signup'), (5, 3, 'checkout')`)

	rows, err := db.Query(context.Background(), `SELECT sum(score) FROM events WHERE tenant_id != 2`)
	if err != nil {
		t.Fatalf("Query sum not equal: %v", err)
	}
	if got := rows.Values[0][0]; got != int64(25) {
		t.Fatalf("sum = %#v, want 25", got)
	}

	rows, err = db.Query(context.Background(), `SELECT event_type, count(*) FROM events WHERE event_type != 'login' GROUP BY event_type`)
	if err != nil {
		t.Fatalf("Query group count not equal: %v", err)
	}
	want := [][]any{{"checkout", uint64(2)}, {"signup", uint64(2)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type, min(score) FROM events WHERE tenant_id = 1 AND score > 6 GROUP BY event_type`)
	if err != nil {
		t.Fatalf("Query group min where and: %v", err)
	}
	want = [][]any{{"signup", int64(10)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}
}

func TestQueryGroupStringCounts(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, event_type TEXT)`)
	mustExec(t, db, `INSERT INTO events VALUES
		(1, 'zeta'),
		(2, 'alpha'),
		(3, 'beta'),
		(4, NULL),
		(5, 'alpha'),
		(6, 'gamma'),
		(7, 'beta')`)

	rows, err := db.Query(context.Background(), `SELECT event_type, count(*) FROM events GROUP BY event_type`)
	if err != nil {
		t.Fatalf("Query group by: %v", err)
	}
	if got, want := rows.Columns, []string{"event_type", "count"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}

	want := [][]any{{"alpha", uint64(2)}, {"beta", uint64(2)}, {"gamma", uint64(1)}, {"zeta", uint64(1)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d", got, len(want))
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}
}

func TestQueryGroupNonTextKeys(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, bucket INT32 NOT NULL, small INT16 NOT NULL, active BOOL NOT NULL, score INT64, event_type TEXT)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, 10, 1, true, 5, 'a'), (1, 20, 2, false, 7, 'b'), (2, 10, 1, true, NULL, 'c')`)
	def, ok := db.Table("events")
	if !ok {
		t.Fatal("events table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO events VALUES (2, 20, 2, true, 3, 'd'), (3, 10, 1, false, 11, 'e')`)

	rows, err := db.Query(context.Background(), `SELECT tenant_id, count(*) FROM events GROUP BY tenant_id`)
	if err != nil {
		t.Fatalf("Query int64 group count: %v", err)
	}
	want := [][]any{{int64(1), uint64(2)}, {int64(2), uint64(2)}, {int64(3), uint64(1)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT bucket, sum(score) FROM events GROUP BY bucket`)
	if err != nil {
		t.Fatalf("Query int32 group sum: %v", err)
	}
	want = [][]any{{int32(10), int64(16)}, {int32(20), int64(10)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT small, count(*) FROM events GROUP BY small`)
	if err != nil {
		t.Fatalf("Query int16 group count: %v", err)
	}
	want = [][]any{{int16(1), uint64(3)}, {int16(2), uint64(2)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT active, max(score) FROM events WHERE tenant_id = 1 OR bucket = 10 GROUP BY active ORDER BY active`)
	if err != nil {
		t.Fatalf("Query bool group max where or: %v", err)
	}
	want = [][]any{{false, int64(11)}, {true, int64(5)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}
}

func TestQueryGroupStringCountColumn(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, score INT64, event_type TEXT)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, 10, 'alpha'), (2, NULL, 'alpha'), (3, 5, 'beta')`)
	def, ok := db.Table("events")
	if !ok {
		t.Fatal("events table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO events VALUES (4, 7, 'alpha'), (5, NULL, 'beta'), (6, 3, NULL)`)

	rows, err := db.Query(context.Background(), `SELECT event_type, count(score) FROM events GROUP BY event_type`)
	if err != nil {
		t.Fatalf("Query group count column: %v", err)
	}
	want := [][]any{{"alpha", uint64(2)}, {"beta", uint64(1)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type AS kind, count(score) AS scored FROM events WHERE tenant_id > 2 GROUP BY event_type`)
	if err != nil {
		t.Fatalf("Query group count column where: %v", err)
	}
	if got, wantCols := rows.Columns, []string{"kind", "scored"}; len(got) != len(wantCols) || got[0] != wantCols[0] || got[1] != wantCols[1] {
		t.Fatalf("columns = %#v, want %#v", got, wantCols)
	}
	want = [][]any{{"alpha", uint64(1)}, {"beta", uint64(1)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}
}

func TestQueryGroupStringOrderBy(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, score INT64, event_type TEXT)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, 10, 'beta'), (2, 20, 'alpha'), (3, 5, 'beta'), (4, 7, 'gamma')`)

	rows, err := db.Query(context.Background(), `SELECT event_type, count(*) AS n FROM events GROUP BY event_type ORDER BY n DESC`)
	if err != nil {
		t.Fatalf("Query order by aggregate: %v", err)
	}
	want := [][]any{{"beta", uint64(2)}, {"alpha", uint64(1)}, {"gamma", uint64(1)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type, count(*) AS n FROM events GROUP BY event_type HAVING n > 1 ORDER BY n DESC`)
	if err != nil {
		t.Fatalf("Query count having: %v", err)
	}
	want = [][]any{{"beta", uint64(2)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type, count(*) FROM events GROUP BY event_type HAVING count(*) > 1`)
	if err != nil {
		t.Fatalf("Query count call having: %v", err)
	}
	want = [][]any{{"beta", uint64(2)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type kind, count(*) n FROM events GROUP BY event_type HAVING kind IN ('alpha', 'gamma') ORDER BY kind`)
	if err != nil {
		t.Fatalf("Query group key having: %v", err)
	}
	want = [][]any{{"alpha", uint64(1)}, {"gamma", uint64(1)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type kind, count(*) n FROM events GROUP BY event_type HAVING kind != 'beta' ORDER BY kind`)
	if err != nil {
		t.Fatalf("Query group key having pushdown: %v", err)
	}
	want = [][]any{{"alpha", uint64(1)}, {"gamma", uint64(1)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type kind, count(*) n FROM events GROUP BY event_type HAVING lower(kind) = 'beta'`)
	if err != nil {
		t.Fatalf("Query group key lower having: %v", err)
	}
	want = [][]any{{"beta", uint64(2)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type kind, count(*) n FROM events GROUP BY event_type HAVING kind || ':ok' IN ('alpha:ok', 'gamma:ok') ORDER BY kind`)
	if err != nil {
		t.Fatalf("Query group key concat having: %v", err)
	}
	want = [][]any{{"alpha", uint64(1)}, {"gamma", uint64(1)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT lower(event_type) kind, count(*) n FROM events GROUP BY lower(event_type) HAVING upper(kind) = 'BETA'`)
	if err != nil {
		t.Fatalf("Query computed group key text having: %v", err)
	}
	want = [][]any{{"beta", uint64(2)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type kind, count(*) n FROM events GROUP BY event_type HAVING kind > 'alpha' ORDER BY kind`)
	if err != nil {
		t.Fatalf("Query group key ordering having: %v", err)
	}
	want = [][]any{{"beta", uint64(2)}, {"gamma", uint64(1)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type kind, count(*) n FROM events GROUP BY event_type HAVING kind BETWEEN 'alpha' AND 'beta' ORDER BY kind`)
	if err != nil {
		t.Fatalf("Query group key between having: %v", err)
	}
	want = [][]any{{"alpha", uint64(1)}, {"beta", uint64(2)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type kind, count(*) n FROM events GROUP BY event_type HAVING sum(score) > 10 ORDER BY kind`)
	if err != nil {
		t.Fatalf("Query non-selected aggregate having: %v", err)
	}
	if got, wantCols := rows.Columns, []string{"kind", "n"}; len(got) != len(wantCols) || got[0] != wantCols[0] || got[1] != wantCols[1] {
		t.Fatalf("columns = %#v, want %#v", got, wantCols)
	}
	want = [][]any{{"alpha", uint64(1)}, {"beta", uint64(2)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type kind, count(*) n FROM events GROUP BY event_type HAVING kind = 'alpha' OR count(*) > 1 ORDER BY kind`)
	if err != nil {
		t.Fatalf("Query having or: %v", err)
	}
	want = [][]any{{"alpha", uint64(1)}, {"beta", uint64(2)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type kind, count(*) n FROM events GROUP BY event_type HAVING (kind = 'alpha' OR kind = 'gamma') AND NOT count(*) > 1 ORDER BY kind`)
	if err != nil {
		t.Fatalf("Query having parentheses not: %v", err)
	}
	want = [][]any{{"alpha", uint64(1)}, {"gamma", uint64(1)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type kind, count(*) n FROM events GROUP BY event_type HAVING kind = 'beta' AND count(*) > 1`)
	if err != nil {
		t.Fatalf("Query having and: %v", err)
	}
	want = [][]any{{"beta", uint64(2)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type AS kind, sum(score) AS total FROM events GROUP BY event_type ORDER BY kind DESC`)
	if err != nil {
		t.Fatalf("Query order by group: %v", err)
	}
	want = [][]any{{"gamma", int64(7)}, {"beta", int64(15)}, {"alpha", int64(20)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type kind, sum(score) total FROM events GROUP BY event_type HAVING total >= 10 ORDER BY total DESC LIMIT 1 OFFSET 1`)
	if err != nil {
		t.Fatalf("Query sum having limit offset: %v", err)
	}
	want = [][]any{{"beta", int64(15)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type, sum(score) FROM events GROUP BY event_type HAVING sum(score) >= 10 ORDER BY sum DESC`)
	if err != nil {
		t.Fatalf("Query sum call having: %v", err)
	}
	want = [][]any{{"alpha", int64(20)}, {"beta", int64(15)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type kind, count(*) n FROM events GROUP BY event_type HAVING sum(score) / count(*) > 7 ORDER BY kind`)
	if err != nil {
		t.Fatalf("Query arithmetic aggregate having: %v", err)
	}
	want = [][]any{{"alpha", uint64(1)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type kind, sum(score) total FROM events GROUP BY event_type HAVING total + 1 > 10 ORDER BY total DESC`)
	if err != nil {
		t.Fatalf("Query arithmetic alias having: %v", err)
	}
	want = [][]any{{"alpha", int64(20)}, {"beta", int64(15)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type, count(*) AS n FROM events GROUP BY event_type ORDER BY n + 1 DESC`)
	if err != nil {
		t.Fatalf("Query arithmetic aggregate order: %v", err)
	}
	want = [][]any{{"beta", uint64(2)}, {"alpha", uint64(1)}, {"gamma", uint64(1)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type kind, sum(score) total FROM events GROUP BY event_type HAVING total > 99`)
	if err != nil {
		t.Fatalf("Query having empty: %v", err)
	}
	if got := len(rows.Values); got != 0 {
		t.Fatalf("having empty rows = %d, want 0 (%#v)", got, rows.Values)
	}

	rows, err = db.Query(context.Background(), `SELECT event_type kind, sum(score) total FROM events GROUP BY event_type HAVING total IN (7, 15) ORDER BY total DESC`)
	if err != nil {
		t.Fatalf("Query having in: %v", err)
	}
	want = [][]any{{"beta", int64(15)}, {"gamma", int64(7)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type kind, sum(score) total FROM events GROUP BY event_type ORDER BY total DESC LIMIT 2 OFFSET 1`)
	if err != nil {
		t.Fatalf("Query limit offset: %v", err)
	}
	want = [][]any{{"beta", int64(15)}, {"gamma", int64(7)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type kind, sum(score) total FROM events GROUP BY event_type ORDER BY total DESC OFFSET 1`)
	if err != nil {
		t.Fatalf("Query offset: %v", err)
	}
	want = [][]any{{"beta", int64(15)}, {"gamma", int64(7)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type kind, sum(score) total FROM events GROUP BY event_type ORDER BY total DESC LIMIT 0`)
	if err != nil {
		t.Fatalf("Query limit zero: %v", err)
	}
	if got := len(rows.Values); got != 0 {
		t.Fatalf("limit zero rows = %d, want 0 (%#v)", got, rows.Values)
	}

	rows, err = db.Query(context.Background(), `SELECT event_type kind, sum(score) total FROM events GROUP BY event_type ORDER BY total DESC OFFSET 99`)
	if err != nil {
		t.Fatalf("Query offset beyond rows: %v", err)
	}
	if got := len(rows.Values); got != 0 {
		t.Fatalf("offset beyond rows = %d, want 0 (%#v)", got, rows.Values)
	}
}

func TestQueryGroupStringCountsSortedAcrossPersistedAndBuffer(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, event_type TEXT)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, 'zeta'), (2, 'alpha')`)
	def, ok := db.Table("events")
	if !ok {
		t.Fatal("events table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO events VALUES (3, 'beta'), (4, 'alpha')`)

	rows, err := db.Query(context.Background(), `SELECT event_type, count(*) FROM events GROUP BY event_type`)
	if err != nil {
		t.Fatalf("Query group by: %v", err)
	}
	want := [][]any{{"alpha", uint64(2)}, {"beta", uint64(1)}, {"zeta", uint64(1)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d", got, len(want))
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}
}

func TestQueryGroupStringCountsWhere(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, active BOOL NOT NULL, event_type TEXT)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, true, 'zeta'), (2, false, 'alpha'), (1, true, 'alpha')`)
	def, ok := db.Table("events")
	if !ok {
		t.Fatal("events table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO events VALUES (1, false, 'beta'), (1, true, 'alpha'), (3, true, 'gamma')`)

	tests := []struct {
		name string
		sql  string
		want [][]any
	}{
		{name: "int64 equal", sql: `SELECT event_type, count(*) FROM events WHERE tenant_id = 1 GROUP BY event_type`, want: [][]any{{"alpha", uint64(2)}, {"beta", uint64(1)}, {"zeta", uint64(1)}}},
		{name: "int64 between", sql: `SELECT event_type, count(*) FROM events WHERE tenant_id BETWEEN 2 AND 3 GROUP BY event_type`, want: [][]any{{"alpha", uint64(1)}, {"gamma", uint64(1)}}},
		{name: "bool equal", sql: `SELECT event_type, count(*) FROM events WHERE active = true GROUP BY event_type`, want: [][]any{{"alpha", uint64(2)}, {"gamma", uint64(1)}, {"zeta", uint64(1)}}},
		{name: "text equal", sql: `SELECT event_type, count(*) FROM events WHERE event_type = 'alpha' GROUP BY event_type`, want: [][]any{{"alpha", uint64(3)}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rows, err := db.Query(context.Background(), tt.sql)
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			if got, want := rows.Columns, []string{"event_type", "count"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
				t.Fatalf("columns = %#v, want %#v", got, want)
			}
			if got := len(rows.Values); got != len(tt.want) {
				t.Fatalf("rows = %d, want %d (%#v)", got, len(tt.want), rows.Values)
			}
			for i := range tt.want {
				if rows.Values[i][0] != tt.want[i][0] || rows.Values[i][1] != tt.want[i][1] {
					t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], tt.want[i])
				}
			}
		})
	}
}

func TestQueryGroupStringSums(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, score INT64, event_type TEXT)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, 10, 'zeta'), (2, 20, 'alpha'), (1, NULL, 'alpha')`)
	def, ok := db.Table("events")
	if !ok {
		t.Fatal("events table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO events VALUES (1, 5, 'beta'), (1, 7, 'alpha'), (3, 11, NULL)`)

	rows, err := db.Query(context.Background(), `SELECT event_type, sum(score) FROM events GROUP BY event_type`)
	if err != nil {
		t.Fatalf("Query group sum: %v", err)
	}
	if got, want := rows.Columns, []string{"event_type", "sum"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
	want := [][]any{{"alpha", int64(27)}, {"beta", int64(5)}, {"zeta", int64(10)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type, sum(score) FROM events WHERE tenant_id = 1 GROUP BY event_type`)
	if err != nil {
		t.Fatalf("Query group sum where: %v", err)
	}
	want = [][]any{{"alpha", int64(7)}, {"beta", int64(5)}, {"zeta", int64(10)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}
}

func TestQueryGroupStringMinMax(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, score INT64, event_type TEXT)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, 10, 'zeta'), (2, 20, 'alpha'), (1, NULL, 'alpha'), (1, 5, 'beta')`)
	def, ok := db.Table("events")
	if !ok {
		t.Fatal("events table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO events VALUES (1, 7, 'alpha'), (3, 11, NULL), (2, 30, 'alpha'), (1, 2, 'beta')`)

	rows, err := db.Query(context.Background(), `SELECT event_type, min(score) FROM events GROUP BY event_type`)
	if err != nil {
		t.Fatalf("Query group min: %v", err)
	}
	if got, want := rows.Columns, []string{"event_type", "min"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
	want := [][]any{{"alpha", int64(7)}, {"beta", int64(2)}, {"zeta", int64(10)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type, max(score) FROM events WHERE tenant_id = 1 GROUP BY event_type`)
	if err != nil {
		t.Fatalf("Query group max where: %v", err)
	}
	if got, want := rows.Columns, []string{"event_type", "max"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
	want = [][]any{{"alpha", int64(7)}, {"beta", int64(5)}, {"zeta", int64(10)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}
}

func TestQueryScanProjection(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, event_type TEXT, active BOOL, score INT32)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, 'signup', true, 10), (2, 'checkout', false, 20), (1, 'login', true, NULL)`)
	def, ok := db.Table("events")
	if !ok {
		t.Fatal("events table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO events VALUES (3, 'return', true, 30), (1, NULL, false, 40)`)

	rows, err := db.Query(context.Background(), `SELECT tenant_id, event_type AS kind, score FROM events WHERE active = true OR tenant_id = 2 LIMIT 3 OFFSET 1`)
	if err != nil {
		t.Fatalf("Query scan projection: %v", err)
	}
	if got, want := rows.Columns, []string{"tenant_id", "kind", "score"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
	want := [][]any{{int64(2), "checkout", int32(20)}, {int64(1), "login", nil}, {int64(3), "return", int32(30)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		for j := range want[i] {
			if rows.Values[i][j] != want[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], want[i][j], rows.Values)
			}
		}
	}
}

func TestQueryScanProjectionOrderBy(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, event_type TEXT, active BOOL, score INT32)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, 'signup', true, 10), (2, 'checkout', false, 20), (1, 'login', true, NULL)`)
	def, ok := db.Table("events")
	if !ok {
		t.Fatal("events table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO events VALUES (3, 'return', true, 30), (1, NULL, false, 40)`)

	rows, err := db.Query(context.Background(), `SELECT tenant_id, event_type FROM events ORDER BY tenant_id DESC`)
	if err != nil {
		t.Fatalf("Query scan order desc: %v", err)
	}
	want := [][]any{{int64(3), "return"}, {int64(2), "checkout"}, {int64(1), "signup"}, {int64(1), "login"}, {int64(1), nil}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("desc rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		for j := range want[i] {
			if rows.Values[i][j] != want[i][j] {
				t.Fatalf("desc row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], want[i][j], rows.Values)
			}
		}
	}

	rows, err = db.Query(context.Background(), `SELECT tenant_id, event_type AS kind FROM events ORDER BY kind ASC LIMIT 2 OFFSET 1`)
	if err != nil {
		t.Fatalf("Query scan order alias: %v", err)
	}
	if got, wantCols := rows.Columns, []string{"tenant_id", "kind"}; len(got) != len(wantCols) || got[0] != wantCols[0] || got[1] != wantCols[1] {
		t.Fatalf("columns = %#v, want %#v", got, wantCols)
	}
	want = [][]any{{int64(1), "login"}, {int64(3), "return"}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("alias rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		for j := range want[i] {
			if rows.Values[i][j] != want[i][j] {
				t.Fatalf("alias row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], want[i][j], rows.Values)
			}
		}
	}
}

func TestQueryScanSelectStar(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, event_type TEXT, active BOOL, score INT32)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, 'signup', true, 10), (2, 'checkout', false, 20), (1, 'login', true, NULL)`)

	rows, err := db.Query(context.Background(), `SELECT * FROM events WHERE active = true ORDER BY event_type DESC LIMIT 2`)
	if err != nil {
		t.Fatalf("Query scan star: %v", err)
	}
	if got, want := rows.Columns, []string{"tenant_id", "event_type", "active", "score"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] || got[3] != want[3] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
	want := [][]any{{int64(1), "signup", true, int32(10)}, {int64(1), "login", true, nil}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		for j := range want[i] {
			if rows.Values[i][j] != want[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], want[i][j], rows.Values)
			}
		}
	}
}

func TestQueryScanCalculatorProjection(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64 NOT NULL, event_type TEXT, active BOOL, score INT32)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, 'signup', true, 10), (2, 'checkout', false, 20), (1, 'login', true, NULL)`)

	rows, err := db.Query(context.Background(), `SELECT tenant_id, 'active' AS label, 1 AS version, true AS ok, NULL AS missing FROM events WHERE active = true ORDER BY label LIMIT 2`)
	if err != nil {
		t.Fatalf("Query scan literal projection: %v", err)
	}
	if got, want := rows.Columns, []string{"tenant_id", "label", "version", "ok", "missing"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] || got[3] != want[3] || got[4] != want[4] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
	want := [][]any{{int64(1), "active", int64(1), true, nil}, {int64(1), "active", int64(1), true, nil}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		for j := range want[i] {
			if rows.Values[i][j] != want[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], want[i][j], rows.Values)
			}
		}
	}

	rows, err = db.Query(context.Background(), `SELECT 'active' AS label, 1 AS version FROM events WHERE active = true ORDER BY version DESC LIMIT 2`)
	if err != nil {
		t.Fatalf("Query scan literal-only projection: %v", err)
	}
	if got, want := rows.Columns, []string{"label", "version"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
	want = [][]any{{"active", int64(1)}, {"active", int64(1)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		for j := range want[i] {
			if rows.Values[i][j] != want[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], want[i][j], rows.Values)
			}
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type, score + 1 AS next_score, 100 - score AS remaining, score + tenant_id AS total FROM events ORDER BY next_score DESC LIMIT 3`)
	if err != nil {
		t.Fatalf("Query scan computed projection: %v", err)
	}
	if got, want := rows.Columns, []string{"event_type", "next_score", "remaining", "total"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] || got[3] != want[3] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
	want = [][]any{{"checkout", int64(21), int64(80), int64(22)}, {"signup", int64(11), int64(90), int64(11)}, {"login", nil, nil, nil}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		for j := range want[i] {
			if rows.Values[i][j] != want[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], want[i][j], rows.Values)
			}
		}
	}

	rows, err = db.Query(context.Background(), `SELECT lower(event_type) AS kind, upper(event_type) AS loud, event_type || ':' || 'done' AS label, concat(event_type, ':v') AS tagged FROM events WHERE lower(event_type) = 'signup'`)
	if err != nil {
		t.Fatalf("Query scan text functions projection: %v", err)
	}
	if got, want := rows.Columns, []string{"kind", "loud", "label", "tagged"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] || got[3] != want[3] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
	want = [][]any{{"signup", "SIGNUP", "signup:done", "signup:v"}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		for j := range want[i] {
			if rows.Values[i][j] != want[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], want[i][j], rows.Values)
			}
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type FROM events WHERE concat(event_type, ':x') IN ('checkout:x')`)
	if err != nil {
		t.Fatalf("Query scan text concat where: %v", err)
	}
	want = [][]any{{"checkout"}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		for j := range want[i] {
			if rows.Values[i][j] != want[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], want[i][j], rows.Values)
			}
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type FROM events ORDER BY upper(event_type) DESC LIMIT 2`)
	if err != nil {
		t.Fatalf("Query scan text function order: %v", err)
	}
	want = [][]any{{"signup"}, {"login"}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		for j := range want[i] {
			if rows.Values[i][j] != want[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], want[i][j], rows.Values)
			}
		}
	}

	rows, err = db.Query(context.Background(), `SELECT lower(event_type) AS kind, count(*) AS n FROM events GROUP BY lower(event_type) ORDER BY kind`)
	if err != nil {
		t.Fatalf("Query grouped text function: %v", err)
	}
	if got, want := rows.Columns, []string{"kind", "n"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
	want = [][]any{{"checkout", uint64(1)}, {"login", uint64(1)}, {"signup", uint64(1)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		for j := range want[i] {
			if rows.Values[i][j] != want[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], want[i][j], rows.Values)
			}
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type FROM events ORDER BY score + tenant_id DESC LIMIT 2`)
	if err != nil {
		t.Fatalf("Query scan arithmetic order: %v", err)
	}
	if got, want := rows.Columns, []string{"event_type"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
	want = [][]any{{"checkout"}, {"signup"}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		for j := range want[i] {
			if rows.Values[i][j] != want[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], want[i][j], rows.Values)
			}
		}
	}

	rows, err = db.Query(context.Background(), `SELECT (5 * 2 - 6) / 2 AS result, 11 % 5 AS remainder, 11 MOD 5 AS modded, 10 DIV 4 AS quotient FROM events LIMIT 1`)
	if err != nil {
		t.Fatalf("Query scan literal arithmetic projection: %v", err)
	}
	if got, want := rows.Columns, []string{"result", "remainder", "modded", "quotient"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] || got[3] != want[3] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
	want = [][]any{{int64(2), int64(1), int64(1), int64(2)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		for j := range want[i] {
			if rows.Values[i][j] != want[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], want[i][j], rows.Values)
			}
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type, score * 2 AS doubled, (score + tenant_id) % 4 AS bucket, 100 DIV tenant_id AS share FROM events WHERE score > 0 ORDER BY doubled DESC`)
	if err != nil {
		t.Fatalf("Query scan mixed arithmetic projection: %v", err)
	}
	if got, want := rows.Columns, []string{"event_type", "doubled", "bucket", "share"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] || got[3] != want[3] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
	want = [][]any{{"checkout", int64(40), int64(2), int64(50)}, {"signup", int64(20), int64(3), int64(100)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		for j := range want[i] {
			if rows.Values[i][j] != want[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], want[i][j], rows.Values)
			}
		}
	}

	rows, err = db.Query(context.Background(), `SELECT event_type FROM events WHERE score * 2 >= 20 ORDER BY event_type`)
	if err != nil {
		t.Fatalf("Query scan arithmetic where: %v", err)
	}
	if got, want := rows.Columns, []string{"event_type"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
	want = [][]any{{"checkout"}, {"signup"}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		for j := range want[i] {
			if rows.Values[i][j] != want[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], want[i][j], rows.Values)
			}
		}
	}

	rows, err = db.Query(context.Background(), `SELECT count(*) FROM events WHERE score + tenant_id > 11`)
	if err != nil {
		t.Fatalf("Query aggregate arithmetic where count: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(1) {
		t.Fatalf("count arithmetic where = %#v, want 1", got)
	}

	rows, err = db.Query(context.Background(), `SELECT sum(score) FROM events WHERE score / 10 = 2`)
	if err != nil {
		t.Fatalf("Query aggregate arithmetic where sum: %v", err)
	}
	if got := rows.Values[0][0]; got != int64(20) {
		t.Fatalf("sum arithmetic where = %#v, want 20", got)
	}

	rows, err = db.Query(context.Background(), `SELECT active, count(*) FROM events WHERE score + tenant_id > 11 GROUP BY active`)
	if err != nil {
		t.Fatalf("Query grouped arithmetic where: %v", err)
	}
	if got, want := rows.Columns, []string{"active", "count"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
	want = [][]any{{false, uint64(1)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		for j := range want[i] {
			if rows.Values[i][j] != want[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], want[i][j], rows.Values)
			}
		}
	}

	rows, err = db.Query(context.Background(), `SELECT score + tenant_id AS bucket, count(*) AS n FROM events WHERE score > 0 GROUP BY score + tenant_id ORDER BY bucket`)
	if err != nil {
		t.Fatalf("Query computed group expression: %v", err)
	}
	if got, want := rows.Columns, []string{"bucket", "n"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
	want = [][]any{{int64(11), uint64(1)}, {int64(22), uint64(1)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		for j := range want[i] {
			if rows.Values[i][j] != want[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], want[i][j], rows.Values)
			}
		}
	}
}

func TestQueryFloatColumnsAndExpressions(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE metrics (id INT64 NOT NULL, f32 FLOAT32, f64 FLOAT64, label TEXT)`)
	mustExec(t, db, `INSERT INTO metrics VALUES (1, 1.5, 2.25, 'persisted'), (2, 3.25, 4.5, 'skip'), (3, NULL, NULL, 'nulls')`)
	def, ok := db.Table("metrics")
	if !ok {
		t.Fatal("metrics table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO metrics VALUES (4, 5.5, 6.75, 'buffered')`)

	rows, err := db.Query(context.Background(), `SELECT id, f32, f64 FROM metrics WHERE f64 >= 2.5 ORDER BY f64`)
	if err != nil {
		t.Fatalf("Query float scan: %v", err)
	}
	if got, want := rows.Columns, []string{"id", "f32", "f64"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
	want := [][]any{{int64(2), float32(3.25), float64(4.5)}, {int64(4), float32(5.5), float64(6.75)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		for j := range want[i] {
			if rows.Values[i][j] != want[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], want[i][j], rows.Values)
			}
		}
	}

	rows, err = db.Query(context.Background(), `SELECT label, f64 + 1.25 AS adjusted, f32 * 2.0 AS doubled FROM metrics WHERE f32 + 1.0 > 4.0 ORDER BY adjusted DESC`)
	if err != nil {
		t.Fatalf("Query float arithmetic scan: %v", err)
	}
	want = [][]any{{"buffered", float64(8), float64(11)}, {"skip", float64(5.75), float64(6.5)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("arithmetic rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		for j := range want[i] {
			if rows.Values[i][j] != want[i][j] {
				t.Fatalf("arithmetic row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], want[i][j], rows.Values)
			}
		}
	}

	rows, err = db.Query(context.Background(), `SELECT label FROM metrics WHERE f64 BETWEEN 2.0 AND 5.0 ORDER BY f64 DESC`)
	if err != nil {
		t.Fatalf("Query float between: %v", err)
	}
	want = [][]any{{"skip"}, {"persisted"}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("between rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] {
			t.Fatalf("between row %d = %#v, want %#v (rows=%#v)", i, rows.Values[i], want[i], rows.Values)
		}
	}

	rows, err = db.Query(context.Background(), `SELECT id FROM metrics WHERE f64 IN (2.25, 6.75) ORDER BY id`)
	if err != nil {
		t.Fatalf("Query float in: %v", err)
	}
	want = [][]any{{int64(1)}, {int64(4)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("in rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] {
			t.Fatalf("in row %d = %#v, want %#v (rows=%#v)", i, rows.Values[i], want[i], rows.Values)
		}
	}

	rows, err = db.Query(context.Background(), `SELECT count(*) FROM metrics WHERE f64 > 4.0`)
	if err != nil {
		t.Fatalf("Query float aggregate filter: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(2) {
		t.Fatalf("float count = %#v, want 2", got)
	}
}

func TestQueryUUIDColumnsAndPredicates(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE events (id UUID, event_type TEXT)`)
	mustExec(t, db, `INSERT INTO events VALUES ('550e8400-e29b-41d4-a716-446655440000', 'persisted'), ('550e8400-e29b-41d4-a716-446655440001', 'skip')`)
	def, ok := db.Table("events")
	if !ok {
		t.Fatal("events table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO events VALUES ('550e8400-e29b-41d4-a716-446655440002', 'buffered')`)

	rows, err := db.Query(context.Background(), `SELECT id, event_type FROM events WHERE id IN ('550e8400-e29b-41d4-a716-446655440000', '550e8400-e29b-41d4-a716-446655440002') ORDER BY id`)
	if err != nil {
		t.Fatalf("Query uuid in: %v", err)
	}
	if got, want := rows.Columns, []string{"id", "event_type"}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("columns = %#v, want %#v", got, want)
	}
	want := [][]any{
		{"550e8400-e29b-41d4-a716-446655440000", "persisted"},
		{"550e8400-e29b-41d4-a716-446655440002", "buffered"},
	}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		for j := range want[i] {
			if rows.Values[i][j] != want[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], want[i][j], rows.Values)
			}
		}
	}

	rows, err = db.Query(context.Background(), `SELECT count(*) FROM events WHERE id != '550e8400-e29b-41d4-a716-446655440001'`)
	if err != nil {
		t.Fatalf("Query uuid count: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(2) {
		t.Fatalf("uuid count = %#v, want 2", got)
	}
}

func TestQueryBytesColumnsAndPredicates(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE blobs (payload BYTES, label TEXT)`)
	mustExec(t, db, `INSERT INTO blobs VALUES ('aa', 'persisted'), ('bb', 'skip')`)
	def, ok := db.Table("blobs")
	if !ok {
		t.Fatal("blobs table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO blobs VALUES ('cc', 'buffered')`)

	rows, err := db.Query(context.Background(), `SELECT payload, label FROM blobs WHERE payload IN ('aa', 'cc') ORDER BY payload`)
	if err != nil {
		t.Fatalf("Query bytes in: %v", err)
	}
	want := [][]any{{"aa", "persisted"}, {"cc", "buffered"}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		for j := range want[i] {
			if rows.Values[i][j] != want[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], want[i][j], rows.Values)
			}
		}
	}

	rows, err = db.Query(context.Background(), `SELECT label FROM blobs WHERE payload NOT IN ('bb') ORDER BY label`)
	if err != nil {
		t.Fatalf("Query bytes not in: %v", err)
	}
	want = [][]any{{"buffered"}, {"persisted"}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("not in rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] {
			t.Fatalf("not in row %d = %#v, want %#v (rows=%#v)", i, rows.Values[i], want[i], rows.Values)
		}
	}

	rows, err = db.Query(context.Background(), `SELECT count(*) FROM blobs WHERE payload != 'bb'`)
	if err != nil {
		t.Fatalf("Query bytes count: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(2) {
		t.Fatalf("bytes count = %#v, want 2", got)
	}
}

func TestQueryUUIDBytesGroupBy(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TABLE uuid_events (id UUID, score INT64)`)
	mustExec(t, db, `INSERT INTO uuid_events VALUES
		('550e8400-e29b-41d4-a716-446655440000', 10),
		('550e8400-e29b-41d4-a716-446655440001', 5),
		('550e8400-e29b-41d4-a716-446655440000', 7)`)
	def, ok := db.Table("uuid_events")
	if !ok {
		t.Fatal("uuid_events table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered uuid_events: %v", err)
	}
	mustExec(t, db, `INSERT INTO uuid_events VALUES
		('550e8400-e29b-41d4-a716-446655440001', 3),
		('550e8400-e29b-41d4-a716-446655440002', 11)`)

	rows, err := db.Query(context.Background(), `SELECT id, sum(score) FROM uuid_events GROUP BY id ORDER BY id`)
	if err != nil {
		t.Fatalf("Query uuid group sum: %v", err)
	}
	want := [][]any{{"550e8400-e29b-41d4-a716-446655440000", int64(17)}, {"550e8400-e29b-41d4-a716-446655440001", int64(8)}, {"550e8400-e29b-41d4-a716-446655440002", int64(11)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT id, sum(score) FROM uuid_events GROUP BY id HAVING id IN ('550E8400-E29B-41D4-A716-446655440000', '550e8400-e29b-41d4-a716-446655440002') ORDER BY id`)
	if err != nil {
		t.Fatalf("Query uuid group having: %v", err)
	}
	want = [][]any{{"550e8400-e29b-41d4-a716-446655440000", int64(17)}, {"550e8400-e29b-41d4-a716-446655440002", int64(11)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	mustExec(t, db, `CREATE TABLE blobs_grouped (payload BYTES, score INT64)`)
	mustExec(t, db, `INSERT INTO blobs_grouped VALUES ('aa', 10), ('bb', 5), ('aa', 7)`)
	blobsDef, ok := db.Table("blobs_grouped")
	if !ok {
		t.Fatal("blobs_grouped table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), blobsDef); err != nil {
		t.Fatalf("FlushBuffered blobs_grouped: %v", err)
	}
	mustExec(t, db, `INSERT INTO blobs_grouped VALUES ('bb', 3), ('cc', 11)`)

	rows, err = db.Query(context.Background(), `SELECT payload, count(*) FROM blobs_grouped GROUP BY payload ORDER BY payload`)
	if err != nil {
		t.Fatalf("Query bytes group count: %v", err)
	}
	want = [][]any{{"aa", uint64(2)}, {"bb", uint64(2)}, {"cc", uint64(1)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT payload, count(*) FROM blobs_grouped GROUP BY payload HAVING payload NOT IN ('bb') ORDER BY payload`)
	if err != nil {
		t.Fatalf("Query bytes group having: %v", err)
	}
	want = [][]any{{"aa", uint64(2)}, {"cc", uint64(1)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}
}

func TestQueryEnumColumnsAndPredicates(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TYPE event_status AS ENUM ('new', 'done', 'archived')`)
	mustExec(t, db, `CREATE TABLE events (status event_status, event_type TEXT)`)
	mustExec(t, db, `INSERT INTO events VALUES ('new', 'persisted'), ('done', 'skip')`)
	def, ok := db.Table("events")
	if !ok {
		t.Fatal("events table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO events VALUES ('archived', 'buffered')`)

	rows, err := db.Query(context.Background(), `SELECT status, event_type FROM events WHERE status IN ('new', 'archived') ORDER BY status`)
	if err != nil {
		t.Fatalf("Query enum in: %v", err)
	}
	want := [][]any{{"archived", "buffered"}, {"new", "persisted"}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		for j := range want[i] {
			if rows.Values[i][j] != want[i][j] {
				t.Fatalf("row %d column %d = %#v, want %#v (rows=%#v)", i, j, rows.Values[i][j], want[i][j], rows.Values)
			}
		}
	}

	rows, err = db.Query(context.Background(), `SELECT count(*) FROM events WHERE status != 'done'`)
	if err != nil {
		t.Fatalf("Query enum count: %v", err)
	}
	if got := rows.Values[0][0]; got != uint64(2) {
		t.Fatalf("enum count = %#v, want 2", got)
	}

	_, err = db.Exec(context.Background(), `INSERT INTO events VALUES ('missing', 'bad')`)
	if err == nil || !strings.Contains(err.Error(), "invalid enum label") {
		t.Fatalf("enum insert error = %v", err)
	}
}

func TestQueryEnumGroupBy(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	mustExec(t, db, `CREATE TYPE event_status AS ENUM ('new', 'done', 'archived')`)
	mustExec(t, db, `CREATE TABLE events (tenant_id INT64, status event_status, score INT64)`)
	mustExec(t, db, `INSERT INTO events VALUES (1, 'new', 10), (1, 'done', 5), (2, 'new', 7)`)
	def, ok := db.Table("events")
	if !ok {
		t.Fatal("events table not found")
	}
	if err := db.data.FlushBuffered(context.Background(), def); err != nil {
		t.Fatalf("FlushBuffered: %v", err)
	}
	mustExec(t, db, `INSERT INTO events VALUES (1, 'archived', 3), (2, 'done', 11)`)

	rows, err := db.Query(context.Background(), `SELECT status, count(*) AS n FROM events GROUP BY status HAVING n > 1 ORDER BY status`)
	if err != nil {
		t.Fatalf("Query enum group count: %v", err)
	}
	want := [][]any{{"done", uint64(2)}, {"new", uint64(2)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT status, sum(score) AS total FROM events WHERE status IN ('new', 'done') GROUP BY status ORDER BY total DESC`)
	if err != nil {
		t.Fatalf("Query enum group sum: %v", err)
	}
	want = [][]any{{"new", int64(17)}, {"done", int64(16)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT status, count(*) AS n FROM events GROUP BY status HAVING status IN ('new', 'archived') ORDER BY status`)
	if err != nil {
		t.Fatalf("Query enum group having in: %v", err)
	}
	want = [][]any{{"archived", uint64(1)}, {"new", uint64(2)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	rows, err = db.Query(context.Background(), `SELECT status, count(*) AS n FROM events GROUP BY status HAVING status != 'done' ORDER BY status`)
	if err != nil {
		t.Fatalf("Query enum group having not equal: %v", err)
	}
	want = [][]any{{"archived", uint64(1)}, {"new", uint64(2)}}
	if got := len(rows.Values); got != len(want) {
		t.Fatalf("rows = %d, want %d (%#v)", got, len(want), rows.Values)
	}
	for i := range want {
		if rows.Values[i][0] != want[i][0] || rows.Values[i][1] != want[i][1] {
			t.Fatalf("row %d = %#v, want %#v", i, rows.Values[i], want[i])
		}
	}

	_, err = db.Query(context.Background(), `SELECT status, count(*) FROM events GROUP BY status HAVING status = 'missing'`)
	if err == nil || !strings.Contains(err.Error(), "invalid enum label") {
		t.Fatalf("enum group having invalid label error = %v", err)
	}
}

func TestOpenRestoresCatalog(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir)
	mustExec(t, db, `
		CREATE TYPE event_status AS ENUM ('new', 'done');
		CREATE TABLE all_types (
			b BOOL,
			i16 INT16,
			i32 INT32,
			i64 INT64,
			f32 FLOAT32,
			f64 FLOAT64,
			dec DECIMAL,
			txt TEXT,
			bin BYTES,
			id UUID,
			ts TIMESTAMP,
			tm TIME,
			d DATE,
			j JSON,
			status event_status
		);
	`)
	_ = db.Close()

	reopened := mustOpen(t, dir)
	if typ, ok := reopened.Type("event_status"); !ok || len(typ.Labels) != 2 {
		t.Fatalf("type = %#v ok=%v", typ, ok)
	}
	if def, ok := reopened.Table("all_types"); !ok || len(def.Columns) != 15 {
		t.Fatalf("table = %#v ok=%v", def, ok)
	} else if got := def.Columns[14].Labels; len(got) != 2 || got[0] != "new" || got[1] != "done" {
		t.Fatalf("enum labels = %#v", got)
	}
}

func TestCreateTableRejectsClosedDB(t *testing.T) {
	db := mustOpen(t, t.TempDir())
	_ = db.Close()
	err := db.CreateTable(context.Background(), schema.TableSpec{Name: "events", Columns: []schema.ColumnSpec{{Name: "tenant_id", Type: sqltype.Int64}}})
	if err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("error = %v", err)
	}
}

func allBuiltInColumns() []schema.ColumnSpec {
	return []schema.ColumnSpec{
		{Name: "b", Type: sqltype.Bool},
		{Name: "i16", Type: sqltype.Int16},
		{Name: "i32", Type: sqltype.Int32},
		{Name: "i64", Type: sqltype.Int64},
		{Name: "f32", Type: sqltype.Float32},
		{Name: "f64", Type: sqltype.Float64},
		{Name: "dec", Type: sqltype.Decimal},
		{Name: "txt", Type: sqltype.Text},
		{Name: "bin", Type: sqltype.Bytes},
		{Name: "id", Type: sqltype.UUID},
		{Name: "ts", Type: sqltype.Timestamp},
		{Name: "tm", Type: sqltype.Time},
		{Name: "d", Type: sqltype.Date},
		{Name: "j", Type: sqltype.JSON},
		{Name: "status", Type: sqltype.Named("event_status")},
	}
}
