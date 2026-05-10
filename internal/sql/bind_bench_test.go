package sql

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

var sqlBenchSink any

type bindBenchQueryShape struct {
	name       string
	sql        string
	wantAgg    AggregateFunc
	wantArg    string
	wantGroup  string
	wantWhere  BoundOp
	wantHidden int
}

func TestBindBenchQueryShapes(t *testing.T) {
	for _, tt := range bindBenchQueryShapes() {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := ParseOne(tt.sql)
			if err != nil {
				t.Fatalf("ParseOne: %v", err)
			}
			selectStmt, ok := stmt.(*SelectStmt)
			if !ok {
				t.Fatalf("stmt = %#v", stmt)
			}
			plan, err := BindSelect(selectStmt, benchEventsDef())
			if err != nil {
				t.Fatalf("BindSelect: %v", err)
			}
			project, ok := plan.(*ProjectPlan)
			if !ok {
				t.Fatalf("plan = %#v", plan)
			}
			agg, ok := project.Source.(*AggregatePlan)
			if !ok || len(agg.Aggregates) != 1 {
				t.Fatalf("aggregate = %#v", project.Source)
			}
			if agg.Aggregates[0].Func != tt.wantAgg || agg.Aggregates[0].ArgName != tt.wantArg {
				t.Fatalf("aggregate spec = %#v", agg.Aggregates[0])
			}
			if tt.wantGroup != "" {
				if len(agg.GroupBy) != 1 || agg.GroupBy[0].Column != tt.wantGroup {
					t.Fatalf("group = %#v", agg.GroupBy)
				}
			}
			scan, ok := agg.Source.(*ScanPlan)
			if !ok || scan.Where == nil || scan.Where.Op != tt.wantWhere {
				t.Fatalf("scan = %#v", agg.Source)
			}
		})
	}
}

func BenchmarkParseQueryShapes(b *testing.B) {
	for _, tt := range bindBenchQueryShapes() {
		b.Run(tt.name, func(b *testing.B) {
			var sink Stmt
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				stmt, err := ParseOne(tt.sql)
				if err != nil {
					b.Fatal(err)
				}
				sink = stmt
			}
			sqlBenchSink = sink
		})
	}
}

func BenchmarkBindQueryShapes(b *testing.B) {
	def := benchEventsDef()
	for _, tt := range bindBenchQueryShapes() {
		stmt, err := ParseOne(tt.sql)
		if err != nil {
			b.Fatalf("ParseOne %q: %v", tt.name, err)
		}
		selectStmt, ok := stmt.(*SelectStmt)
		if !ok {
			b.Fatalf("stmt %q = %#v", tt.name, stmt)
		}
		b.Run(tt.name, func(b *testing.B) {
			var sink Plan
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				plan, err := BindSelect(selectStmt, def)
				if err != nil {
					b.Fatal(err)
				}
				sink = plan
			}
			sqlBenchSink = sink
		})
	}
}

func BenchmarkParseBindQueryShapes(b *testing.B) {
	def := benchEventsDef()
	for _, tt := range bindBenchQueryShapes() {
		b.Run(tt.name, func(b *testing.B) {
			var sink Plan
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				stmt, err := ParseOne(tt.sql)
				if err != nil {
					b.Fatal(err)
				}
				selectStmt, ok := stmt.(*SelectStmt)
				if !ok {
					b.Fatalf("stmt = %#v", stmt)
				}
				plan, err := BindSelect(selectStmt, def)
				if err != nil {
					b.Fatal(err)
				}
				sink = plan
			}
			sqlBenchSink = sink
		})
	}
}

func bindBenchQueryShapes() []bindBenchQueryShape {
	return []bindBenchQueryShape{
		{name: "event checkout for tenant", sql: "SELECT count(*) FROM events WHERE tenant_id = 42 AND event_type = 'checkout'", wantAgg: AggregateCount, wantWhere: BoundOpAnd},
		{name: "absent tenant", sql: "SELECT count(*) FROM events WHERE tenant_id = 999999", wantAgg: AggregateCount, wantWhere: BoundOpEqual},
		{name: "checkout amount for tenant", sql: "SELECT sum(amount) FROM events WHERE tenant_id = 42 AND event_type = 'checkout'", wantAgg: AggregateSum, wantArg: "amount", wantWhere: BoundOpAnd},
		{name: "checkout counts by country", sql: "SELECT country, count(*) FROM events WHERE event_type = 'checkout' GROUP BY country", wantAgg: AggregateCount, wantGroup: "country", wantWhere: BoundOpEqual},
	}
}

func benchEventsDef() BoundTableDef {
	return BoundTableDef{
		Name: "events",
		Columns: []BoundColumnDef{
			{ID: 1, Name: "tenant_id", Type: types.Int64},
			{ID: 2, Name: "event_type", Type: types.Text},
			{ID: 3, Name: "amount", Type: types.Int64},
			{ID: 4, Name: "country", Type: types.Text},
		},
	}
}
