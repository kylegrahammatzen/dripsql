package sql

import "testing"

func BenchmarkParseStatementShapes(b *testing.B) {
	for _, tt := range []struct {
		name string
		sql  string
	}{
		{name: "create table", sql: "create table if not exists events (tenant_id int64 not null, event_type text, status event_status) with (storage=columnar, segment_rows=100000, compression='auto')"},
		{name: "insert two rows", sql: "insert into events (tenant_id, event_type, active) values (1, 'signup', true), (2, 'checkout', null)"},
		{name: "select scan", sql: "select tenant_id, lower(event_type) as kind from events where tenant_id + 1 >= 10 and event_type not in ('debug') order by kind desc limit 5 offset 2"},
		{name: "aggregate having", sql: "select event_type as kind, count(*) as n from events group by event_type having n > 1 order by n desc"},
		{name: "multi order by", sql: "select tenant_id, event_type as kind, lower(country) from events order by tenant_id desc, lower(country), tenant_id + 1"},
		{name: "boolean precedence", sql: "select count(*) from events where active = true or tenant_id = 1 and not event_type = 'debug'"},
	} {
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
