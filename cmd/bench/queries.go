package main

// query is one entry in the registered query set the bench driver runs.
// Names match the design note examples; SQL is the raw text fed to engine.DB.
type query struct {
	Name string
	SQL  string
}

var registeredQueries = []query{
	{
		Name: "event checkout for tenant",
		SQL:  "SELECT count(*) FROM events WHERE tenant_id = 42 AND event_type = 'checkout'",
	},
	{
		Name: "absent tenant",
		SQL:  "SELECT count(*) FROM events WHERE tenant_id = 999999",
	},
	{
		Name: "checkout amount for tenant",
		SQL:  "SELECT sum(amount) FROM events WHERE tenant_id = 42 AND event_type = 'checkout'",
	},
	{
		Name: "checkout counts by country",
		SQL:  "SELECT country, count(*) FROM events WHERE event_type = 'checkout' GROUP BY country",
	},
}
