package main

import (
	"bytes"
	"encoding/json"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestBenchSmokeProducesQueryResults(t *testing.T) {
	var buf bytes.Buffer
	args := []string{"-rows", "2000", "-runs", "2", "-dir", t.TempDir()}
	if err := run(args, &buf); err != nil {
		t.Fatalf("run: %v\n%s", err, buf.String())
	}
	out := buf.String()
	for _, want := range []string{"DripSQL v3 Benchmark", "Setup", "Existing rows:", "Target rows:", "Appended rows:", "Wall:", "Generate workers:", "Append calls:", "Flush/seal wait:", "Storage", "Query Benchmark", "event checkout for tenant", "user id lookup", "uuid lookup"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in output\n%s", want, out)
		}
	}
}

func TestParseOptionsDefaultsToBenchmarkRows(t *testing.T) {
	got, err := parseOptions(nil)
	if err != nil {
		t.Fatalf("parseOptions: %v", err)
	}
	want := defaultBenchmarkRows
	if len(got.rows) != len(want) {
		t.Fatalf("rows len = %d, want %d", len(got.rows), len(want))
	}
	for i := range want {
		if got.rows[i] != want[i] {
			t.Fatalf("rows[%d] = %d, want %d", i, got.rows[i], want[i])
		}
	}
	if got.runs != 5 {
		t.Fatalf("runs = %d, want 5", got.runs)
	}
	if got.profile != "all" {
		t.Fatalf("profile = %q, want all", got.profile)
	}
	if got.json {
		t.Fatalf("json should default to false")
	}
}

func TestBenchWarmReopenMode(t *testing.T) {
	var buf bytes.Buffer
	args := []string{"-rows", "1000", "-runs", "1", "-mode", "warm-reopen", "-dir", t.TempDir()}
	if err := run(args, &buf); err != nil {
		t.Fatalf("run: %v\n%s", err, buf.String())
	}
	out := buf.String()
	for _, want := range []string{"Mode:         warm-reopen", "database closed and reopened before queries"} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing %q in output\n%s", want, out)
		}
	}
}

func TestBuildBenchmarkQueriesUsesProfileValues(t *testing.T) {
	profile, err := profileSpecFor("random")
	if err != nil {
		t.Fatal(err)
	}
	const rows = 1000
	lookupRow := benchmarkRowForLookup(rows)
	queries := buildBenchmarkQueries(profile, rows)

	expectedLen := len(baseRegisteredQueries) + 4
	if len(queries) != expectedLen {
		t.Fatalf("query count = %d, want %d", len(queries), expectedLen)
	}

	queryByName := map[string]query{}
	for _, q := range queries {
		queryByName[q.Name] = q
	}

	checkMatch := func(name, want string) {
		if q, ok := queryByName[name]; !ok {
			t.Fatalf("missing %q query", name)
		} else if !strings.Contains(q.SQL, want) {
			t.Fatalf("%q sql = %q, want containing %q", name, q.SQL, want)
		}
	}

	checkMatch("user id lookup", strconv.FormatInt(profile.userFor(lookupRow), 10))
	checkMatch("created_at point lookup", strconv.FormatInt(createdAtForRow(lookupRow), 10))
	checkMatch("uuid lookup", "'"+uuidForRow(lookupRow)+"'")
	checkMatch("url lookup", "'"+urlForRow(lookupRow)+"'")
}

func TestQueryMatchesFilterChecksNameAndSQL(t *testing.T) {
	q := query{Name: "uuid lookup", SQL: "SELECT count(*) FROM events WHERE event_uuid = 'abc-123'"}
	for _, filter := range []string{"uuid", "SELECT", "abc-123", "EVENTS", ""} {
		if !queryMatchesFilter(q, filter) {
			t.Fatalf("expected %q to match filter %q", q.Name, filter)
		}
	}
	if queryMatchesFilter(q, "missing") {
		t.Fatal("unexpected filter match for missing text")
	}
}

func TestLookupQueryFilterRunsSingleQuery(t *testing.T) {
	var buf bytes.Buffer
	args := []string{"-rows", "1", "-runs", "1", "-query", "uuid lookup", "-json", "-profile", "structured", "-dir", t.TempDir()}
	if err := run(args, &buf); err != nil {
		t.Fatalf("run: %v\n%s", err, buf.String())
	}
	var report benchReport
	if err := json.Unmarshal(buf.Bytes(), &report); err != nil {
		t.Fatalf("json: %v\n%s", err, buf.String())
	}
	if len(report.Queries) != 1 {
		t.Fatalf("query count = %d, want 1", len(report.Queries))
	}
	q := report.Queries[0]
	if q.Name != "uuid lookup" {
		t.Fatalf("query name = %q, want %q", q.Name, "uuid lookup")
	}
	if len(q.Values) == 0 || len(q.Values[0]) == 0 {
		t.Fatalf("missing query values: %#v", q.Values)
	}
	if q.Values[0][0] != float64(1) {
		t.Fatalf("uuid lookup result = %#v, want 1", q.Values)
	}
}

func TestBenchEmitsJSONResults(t *testing.T) {
	const rows = 1000
	report := runBenchReportJSON(t, []string{"-rows", strconv.Itoa(rows), "-runs", "1", "-json", "-profile", "structured"}...)
	profile, err := profileSpecFor("structured")
	if err != nil {
		t.Fatal(err)
	}
	if report.Rows != rows || report.Mode != benchModeSameProcess || len(report.Queries) != len(buildBenchmarkQueries(profile, rows)) {
		t.Fatalf("report = %#v", report)
	}
	if report.Load.Elapsed == 0 || report.Load.GenerateNs == 0 || report.Load.AppendNs == 0 || report.Load.AppendCallNs == 0 || report.Load.FlushNs == 0 {
		t.Fatalf("missing setup timing breakdown = %#v", report.Load)
	}
	for _, tc := range []struct {
		name string
		want float64
	}{
		{name: "event checkout for tenant", want: 1},
		{name: "uuid lookup", want: 1},
		{name: "url lookup", want: 1},
	} {
		query := queryReportByName(t, report, tc.name)
		if got := firstNumericQueryValue(t, query); got != tc.want {
			t.Fatalf("%q = %v, want %v", tc.name, got, tc.want)
		}
	}
	first := queryReportByName(t, report, "event checkout for tenant").Explain
	if first.Reduction.Rows.Scanned != rows || first.Reduction.Rows.Matched != 1 || !first.Selectivity.Valid || !first.BytesPerMatch.Valid {
		t.Fatalf("first explain = %#v", first)
	}
	if first.Timing.Samples != 1 {
		t.Fatalf("first explain missing timing/access/read = %#v", first)
	}
	tenantCount := queryReportByName(t, report, "event checkout for tenant")
	if tenantCount.Timing.AvgNs == 0 || tenantCount.Timing.P95Ns == 0 || tenantCount.Explain.Timing.AvgNs == 0 {
		t.Fatalf("missing p95 timing = %#v %#v", tenantCount.Timing, tenantCount.Explain.Timing)
	}
	if tenantCount.Timing.Samples != 1 {
		t.Fatalf("missing sample count = %#v", tenantCount.Timing)
	}
}

func TestDefaultProfileRunsAllProfiles(t *testing.T) {
	reports := runBenchReportsJSON(t, "-rows", "1", "-runs", "1", "-json", "-query", "uuid lookup")
	if len(reports) != 3 {
		t.Fatalf("reports count = %d, want 3", len(reports))
	}
	got := map[string]bool{}
	for _, report := range reports {
		got[report.Profile] = true
	}
	for _, want := range []string{"structured", "random", "skewed"} {
		if !got[want] {
			t.Fatalf("missing profile %q in reports: %#v", want, got)
		}
	}
}

func TestBenchmarkReusesExistingRowsAndAppendsMissingRows(t *testing.T) {
	root := t.TempDir()
	args := []string{"-runs", "1", "-json", "-profile", "structured", "-query", "uuid lookup", "-dir", root}

	first := runBenchReportJSON(t, append(args, "-rows", "10")...)
	if want := filepath.Join(root, "structured", "seg-default"); first.Dir != want {
		t.Fatalf("dir = %q, want %q", first.Dir, want)
	}
	if first.Load.ExistingRows != 0 || first.Load.TargetRows != 10 || first.Load.Rows != 10 || first.Rows != 10 {
		t.Fatalf("first load = %#v rows=%d", first.Load, first.Rows)
	}

	second := runBenchReportJSON(t, append(args, "-rows", "10")...)
	if second.Load.ExistingRows != 10 || second.Load.TargetRows != 10 || second.Load.Rows != 0 || second.Rows != 10 {
		t.Fatalf("second load = %#v rows=%d", second.Load, second.Rows)
	}

	larger := runBenchReportJSON(t, append(args, "-rows", "15")...)
	if larger.Load.ExistingRows != 10 || larger.Load.TargetRows != 15 || larger.Load.Rows != 5 || larger.Rows != 15 {
		t.Fatalf("larger load = %#v rows=%d", larger.Load, larger.Rows)
	}
}

func TestBenchmarkSeparatesProfilesAndSegmentRows(t *testing.T) {
	root := t.TempDir()
	common := []string{"-rows", "3", "-runs", "1", "-json", "-query", "uuid lookup", "-dir", root}

	structured := runBenchReportJSON(t, append(common, "-profile", "structured")...)
	random := runBenchReportJSON(t, append(common, "-profile", "random")...)
	customSegment := runBenchReportJSON(t, append(common, "-profile", "structured", "-segment-rows", "16")...)

	if structured.Dir != filepath.Join(root, "structured", "seg-default") {
		t.Fatalf("structured dir = %q", structured.Dir)
	}
	if random.Dir != filepath.Join(root, "random", "seg-default") {
		t.Fatalf("random dir = %q", random.Dir)
	}
	if customSegment.Dir != filepath.Join(root, "structured", "seg-16") {
		t.Fatalf("custom segment dir = %q", customSegment.Dir)
	}
	if structured.Load.ExistingRows != 0 || random.Load.ExistingRows != 0 || customSegment.Load.ExistingRows != 0 {
		t.Fatalf("expected isolated first loads, got %#v %#v %#v", structured.Load, random.Load, customSegment.Load)
	}
}

func runBenchReportJSON(t *testing.T, args ...string) benchReport {
	t.Helper()
	var buf bytes.Buffer
	args = appendBenchTempDir(t, args)
	if err := run(args, &buf); err != nil {
		t.Fatalf("run: %v\n%s", err, buf.String())
	}
	var report benchReport
	if err := json.Unmarshal(buf.Bytes(), &report); err != nil {
		t.Fatalf("json: %v\n%s", err, buf.String())
	}
	return report
}

func runBenchReportsJSON(t *testing.T, args ...string) []benchReport {
	t.Helper()
	var buf bytes.Buffer
	args = appendBenchTempDir(t, args)
	if err := run(args, &buf); err != nil {
		t.Fatalf("run: %v\n%s", err, buf.String())
	}
	dec := json.NewDecoder(&buf)
	var reports []benchReport
	for {
		var report benchReport
		err := dec.Decode(&report)
		if err != nil {
			if err == io.EOF {
				break
			}
			t.Fatalf("json: %v\n%s", err, buf.String())
		}
		reports = append(reports, report)
	}
	if len(reports) == 0 {
		t.Fatal("no reports in JSON output")
	}
	return reports
}

func appendBenchTempDir(t *testing.T, args []string) []string {
	t.Helper()
	for i, arg := range args {
		if arg == "-dir" || strings.HasPrefix(arg, "-dir=") {
			if arg == "-dir" && i == len(args)-1 {
				t.Fatalf("-dir requires a value")
			}
			return args
		}
	}
	out := append([]string(nil), args...)
	return append(out, "-dir", t.TempDir())
}

func firstNumericQueryValue(t *testing.T, q queryReport) float64 {
	t.Helper()
	if len(q.Values) == 0 || len(q.Values[0]) == 0 {
		t.Fatalf("query %q has no values: %#v", q.Name, q.Values)
	}
	value, ok := q.Values[0][0].(float64)
	if !ok {
		t.Fatalf("query %q expected numeric value, got %T", q.Name, q.Values[0][0])
	}
	return value
}

func TestPercentileNearestRank(t *testing.T) {
	if got := percentileNearestRank([]int64{10, 50, 20, 30, 40}, 95); got != 50 {
		t.Fatalf("p95 = %d, want 50", got)
	}
	if got := percentileNearestRank([]int64{10, 50, 20, 30, 40}, 50); got != 30 {
		t.Fatalf("p50 = %d, want 30", got)
	}
}

func queryReportByName(t *testing.T, report benchReport, name string) queryReport {
	t.Helper()
	for _, query := range report.Queries {
		if query.Name == name {
			return query
		}
	}
	t.Fatalf("missing query report %q in %#v", name, report.Queries)
	return queryReport{}
}
