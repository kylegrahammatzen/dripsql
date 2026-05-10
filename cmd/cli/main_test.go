package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"version"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code = %d, stderr = %q", code, stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != version {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestExecThenQueryAcrossProcesses(t *testing.T) {
	dir := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := run([]string{"exec", dir, `CREATE TABLE events (tenant_id INT64 NOT NULL, event_type TEXT NOT NULL, amount INT64 NOT NULL); INSERT INTO events VALUES (42, 'checkout', 100), (7, 'login', 5)`}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exec code = %d, stdout = %q stderr = %q", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()

	code = run([]string{"query", dir, `SELECT sum(amount) FROM events WHERE tenant_id = 42`}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("query code = %d, stdout = %q stderr = %q", code, stdout.String(), stderr.String())
	}
	if got := stdout.String(); !strings.Contains(got, "sum\n100\n") {
		t.Fatalf("query stdout = %q", got)
	}
}
