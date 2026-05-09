package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"version"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, stderr = %q", code, stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != version {
		t.Fatalf("stdout = %q, want %q", stdout.String(), version)
	}
}

func TestRunExecAndQuery(t *testing.T) {
	dir := t.TempDir()
	var stdout, stderr bytes.Buffer
	code := run([]string{"exec", dir,
		`CREATE TABLE events (tenant_id INT64 NOT NULL, event_type TEXT NOT NULL);
		 INSERT INTO events VALUES (1, 'signup'), (1, 'checkout'), (2, 'checkout')`,
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exec exit code = %d, stderr = %q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "rows_affected=3") {
		t.Fatalf("exec stdout = %q", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = run([]string{"query", dir, `SELECT count(*) FROM events WHERE tenant_id = 1`}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("query exit code = %d, stderr = %q", code, stderr.String())
	}
	if stdout.String() != "count\n2\n" {
		t.Fatalf("query stdout = %q", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	code = run([]string{"query", dir,
		`EXPLAIN SELECT count(*) FROM events WHERE tenant_id = 1`,
	}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("explain exit code = %d, stderr = %q", code, stderr.String())
	}
	for _, want := range []string{"plan", "Count", "ReadSegments(events)", "tenant_id = 1"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("EXPLAIN missing %q in:\n%s", want, stdout.String())
		}
	}
}

func TestRunUsageError(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(nil, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "dripsql exec") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}
