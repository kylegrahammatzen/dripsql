package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestBenchSmokeProducesAllSections(t *testing.T) {
	var buf bytes.Buffer
	args := []string{
		"-rows", "2000",
		"-runs", "2",
		"-keep=false",
	}
	if err := run(args, &buf); err != nil {
		t.Fatalf("run: %v\n%s", err, buf.String())
	}
	out := buf.String()
	for _, want := range []string{
		"DripSQL Benchmark",
		"Load Benchmark",
		"Storage",
		"Columns",
		"Query Benchmark",
		"Findings",
		"event checkout for tenant",
		"checkout amount for tenant",
		"checkout counts by country",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in bench output:\n%s", want, out)
		}
	}
}

func TestBenchEmitsJSONShape(t *testing.T) {
	var buf bytes.Buffer
	args := []string{
		"-rows", "1000",
		"-runs", "1",
		"-emit", "json",
	}
	if err := run(args, &buf); err != nil {
		t.Fatalf("run: %v\n%s", err, buf.String())
	}
	out := buf.String()
	for _, want := range []string{
		`"env":`,
		`"profile":`,
		`"load":`,
		`"storage":`,
		`"queries":`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in JSON bench output:\n%s", want, out)
		}
	}
}
