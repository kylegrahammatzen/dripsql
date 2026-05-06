package table

import (
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func BenchmarkCountInt64EqualTable(b *testing.B) {
	tbl := benchmarkEventsTable(b, 10, 100_000)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		count, err := tbl.CountInt64Equal("tenant_id", 7)
		if err != nil {
			b.Fatal(err)
		}
		if count != 980 {
			b.Fatalf("count = %d, want 980", count)
		}
	}
}

func BenchmarkCountStringEqualTable(b *testing.B) {
	tbl := benchmarkEventsTable(b, 10, 100_000)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		count, err := tbl.CountStringEqual("event_type", "checkout")
		if err != nil {
			b.Fatal(err)
		}
		if count != 250_000 {
			b.Fatalf("count = %d, want 250000", count)
		}
	}
}

func BenchmarkScannerCountInt64EqualTable(b *testing.B) {
	tbl := benchmarkEventsTable(b, 10, 100_000)
	scanner := tbl.NewScanner()
	b.Cleanup(func() { _ = scanner.Close() })
	if count, err := scanner.CountInt64Equal("tenant_id", 7); err != nil {
		b.Fatal(err)
	} else if count != 980 {
		b.Fatalf("count = %d, want 980", count)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		count, err := scanner.CountInt64Equal("tenant_id", 7)
		if err != nil {
			b.Fatal(err)
		}
		if count != 980 {
			b.Fatalf("count = %d, want 980", count)
		}
	}
}

func BenchmarkScannerCountInt64EqualParallelTable(b *testing.B) {
	tbl := benchmarkEventsTable(b, 10, 100_000)
	scanner := tbl.NewScanner()
	b.Cleanup(func() { _ = scanner.Close() })
	if count, err := scanner.CountInt64EqualParallel("tenant_id", 7, 4); err != nil {
		b.Fatal(err)
	} else if count != 980 {
		b.Fatalf("count = %d, want 980", count)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		count, err := scanner.CountInt64EqualParallel("tenant_id", 7, 4)
		if err != nil {
			b.Fatal(err)
		}
		if count != 980 {
			b.Fatalf("count = %d, want 980", count)
		}
	}
}

func BenchmarkScannerCountStringEqualTable(b *testing.B) {
	tbl := benchmarkEventsTable(b, 10, 100_000)
	scanner := tbl.NewScanner()
	b.Cleanup(func() { _ = scanner.Close() })
	if count, err := scanner.CountStringEqual("event_type", "checkout"); err != nil {
		b.Fatal(err)
	} else if count != 250_000 {
		b.Fatalf("count = %d, want 250000", count)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		count, err := scanner.CountStringEqual("event_type", "checkout")
		if err != nil {
			b.Fatal(err)
		}
		if count != 250_000 {
			b.Fatalf("count = %d, want 250000", count)
		}
	}
}

func BenchmarkScannerCountStringEqualParallelTable(b *testing.B) {
	tbl := benchmarkEventsTable(b, 10, 100_000)
	scanner := tbl.NewScanner()
	b.Cleanup(func() { _ = scanner.Close() })
	if count, err := scanner.CountStringEqualParallel("event_type", "checkout", 4); err != nil {
		b.Fatal(err)
	} else if count != 250_000 {
		b.Fatalf("count = %d, want 250000", count)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		count, err := scanner.CountStringEqualParallel("event_type", "checkout", 4)
		if err != nil {
			b.Fatal(err)
		}
		if count != 250_000 {
			b.Fatalf("count = %d, want 250000", count)
		}
	}
}

func BenchmarkGroupStringCountsTable(b *testing.B) {
	tbl := benchmarkEventsTable(b, 10, 100_000)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		counts, err := tbl.GroupStringCounts("event_type")
		if err != nil {
			b.Fatal(err)
		}
		if counts["checkout"] != 250_000 {
			b.Fatalf("checkout count = %d, want 250000", counts["checkout"])
		}
	}
}

func BenchmarkScannerGroupStringCountsTable(b *testing.B) {
	tbl := benchmarkEventsTable(b, 10, 100_000)
	scanner := tbl.NewScanner()
	b.Cleanup(func() { _ = scanner.Close() })
	counts := make(map[string]int, 4)
	if _, err := scanner.GroupStringCountsInto("event_type", counts); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		clear(counts)
		counts, err := scanner.GroupStringCountsInto("event_type", counts)
		if err != nil {
			b.Fatal(err)
		}
		if counts["checkout"] != 250_000 {
			b.Fatalf("checkout count = %d, want 250000", counts["checkout"])
		}
	}
}

func BenchmarkScannerHighCardinalityStringTable(b *testing.B) {
	const rows = 1_000_000
	tbl, values := benchmarkURLTable(b, rows)
	scanner := tbl.NewScanner()
	b.Cleanup(func() { _ = scanner.Close() })
	target := values[12_345]
	if count, err := scanner.CountStringEqual("url", target); err != nil {
		b.Fatal(err)
	} else if count != 1 {
		b.Fatalf("count = %d, want 1", count)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		count, err := scanner.CountStringEqual("url", target)
		if err != nil {
			b.Fatal(err)
		}
		if count != 1 {
			b.Fatalf("count = %d, want 1", count)
		}
	}
}

func benchmarkEventsTable(b *testing.B, segments int, rowsPerSegment int) *Table {
	b.Helper()
	tbl := createEventsTable(b, b.TempDir())
	choices := []string{"signup", "checkout", "page_view", "cancel"}
	for segment := 0; segment < segments; segment++ {
		tenants := make([]int64, rowsPerSegment)
		events := make([]string, rowsPerSegment)
		for row := range rowsPerSegment {
			tenants[row] = int64(row % 1024)
			events[row] = choices[row%len(choices)]
		}
		appendBatch(b, tbl, tenants, events)
	}
	return tbl
}

func benchmarkURLTable(b *testing.B, rows int) (*Table, []string) {
	b.Helper()
	tbl, err := Create(b.TempDir(), []Column{{Name: "url", Kind: vector.KindString}})
	if err != nil {
		b.Fatal(err)
	}
	values := make([]string, rows)
	for row := range values {
		values[row] = "/item/" + benchmarkBase36(row)
	}
	if err := tbl.Append(mustBatch(b, vector.Column{Name: "url", Vector: vector.FromString(values)})); err != nil {
		b.Fatal(err)
	}
	return tbl, values
}

func benchmarkBase36(n int) string {
	const digits = "0123456789abcdefghijklmnopqrstuvwxyz"
	if n == 0 {
		return "0"
	}
	var buf [16]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = digits[n%len(digits)]
		n /= len(digits)
	}
	return string(buf[pos:])
}
