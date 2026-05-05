package storage

import (
	"bytes"
	"maps"
	"slices"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestSelectSegmentInt64EqualBytes(t *testing.T) {
	data := selectSegmentData(t)
	runSelectSegmentInt64Equal(t, func(column string, value int64, scratch []uint32) ([]uint32, bool, error) {
		return SelectSegmentInt64EqualBytes(data, column, value, scratch)
	})
}

func TestCountSegmentInt64EqualBytes(t *testing.T) {
	data := writeSegmentBytes(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "signup", "cancel"})},
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7, 11})},
	)

	count, ok, err := CountSegmentInt64EqualBytes(data, "tenant_id", 7)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing tenant_id column")
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}

	count, ok, err = CountSegmentInt64EqualBytes(data, "tenant_id", 99)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing tenant_id column")
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}

	_, ok, err = CountSegmentInt64EqualBytes(data, "missing", 7)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("did not expect missing column")
	}

	_, ok, err = CountSegmentInt64EqualBytes(data, "event_type", 7)
	if err == nil {
		t.Fatal("expected wrong type error")
	}
	if !ok {
		t.Fatal("expected event_type column to exist")
	}
}

func TestCountSegmentInt64EqualBytesDictionary(t *testing.T) {
	data := writeSegmentBytes(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7, 7, 42, 7})},
	)
	stats, payload, found, err := findColumnPayloadBytes(data, "tenant_id")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("missing tenant_id column")
	}
	if payload[0] != int64CodecDictionary {
		t.Fatalf("int64 codec = %d, want dictionary", payload[0])
	}
	if stats.EncodedLen >= plainInt64PayloadLen([]int64{7, 42, 7, 7, 42, 7}) {
		t.Fatalf("encoded len = %d, want below plain", stats.EncodedLen)
	}

	count, ok, err := CountSegmentInt64EqualBytes(data, "tenant_id", 7)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing tenant_id column")
	}
	if count != 4 {
		t.Fatalf("count = %d, want 4", count)
	}

	selected, ok, err := SelectSegmentInt64EqualBytes(data, "tenant_id", 42, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing tenant_id column")
	}
	if !slices.Equal(selected, []uint32{1, 4}) {
		t.Fatalf("selection = %v, want [1 4]", selected)
	}
}

func TestCountSegmentInt64EqualAt(t *testing.T) {
	data := writeSegmentBytes(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "signup", "cancel"})},
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7, 11})},
	)

	count, ok, scratch, bytesRead, err := CountSegmentInt64EqualAt(bytes.NewReader(data), 0, int64(len(data)), "tenant_id", 7, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing tenant_id column")
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
	if len(scratch) == 0 {
		t.Fatal("expected scratch to retain column payload")
	}
	if bytesRead <= 0 || bytesRead >= int64(len(data)) {
		t.Fatalf("bytes read = %d, want between 1 and full segment %d", bytesRead, len(data))
	}
}

func TestSegmentColumnPayloadRanges(t *testing.T) {
	data := writeSegmentBytes(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "signup", "cancel"})},
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7, 11})},
	)
	ranges, err := SegmentColumnPayloadRanges(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(ranges) != 2 {
		t.Fatalf("ranges = %d, want 2", len(ranges))
	}
	tenantRange := mustColumnPayloadRange(t, ranges, "tenant_id")
	if tenantRange.Offset <= 0 || tenantRange.Bytes <= 0 || tenantRange.Offset+tenantRange.Bytes >= int64(len(data)) {
		t.Fatalf("tenant range = %+v, segment bytes = %d", tenantRange, len(data))
	}
	tenantStats, _, found, err := findColumnPayloadBytes(data, "tenant_id")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("missing tenant_id column")
	}
	count, scratch, bytesRead, err := CountColumnInt64EqualAt(bytes.NewReader(data), tenantRange.Offset, tenantRange.Bytes, tenantStats, 7, nil)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("tenant count = %d, want 2", count)
	}
	if len(scratch) == 0 || bytesRead != tenantRange.Bytes {
		t.Fatalf("scratch len / bytes read = %d / %d, want payload bytes %d", len(scratch), bytesRead, tenantRange.Bytes)
	}

	eventRange := mustColumnPayloadRange(t, ranges, "event_type")
	eventStats, _, found, err := findColumnPayloadBytes(data, "event_type")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("missing event_type column")
	}
	count, scratch, bytesRead, err = CountColumnStringEqualAt(bytes.NewReader(data), eventRange.Offset, eventRange.Bytes, eventStats, "checkout", scratch)
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("event count = %d, want 1", count)
	}
	if bytesRead != eventRange.Bytes {
		t.Fatalf("bytes read = %d, want %d", bytesRead, eventRange.Bytes)
	}
}

func TestCountSegmentStringEqualBytes(t *testing.T) {
	data := writeSegmentBytes(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "signup", "cancel", "checkout"})},
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7, 11, 42})},
	)
	_, payload, found, err := findColumnPayloadBytes(data, "event_type")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("missing event_type column")
	}
	if !stringDictionaryIDEncodingIsPacked(int(payload[5])) {
		t.Fatalf("string dictionary id encoding = %d, want packed", payload[5])
	}

	count, ok, err := CountSegmentStringEqualBytes(data, "event_type", "checkout")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing event_type column")
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}

	count, ok, err = CountSegmentStringEqualBytes(data, "event_type", "missing")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing event_type column")
	}
	if count != 0 {
		t.Fatalf("count = %d, want 0", count)
	}

	_, ok, err = CountSegmentStringEqualBytes(data, "missing", "checkout")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("did not expect missing column")
	}

	_, ok, err = CountSegmentStringEqualBytes(data, "tenant_id", "checkout")
	if err == nil {
		t.Fatal("expected wrong type error")
	}
	if !ok {
		t.Fatal("expected tenant_id column to exist")
	}
}

func TestCountSegmentStringEqualAt(t *testing.T) {
	data := writeSegmentBytes(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "signup", "cancel", "checkout"})},
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7, 11, 42})},
	)

	count, ok, scratch, bytesRead, err := CountSegmentStringEqualAt(bytes.NewReader(data), 0, int64(len(data)), "event_type", "checkout", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing event_type column")
	}
	if count != 2 {
		t.Fatalf("count = %d, want 2", count)
	}
	if len(scratch) == 0 {
		t.Fatal("expected scratch to retain column payload")
	}
	if bytesRead <= 0 || bytesRead >= int64(len(data)) {
		t.Fatalf("bytes read = %d, want between 1 and full segment %d", bytesRead, len(data))
	}
}

func TestSelectSegmentStringEqualBytes(t *testing.T) {
	data := writeSegmentBytes(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "signup", "cancel", "checkout"})},
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7, 11, 42})},
	)
	scratch := make([]uint32, 0, 8)

	selected, ok, err := SelectSegmentStringEqualBytes(data, "event_type", "checkout", scratch)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing event_type column")
	}
	if !slices.Equal(selected, []uint32{1, 4}) {
		t.Fatalf("selection = %v, want [1 4]", selected)
	}
	if cap(selected) != cap(scratch) {
		t.Fatalf("scratch cap = %d, want %d", cap(selected), cap(scratch))
	}
}

func TestCountSegmentStringEqualBytesPlain(t *testing.T) {
	data := writeSegmentBytes(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"alpha", "bravo", "charlie", "delta"})},
	)

	count, ok, err := CountSegmentStringEqualBytes(data, "event_type", "charlie")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing event_type column")
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1", count)
	}
}

func TestGroupSegmentStringCountsBytes(t *testing.T) {
	data := writeSegmentBytes(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "signup", "cancel", "checkout"})},
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7, 11, 42})},
	)
	counts := map[string]int{"existing": 3}

	ok, err := GroupSegmentStringCountsBytes(data, "event_type", counts)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing event_type column")
	}
	want := map[string]int{"existing": 3, "signup": 2, "checkout": 2, "cancel": 1}
	if !maps.Equal(counts, want) {
		t.Fatalf("counts = %v, want %v", counts, want)
	}

	_, err = GroupSegmentStringCountsBytes(data, "tenant_id", counts)
	if err == nil {
		t.Fatal("expected wrong type error")
	}
}

func TestGroupSegmentStringCountsAtCached(t *testing.T) {
	data := writeSegmentBytes(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "signup", "cancel", "checkout"})},
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7, 11, 42})},
	)
	counts := map[string]int{"existing": 3}

	ok, scratch, keyScratch, bytesRead, err := GroupSegmentStringCountsAtCached(bytes.NewReader(data), 0, int64(len(data)), "event_type", counts, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing event_type column")
	}
	want := map[string]int{"existing": 3, "signup": 2, "checkout": 2, "cancel": 1}
	if !maps.Equal(counts, want) {
		t.Fatalf("counts = %v, want %v", counts, want)
	}
	if len(scratch) == 0 {
		t.Fatal("expected scratch to retain column payload")
	}
	if len(keyScratch) == 0 {
		t.Fatal("expected key scratch to retain grouped keys")
	}
	if bytesRead <= 0 || bytesRead >= int64(len(data)) {
		t.Fatalf("bytes read = %d, want between 1 and full segment %d", bytesRead, len(data))
	}
}

func TestGroupSegmentStringCountsBytesPlain(t *testing.T) {
	data := writeSegmentBytes(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"alpha", "bravo", "charlie", "delta"})},
	)
	counts := make(map[string]int)

	ok, err := GroupSegmentStringCountsBytes(data, "event_type", counts)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing event_type column")
	}
	want := map[string]int{"alpha": 1, "bravo": 1, "charlie": 1, "delta": 1}
	if !maps.Equal(counts, want) {
		t.Fatalf("counts = %v, want %v", counts, want)
	}
}

func BenchmarkSelectSegmentInt64EqualBytesWithString(b *testing.B) {
	b.ReportAllocs()

	data := benchmarkInt64StringSegment(b)
	scratch := make([]uint32, 0, 100_000)
	b.ResetTimer()

	for b.Loop() {
		selected, ok, err := SelectSegmentInt64EqualBytes(data, "tenant_id", 7, scratch)
		if err != nil {
			b.Fatal(err)
		}
		if !ok {
			b.Fatal("missing tenant_id column")
		}
		if len(selected) != 98 {
			b.Fatalf("selection len = %d, want 98", len(selected))
		}
		scratch = selected
	}
}

func BenchmarkCountSegmentInt64EqualBytesWithString(b *testing.B) {
	b.ReportAllocs()

	data := benchmarkInt64StringSegment(b)
	b.ResetTimer()

	for b.Loop() {
		count, ok, err := CountSegmentInt64EqualBytes(data, "tenant_id", 7)
		if err != nil {
			b.Fatal(err)
		}
		if !ok {
			b.Fatal("missing tenant_id column")
		}
		if count != 98 {
			b.Fatalf("count = %d, want 98", count)
		}
	}
}

func BenchmarkCountSegmentStringEqualBytes(b *testing.B) {
	b.ReportAllocs()

	choices := []string{"signup", "checkout", "page_view", "cancel"}
	values := make([]string, 100_000)
	for i := range values {
		values[i] = choices[i%len(choices)]
	}
	data := writeSegmentBytes(b,
		vector.Column{Name: "event_type", Vector: vector.NewString(values)},
	)
	b.ResetTimer()

	for b.Loop() {
		count, ok, err := CountSegmentStringEqualBytes(data, "event_type", "checkout")
		if err != nil {
			b.Fatal(err)
		}
		if !ok {
			b.Fatal("missing event_type column")
		}
		if count != 25_000 {
			b.Fatalf("count = %d, want 25000", count)
		}
	}
}

func BenchmarkGroupSegmentStringCountsBytes(b *testing.B) {
	b.ReportAllocs()

	choices := []string{"signup", "checkout", "page_view", "cancel"}
	values := make([]string, 100_000)
	for i := range values {
		values[i] = choices[i%len(choices)]
	}
	data := writeSegmentBytes(b,
		vector.Column{Name: "event_type", Vector: vector.NewString(values)},
	)
	counts := make(map[string]int, len(choices))
	b.ResetTimer()

	for b.Loop() {
		clear(counts)
		ok, err := GroupSegmentStringCountsBytes(data, "event_type", counts)
		if err != nil {
			b.Fatal(err)
		}
		if !ok {
			b.Fatal("missing event_type column")
		}
		if counts["checkout"] != 25_000 {
			b.Fatalf("checkout count = %d, want 25000", counts["checkout"])
		}
	}
}

func mustColumnPayloadRange(t testing.TB, ranges []ColumnPayloadRange, name string) ColumnPayloadRange {
	t.Helper()
	for _, columnRange := range ranges {
		if columnRange.Name == name {
			return columnRange
		}
	}
	t.Fatalf("missing column range %q", name)
	return ColumnPayloadRange{}
}
