package storage

import (
	"bytes"
	"encoding/binary"
	"slices"
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestSegmentRoundTrip(t *testing.T) {
	tests := []struct {
		name  string
		batch vector.Batch
	}{
		{
			name: "int64 and string",
			batch: mustBatch(t,
				vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7})},
				vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "signup"})},
			),
		},
		{
			name: "empty columns",
			batch: mustBatch(t,
				vector.Column{Name: "tenant_id", Vector: vector.NewInt64(nil)},
				vector.Column{Name: "event_type", Vector: vector.NewString(nil)},
			),
		},
		{
			name: "edge values",
			batch: mustBatch(t,
				vector.Column{Name: "value", Vector: vector.NewInt64([]int64{-10, 0, 99})},
				vector.Column{Name: "label", Vector: vector.NewString([]string{"", "signup", ""})},
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writeStats, got, readStats := roundTrip(t, tt.batch)

			if writeStats.Rows != tt.batch.Count {
				t.Fatalf("writeStats.Rows = %d, want %d", writeStats.Rows, tt.batch.Count)
			}
			if readStats.Rows != tt.batch.Count {
				t.Fatalf("readStats.Rows = %d, want %d", readStats.Rows, tt.batch.Count)
			}

			requireBatchEqual(t, tt.batch, got)
		})
	}
}

func TestSegmentPersistsInt64Stats(t *testing.T) {
	tests := []struct {
		name       string
		column     string
		values     []int64
		wantMin    int64
		wantMax    int64
		encodedLen int
	}{
		{name: "positive", column: "tenant_id", values: []int64{7, 42, 7}, wantMin: 7, wantMax: 42, encodedLen: 24},
		{name: "negative", column: "value", values: []int64{-99, -7, -42}, wantMin: -99, wantMax: -7, encodedLen: 24},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batch := mustBatch(t,
				vector.Column{Name: tt.column, Vector: vector.NewInt64(tt.values)},
			)

			writeStats, _, readStats := roundTrip(t, batch)
			statsToCheck := []struct {
				label string
				stats ColumnStats
			}{
				{label: "written", stats: columnStats(t, writeStats, tt.column)},
				{label: "read", stats: columnStats(t, readStats, tt.column)},
			}

			for _, got := range statsToCheck {
				if !got.stats.HasMinMax {
					t.Fatalf("expected %s min/max stats", got.label)
				}
				if got.stats.MinInt64 != tt.wantMin || got.stats.MaxInt64 != tt.wantMax {
					t.Fatalf("%s min/max = %d/%d, want %d/%d", got.label, got.stats.MinInt64, got.stats.MaxInt64, tt.wantMin, tt.wantMax)
				}
				if got.stats.EncodedLen != tt.encodedLen {
					t.Fatalf("%s EncodedLen = %d, want %d", got.label, got.stats.EncodedLen, tt.encodedLen)
				}
			}
		})
	}
}

func TestReadSegmentStats(t *testing.T) {
	batch := mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7})},
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "signup"})},
	)

	var buf bytes.Buffer
	writeStats, err := WriteSegment(&buf, batch)
	if err != nil {
		t.Fatal(err)
	}

	readStats, err := ReadSegmentStats(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}

	if readStats.Rows != writeStats.Rows {
		t.Fatalf("rows = %d, want %d", readStats.Rows, writeStats.Rows)
	}
	if len(readStats.Columns) != len(writeStats.Columns) {
		t.Fatalf("column count = %d, want %d", len(readStats.Columns), len(writeStats.Columns))
	}

	tenantStats := columnStats(t, readStats, "tenant_id")
	if tenantStats.Kind != vector.KindInt64 || !tenantStats.HasMinMax {
		t.Fatalf("tenant stats = %+v, want int64 min/max", tenantStats)
	}
	if tenantStats.MinInt64 != 7 || tenantStats.MaxInt64 != 42 {
		t.Fatalf("tenant min/max = %d/%d, want 7/42", tenantStats.MinInt64, tenantStats.MaxInt64)
	}

	eventStats := columnStats(t, readStats, "event_type")
	if eventStats.Kind != vector.KindString {
		t.Fatalf("event kind = %s, want string", eventStats.Kind)
	}
	if eventStats.HasMinMax {
		t.Fatal("did not expect string min/max stats")
	}

	if _, ok := readStats.Column("missing"); ok {
		t.Fatal("did not expect missing column stats")
	}
}

func TestEncodeStringUsesDictionaryWhenSmaller(t *testing.T) {
	values := []string{"signup", "checkout", "signup", "signup", "checkout", "signup"}

	encoded, err := encodeString(values)
	if err != nil {
		t.Fatal(err)
	}
	if encoded[0] != stringCodecDictionary {
		t.Fatalf("string codec = %d, want dictionary", encoded[0])
	}
	if len(encoded) >= plainStringPayloadLen(values) {
		t.Fatalf("encoded length = %d, want less than plain length %d", len(encoded), plainStringPayloadLen(values))
	}

	decoded, err := decodeString(encoded, len(values))
	if err != nil {
		t.Fatal(err)
	}
	requireVectorEqual(t, vector.NewString(values), decoded)
}

func TestEncodeStringUsesPlainWhenDictionaryIsLarger(t *testing.T) {
	values := []string{"alpha", "bravo", "charlie", "delta"}

	encoded, err := encodeString(values)
	if err != nil {
		t.Fatal(err)
	}
	if encoded[0] != stringCodecPlain {
		t.Fatalf("string codec = %d, want plain", encoded[0])
	}

	decoded, err := decodeString(encoded, len(values))
	if err != nil {
		t.Fatal(err)
	}
	requireVectorEqual(t, vector.NewString(values), decoded)
}

func TestEncodeStringDictionaryHandlesEmptyStrings(t *testing.T) {
	values := []string{"", "", "", ""}

	encoded, err := encodeString(values)
	if err != nil {
		t.Fatal(err)
	}
	if encoded[0] != stringCodecDictionary {
		t.Fatalf("string codec = %d, want dictionary", encoded[0])
	}

	decoded, err := decodeString(encoded, len(values))
	if err != nil {
		t.Fatal(err)
	}
	requireVectorEqual(t, vector.NewString(values), decoded)
}

func TestSegmentRewritesCompactString(t *testing.T) {
	original := mustBatch(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "signup", "signup"})},
	)

	_, compact, _ := roundTrip(t, original)
	_, got, _ := roundTrip(t, compact)

	requireBatchEqual(t, original, got)
}

func TestDecodeStringRejectsBadPayload(t *testing.T) {
	validOneValueDictionary := []byte{stringCodecDictionary}
	validOneValueDictionary = binary.LittleEndian.AppendUint32(validOneValueDictionary, 1)
	validOneValueDictionary = append(validOneValueDictionary, 1)
	validOneValueDictionary = binary.LittleEndian.AppendUint32(validOneValueDictionary, 1)
	validOneValueDictionary = append(validOneValueDictionary, 'a')

	shortDictionaryData := []byte{stringCodecDictionary}
	shortDictionaryData = binary.LittleEndian.AppendUint32(shortDictionaryData, 1)
	shortDictionaryData = append(shortDictionaryData, 1)
	shortDictionaryData = binary.LittleEndian.AppendUint32(shortDictionaryData, 2)
	shortDictionaryData = append(shortDictionaryData, 'a')

	tests := []struct {
		name    string
		encoded []byte
		count   int
		wantErr string
	}{
		{name: "missing codec", encoded: nil, wantErr: "missing string codec"},
		{name: "unknown codec", encoded: []byte{255}, wantErr: "unsupported string codec"},
		{name: "short plain", encoded: []byte{stringCodecPlain, 0, 0, 0}, count: 1, wantErr: "short string length"},
		{name: "plain trailing", encoded: []byte{stringCodecPlain, 0}, wantErr: "trailing string bytes"},
		{name: "short dictionary header", encoded: []byte{stringCodecDictionary, 0, 0, 0}, wantErr: "short string dictionary header"},
		{name: "bad dictionary id width", encoded: []byte{stringCodecDictionary, 0, 0, 0, 0, 3}, wantErr: "unsupported string dictionary id width"},
		{name: "short dictionary data", encoded: shortDictionaryData, count: 1, wantErr: "short string dictionary data"},
		{name: "short dictionary ids", encoded: append(slices.Clone(validOneValueDictionary), 0), count: 2, wantErr: "short string dictionary ids"},
		{name: "dictionary id out of range", encoded: append(slices.Clone(validOneValueDictionary), 1), count: 1, wantErr: "out of range"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeString(tt.encoded, tt.count)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %q, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestReadSegmentColumns(t *testing.T) {
	batch := mustBatch(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "signup"})},
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7})},
	)

	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		t.Fatal(err)
	}

	got, stats, err := ReadSegmentColumns(bytes.NewReader(buf.Bytes()), "tenant_id")
	if err != nil {
		t.Fatal(err)
	}
	if got.Count != 3 {
		t.Fatalf("count = %d, want 3", got.Count)
	}
	if len(got.Columns) != 1 {
		t.Fatalf("column count = %d, want 1", len(got.Columns))
	}
	if _, ok := got.Column("event_type"); ok {
		t.Fatal("did not expect unrequested event_type column")
	}

	tenantCol, ok := got.Column("tenant_id")
	if !ok {
		t.Fatal("missing tenant_id column")
	}
	requireVectorEqual(t, vector.NewInt64([]int64{7, 42, 7}), tenantCol.Vector)

	if stats.Rows != 3 {
		t.Fatalf("stats rows = %d, want 3", stats.Rows)
	}
	if len(stats.Columns) != 1 {
		t.Fatalf("stats column count = %d, want 1", len(stats.Columns))
	}
	if stats.Columns[0].Name != "tenant_id" {
		t.Fatalf("stats column = %q, want tenant_id", stats.Columns[0].Name)
	}

	_, _, err = ReadSegmentColumns(bytes.NewReader(buf.Bytes()), "missing")
	if err == nil {
		t.Fatal("expected missing column error")
	}

	_, _, err = ReadSegmentColumns(bytes.NewReader(buf.Bytes()), "missing_a", "missing_b")
	if err == nil {
		t.Fatal("expected missing columns error")
	}
	if !strings.Contains(err.Error(), `"missing_a"`) {
		t.Fatalf("error = %q, want missing_a first", err)
	}

	_, _, err = ReadSegmentColumns(bytes.NewReader(buf.Bytes()), "tenant_id", "tenant_id")
	if err == nil {
		t.Fatal("expected duplicate requested column error")
	}
}

func TestReadSegmentColumnsConsumesFullSegment(t *testing.T) {
	first := writeSegmentBytes(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42})},
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout"})},
	)
	second := writeSegmentBytes(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{11, 12})},
	)

	reader := bytes.NewReader(append(first, second...))
	got, _, err := ReadSegmentColumns(reader, "tenant_id")
	if err != nil {
		t.Fatal(err)
	}
	if got.Count != 2 {
		t.Fatalf("count = %d, want 2", got.Count)
	}

	next, _, err := ReadSegment(reader)
	if err != nil {
		t.Fatal(err)
	}
	nextCol, ok := next.Column("tenant_id")
	if !ok {
		t.Fatal("missing second tenant_id column")
	}
	requireVectorEqual(t, vector.NewInt64([]int64{11, 12}), nextCol.Vector)
}

func TestSelectSegmentInt64Equal(t *testing.T) {
	data := selectSegmentData(t)
	runSelectSegmentInt64Equal(t, func(column string, value int64, scratch []uint32) ([]uint32, bool, error) {
		return SelectSegmentInt64Equal(bytes.NewReader(data), column, value, scratch)
	})
}

func TestSelectSegmentInt64EqualBytes(t *testing.T) {
	data := selectSegmentData(t)
	runSelectSegmentInt64Equal(t, func(column string, value int64, scratch []uint32) ([]uint32, bool, error) {
		return SelectSegmentInt64EqualBytes(data, column, value, scratch)
	})
}

func runSelectSegmentInt64Equal(t testing.TB, selectFn func(column string, value int64, scratch []uint32) ([]uint32, bool, error)) {
	t.Helper()

	scratch := make([]uint32, 0, 8)

	selected, ok, err := selectFn("tenant_id", 7, scratch)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing tenant_id column")
	}
	if !slices.Equal(selected, []uint32{0, 2}) {
		t.Fatalf("selection = %v, want [0 2]", selected)
	}
	if cap(selected) != cap(scratch) {
		t.Fatalf("scratch cap = %d, want %d", cap(selected), cap(scratch))
	}

	selected, ok, err = selectFn("tenant_id", 99, selected)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing tenant_id column")
	}
	if len(selected) != 0 {
		t.Fatalf("selection len = %d, want 0", len(selected))
	}

	_, ok, err = selectFn("missing", 7, selected)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("did not expect missing column")
	}

	_, ok, err = selectFn("event_type", 7, selected)
	if err == nil {
		t.Fatal("expected wrong type error")
	}
	if !ok {
		t.Fatal("expected event_type column to exist")
	}
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

func TestSelectSegmentInt64EqualConsumesFullSegment(t *testing.T) {
	first := writeSegmentBytes(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42})},
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout"})},
	)
	second := writeSegmentBytes(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{11, 12})},
	)

	reader := bytes.NewReader(append(first, second...))
	selected, ok, err := SelectSegmentInt64Equal(reader, "tenant_id", 7, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("missing tenant_id column")
	}
	if !slices.Equal(selected, []uint32{0}) {
		t.Fatalf("selection = %v, want [0]", selected)
	}

	next, _, err := ReadSegment(reader)
	if err != nil {
		t.Fatal(err)
	}
	nextCol, ok := next.Column("tenant_id")
	if !ok {
		t.Fatal("missing second tenant_id column")
	}
	requireVectorEqual(t, vector.NewInt64([]int64{11, 12}), nextCol.Vector)
}

func TestCanSkipInt64Equal(t *testing.T) {
	stats := ColumnStats{
		Name:      "tenant_id",
		Kind:      vector.KindInt64,
		HasMinMax: true,
		MinInt64:  7,
		MaxInt64:  42,
	}

	for _, value := range []int64{6, 43} {
		if !canSkipInt64Equal(stats, value) {
			t.Fatalf("expected value %d to skip", value)
		}
	}
	for _, value := range []int64{7, 20, 42} {
		if canSkipInt64Equal(stats, value) {
			t.Fatalf("did not expect value %d to skip", value)
		}
	}

	stats.HasMinMax = false
	if canSkipInt64Equal(stats, 6) {
		t.Fatal("did not expect skip without min/max")
	}

	stats.HasMinMax = true
	stats.Kind = vector.KindString
	if canSkipInt64Equal(stats, 6) {
		t.Fatal("did not expect skip for non-int64 stats")
	}
}

func TestReadSegmentRejectsBadInput(t *testing.T) {
	t.Run("bad magic", func(t *testing.T) {
		_, _, err := ReadSegment(bytes.NewReader([]byte("not-a-segment")))
		if err == nil {
			t.Fatal("expected invalid magic error")
		}
	})

	t.Run("unsupported version", func(t *testing.T) {
		data := []byte(segmentMagic)
		data = binary.LittleEndian.AppendUint16(data, segmentVersion+1)
		data = binary.LittleEndian.AppendUint64(data, 0)
		data = binary.LittleEndian.AppendUint32(data, 0)

		_, _, err := ReadSegment(bytes.NewReader(data))
		if err == nil {
			t.Fatal("expected unsupported version error")
		}
	})

	t.Run("unknown kind", func(t *testing.T) {
		var buf bytes.Buffer
		sw := newSegmentWriter(&buf)

		if err := sw.WriteHeader(1, 1); err != nil {
			t.Fatal(err)
		}

		stats := ColumnStats{Name: "mystery", Kind: vector.Kind(255), Count: 1, EncodedLen: 1}
		if err := sw.WriteColumn(stats, []byte{0}); err != nil {
			t.Fatal(err)
		}

		_, _, err := ReadSegment(bytes.NewReader(buf.Bytes()))
		if err == nil {
			t.Fatal("expected unknown kind error")
		}
	})

	t.Run("huge encoded column", func(t *testing.T) {
		var buf bytes.Buffer
		sw := newSegmentWriter(&buf)

		if err := sw.WriteHeader(1, 1); err != nil {
			t.Fatal(err)
		}
		stats := ColumnStats{Name: "tenant_id", Kind: vector.KindInt64, Count: 1}
		if err := sw.writeColumnHeader(stats, maxEncodedColumnLen+1); err != nil {
			t.Fatal(err)
		}

		_, _, err := ReadSegment(bytes.NewReader(buf.Bytes()))
		if err == nil {
			t.Fatal("expected huge encoded column error")
		}
	})
}

func TestWriteHeaderRejectsInvalidCounts(t *testing.T) {
	var buf bytes.Buffer
	sw := newSegmentWriter(&buf)

	if err := sw.WriteHeader(-1, 1); err == nil {
		t.Fatal("expected negative row count error")
	}
	if err := sw.WriteHeader(1, -1); err == nil {
		t.Fatal("expected negative column count error")
	}
}

func TestReadSegmentRejectsTruncatedInput(t *testing.T) {
	batch := mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{1, 2, 3})},
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "cancel"})},
	)

	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		t.Fatal(err)
	}

	data := buf.Bytes()
	for i := range data {
		_, _, err := ReadSegment(bytes.NewReader(data[:i]))
		if err == nil {
			t.Fatalf("expected error for truncated segment length %d", i)
		}

		_, err = ReadSegmentStats(bytes.NewReader(data[:i]))
		if err == nil {
			t.Fatalf("expected stats error for truncated segment length %d", i)
		}
	}
}

func FuzzSegmentRoundTrip(f *testing.F) {
	f.Add([]byte{1, 2, 3}, "signup")
	f.Add([]byte{}, "")
	f.Add([]byte{255, 0, 128}, "checkout")

	f.Fuzz(func(t *testing.T, raw []byte, label string) {
		ints := make([]int64, len(raw))
		strings := make([]string, len(raw))

		for i, value := range raw {
			ints[i] = int64(int8(value))
			strings[i] = label
		}

		batch := mustBatch(t,
			vector.Column{Name: "value", Vector: vector.NewInt64(ints)},
			vector.Column{Name: "label", Vector: vector.NewString(strings)},
		)

		_, got, _ := roundTrip(t, batch)
		requireBatchEqual(t, batch, got)
	})
}

func BenchmarkWriteSegmentInt64(b *testing.B) {
	b.ReportAllocs()

	values := make([]int64, 100_000)
	for i := range values {
		values[i] = int64(i % 1024)
	}

	batch := mustBatch(b,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64(values)},
	)

	for b.Loop() {
		var buf bytes.Buffer
		buf.Grow(900_000)
		if _, err := WriteSegment(&buf, batch); err != nil {
			b.Fatal(err)
		}
	}
}

// Measures the write path when the caller owns and reuses the output buffer.
func BenchmarkWriteSegmentInt64ReuseBuffer(b *testing.B) {
	b.ReportAllocs()

	values := make([]int64, 100_000)
	for i := range values {
		values[i] = int64(i % 1024)
	}

	batch := mustBatch(b,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64(values)},
	)

	var buf bytes.Buffer
	buf.Grow(900_000)

	for b.Loop() {
		buf.Reset()
		if _, err := WriteSegment(&buf, batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadSegmentInt64(b *testing.B) {
	b.ReportAllocs()

	values := make([]int64, 100_000)
	for i := range values {
		values[i] = int64(i % 1024)
	}

	batch := mustBatch(b,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64(values)},
	)

	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		b.Fatal(err)
	}

	data := buf.Bytes()
	b.ResetTimer()

	for b.Loop() {
		if _, _, err := ReadSegment(bytes.NewReader(data)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadSegmentStatsInt64(b *testing.B) {
	b.ReportAllocs()

	values := make([]int64, 100_000)
	for i := range values {
		values[i] = int64(i % 1024)
	}

	batch := mustBatch(b,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64(values)},
	)

	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		b.Fatal(err)
	}

	data := buf.Bytes()
	b.ResetTimer()

	for b.Loop() {
		if _, err := ReadSegmentStats(bytes.NewReader(data)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadSegmentInt64WithString(b *testing.B) {
	b.ReportAllocs()

	data := benchmarkInt64StringSegment(b)
	b.ResetTimer()

	for b.Loop() {
		if _, _, err := ReadSegment(bytes.NewReader(data)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadSegmentString(b *testing.B) {
	b.ReportAllocs()

	choices := []string{"signup", "checkout", "page_view", "cancel"}
	values := make([]string, 100_000)
	for i := range values {
		values[i] = choices[i%len(choices)]
	}

	batch := mustBatch(b,
		vector.Column{Name: "event_type", Vector: vector.NewString(values)},
	)

	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		b.Fatal(err)
	}
	data := buf.Bytes()
	b.ResetTimer()

	for b.Loop() {
		if _, _, err := ReadSegment(bytes.NewReader(data)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadSegmentColumnsInt64WithString(b *testing.B) {
	b.ReportAllocs()

	data := benchmarkInt64StringSegment(b)
	b.ResetTimer()

	for b.Loop() {
		batch, _, err := ReadSegmentColumns(bytes.NewReader(data), "tenant_id")
		if err != nil {
			b.Fatal(err)
		}
		if batch.Count != 100_000 {
			b.Fatalf("count = %d, want 100000", batch.Count)
		}
	}
}

func BenchmarkSelectSegmentInt64EqualWithString(b *testing.B) {
	b.ReportAllocs()

	data := benchmarkInt64StringSegment(b)
	scratch := make([]uint32, 0, 100_000)
	b.ResetTimer()

	for b.Loop() {
		selected, ok, err := SelectSegmentInt64Equal(bytes.NewReader(data), "tenant_id", 7, scratch)
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

func BenchmarkWriteSegmentString(b *testing.B) {
	b.ReportAllocs()

	choices := []string{"signup", "checkout", "page_view", "cancel"}
	values := make([]string, 100_000)
	for i := range values {
		values[i] = choices[i%len(choices)]
	}

	batch := mustBatch(b,
		vector.Column{Name: "event_type", Vector: vector.NewString(values)},
	)

	for b.Loop() {
		var buf bytes.Buffer
		buf.Grow(2_000_000)
		if _, err := WriteSegment(&buf, batch); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeInt64(b *testing.B) {
	b.ReportAllocs()

	values := make([]int64, 100_000)
	for i := range values {
		values[i] = int64(i % 1024)
	}

	for b.Loop() {
		_ = encodeInt64(values)
	}
}

func BenchmarkDecodeInt64(b *testing.B) {
	b.ReportAllocs()

	values := make([]int64, 100_000)
	for i := range values {
		values[i] = int64(i % 1024)
	}
	encoded := encodeInt64(values)

	for b.Loop() {
		if _, err := decodeInt64(encoded, len(values)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeString(b *testing.B) {
	b.ReportAllocs()

	choices := []string{"signup", "checkout", "page_view", "cancel"}
	values := make([]string, 100_000)
	for i := range values {
		values[i] = choices[i%len(choices)]
	}
	encoded, err := encodeString(values)
	if err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		if _, err := decodeString(encoded, len(values)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeStringPlain(b *testing.B) {
	b.ReportAllocs()

	values := make([]string, 100_000)
	for i := range values {
		values[i] = string([]byte{byte(i), byte(i >> 8), byte(i >> 16)})
	}
	encoded, err := encodeString(values)
	if err != nil {
		b.Fatal(err)
	}
	if encoded[0] != stringCodecPlain {
		b.Fatalf("string codec = %d, want plain", encoded[0])
	}

	for b.Loop() {
		if _, err := decodeString(encoded, len(values)); err != nil {
			b.Fatal(err)
		}
	}
}

func roundTrip(t testing.TB, batch vector.Batch) (SegmentStats, vector.Batch, SegmentStats) {
	t.Helper()

	var buf bytes.Buffer

	writeStats, err := WriteSegment(&buf, batch)
	if err != nil {
		t.Fatal(err)
	}

	got, readStats, err := ReadSegment(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}

	return writeStats, got, readStats
}

func writeSegmentBytes(t testing.TB, columns ...vector.Column) []byte {
	t.Helper()

	batch := mustBatch(t, columns...)
	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func plainStringPayloadLen(values []string) int {
	size := 1
	for _, value := range values {
		size += 4 + len(value)
	}
	return size
}

func selectSegmentData(t testing.TB) []byte {
	t.Helper()

	return writeSegmentBytes(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "signup", "cancel"})},
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7, 11})},
	)
}

func mustBatch(t testing.TB, columns ...vector.Column) vector.Batch {
	t.Helper()

	batch, err := vector.NewBatch(columns...)
	if err != nil {
		t.Fatal(err)
	}

	return batch
}

func requireBatchEqual(t *testing.T, want, got vector.Batch) {
	t.Helper()

	if got.Count != want.Count {
		t.Fatalf("batch count = %d, want %d", got.Count, want.Count)
	}
	if len(got.Columns) != len(want.Columns) {
		t.Fatalf("column count = %d, want %d", len(got.Columns), len(want.Columns))
	}

	for _, wantCol := range want.Columns {
		gotCol, ok := got.Column(wantCol.Name)
		if !ok {
			t.Fatalf("missing column %q", wantCol.Name)
		}

		requireVectorEqual(t, wantCol.Vector, gotCol.Vector)
	}
}

func requireVectorEqual(t *testing.T, want, got vector.Vector) {
	t.Helper()

	if got.Kind() != want.Kind() {
		t.Fatalf("kind = %s, want %s", got.Kind(), want.Kind())
	}

	switch wantValues := want.(type) {
	case vector.Int64:
		gotValues, ok := got.(vector.Int64)
		if !ok {
			t.Fatalf("got %T, want vector.Int64", got)
		}
		if !slices.Equal(gotValues.Values, wantValues.Values) {
			t.Fatalf("values = %v, want %v", gotValues.Values, wantValues.Values)
		}

	case vector.String:
		gotValues, ok := got.(vector.String)
		if !ok {
			t.Fatalf("got %T, want vector.String", got)
		}
		if gotValues.Len() != wantValues.Len() {
			t.Fatalf("values len = %d, want %d", gotValues.Len(), wantValues.Len())
		}
		for i := 0; i < wantValues.Len(); i++ {
			if gotValues.Value(i) != wantValues.Value(i) {
				t.Fatalf("value[%d] = %q, want %q", i, gotValues.Value(i), wantValues.Value(i))
			}
		}

	default:
		t.Fatalf("unsupported vector type %T", want)
	}
}

func columnStats(t *testing.T, stats SegmentStats, name string) ColumnStats {
	t.Helper()

	for _, col := range stats.Columns {
		if col.Name == name {
			return col
		}
	}

	t.Fatalf("missing column stats for %q", name)
	return ColumnStats{}
}

func benchmarkInt64StringSegment(b *testing.B) []byte {
	b.Helper()

	ids := make([]int64, 100_000)
	for i := range ids {
		ids[i] = int64(i % 1024)
	}

	events := make([]string, 100_000)
	for i := range events {
		events[i] = "event"
	}

	batch := mustBatch(b,
		vector.Column{Name: "tenant_id", Vector: vector.FromInt64(ids)},
		vector.Column{Name: "event_type", Vector: vector.FromString(events)},
	)

	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		b.Fatal(err)
	}
	return buf.Bytes()
}
