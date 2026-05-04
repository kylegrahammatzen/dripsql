package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
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
	batch := mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42, 7})},
	)

	writeStats, _, readStats := roundTrip(t, batch)

	for label, stats := range map[string]ColumnStats{
		"written": columnStats(t, writeStats, "tenant_id"),
		"read":    columnStats(t, readStats, "tenant_id"),
	} {
		if !stats.HasMinMax {
			t.Fatalf("expected %s min/max stats", label)
		}
		if stats.MinInt64 != 7 || stats.MaxInt64 != 42 {
			t.Fatalf("%s min/max = %d/%d, want 7/42", label, stats.MinInt64, stats.MaxInt64)
		}
		if stats.EncodedLen != 24 {
			t.Fatalf("%s EncodedLen = %d, want 24", label, stats.EncodedLen)
		}
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
		data := append([]byte(nil), segmentMagic...)
		data = binary.LittleEndian.AppendUint16(data, segmentVersion+1)

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
		if err := sw.string16("tenant_id"); err != nil {
			t.Fatal(err)
		}
		if err := sw.u8(uint8(vector.KindInt64)); err != nil {
			t.Fatal(err)
		}
		if err := sw.u64(1); err != nil {
			t.Fatal(err)
		}
		if err := sw.u64(maxEncodedColumnLen + 1); err != nil {
			t.Fatal(err)
		}

		_, _, err := ReadSegment(bytes.NewReader(buf.Bytes()))
		if err == nil {
			t.Fatal("expected huge encoded column error")
		}
	})
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
		t.Run(fmt.Sprintf("len=%d", i), func(t *testing.T) {
			_, _, err := ReadSegment(bytes.NewReader(data[:i]))
			if err == nil {
				t.Fatal("expected error for truncated segment")
			}
		})
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
		if !slices.Equal(gotValues.Values, wantValues.Values) {
			t.Fatalf("values = %v, want %v", gotValues.Values, wantValues.Values)
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
