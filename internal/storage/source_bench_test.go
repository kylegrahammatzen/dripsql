package storage

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func BenchmarkReaderSourceCountInt64(b *testing.B) {
	const rows = 100_000
	values := make([]int64, rows)
	for i := range values {
		values[i] = int64(i % 10)
	}
	batch := mustBenchmarkBatch(b, vector.Column{Name: "tenant_id", Vector: vector.FromInt64(values)})
	data := mustBenchmarkSegmentBytes(b, batch)
	benchReaderSources(b, data, func(b *testing.B, reader Reader, scratch []byte) {
		meta, _ := reader.Column("tenant_id")
		if meta.Codec != CodecDictionary {
			b.Fatalf("tenant_id codec = %s, want dictionary", meta.Codec)
		}
		b.ReportAllocs()

		count, nextScratch, stats, err := CountInt64Equal(reader, "tenant_id", 7, scratch)
		if err != nil {
			b.Fatalf("CountInt64Equal() warmup error = %v", err)
		}
		scratch = nextScratch
		b.SetBytes(stats.BytesScanned)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			count, nextScratch, _, err = CountInt64Equal(reader, "tenant_id", 7, scratch)
			if err != nil {
				b.Fatalf("CountInt64Equal() error = %v", err)
			}
			scratch = nextScratch
		}
		benchmarkCountResult = count
		if benchmarkCountResult != rows/10 {
			b.Fatalf("count = %d, want %d", benchmarkCountResult, rows/10)
		}
	})
}

func BenchmarkReaderSourceCountString(b *testing.B) {
	const rows = 100_000
	choices := []string{"signup", "checkout", "view", "cancel"}
	values := make([]string, rows)
	for i := range values {
		values[i] = choices[i%len(choices)]
	}
	batch := mustBenchmarkBatch(b, vector.Column{Name: "event_type", Vector: vector.FromString(values)})
	data := mustBenchmarkSegmentBytes(b, batch)
	benchReaderSources(b, data, func(b *testing.B, reader Reader, scratch []byte) {
		meta, _ := reader.Column("event_type")
		if meta.Codec != CodecDictionary {
			b.Fatalf("event_type codec = %s, want dictionary", meta.Codec)
		}
		b.ReportAllocs()

		count, nextScratch, stats, err := CountStringEqual(reader, "event_type", "checkout", scratch)
		if err != nil {
			b.Fatalf("CountStringEqual() warmup error = %v", err)
		}
		scratch = nextScratch
		b.SetBytes(stats.BytesScanned)
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			count, nextScratch, _, err = CountStringEqual(reader, "event_type", "checkout", scratch)
			if err != nil {
				b.Fatalf("CountStringEqual() error = %v", err)
			}
			scratch = nextScratch
		}
		benchmarkCountResult = count
		if benchmarkCountResult != rows/len(choices) {
			b.Fatalf("count = %d, want %d", benchmarkCountResult, rows/len(choices))
		}
	})
}

func BenchmarkOpenSegmentReaderSources(b *testing.B) {
	const rows = 100_000
	values := make([]string, rows)
	for i := range values {
		values[i] = pageBloomValue(i)
	}
	batch := mustBenchmarkBatch(b, vector.Column{Name: "url", Vector: vector.FromString(values)})
	data := mustBenchmarkSegmentBytes(b, batch)

	for _, sourceName := range []string{"bytes_view", "bytes_reader_at", "file_read_at", "file_seek_read"} {
		sourceName := sourceName
		b.Run(sourceName, func(b *testing.B) {
			open := openSegmentBenchmarkSource(b, sourceName, data)
			var scratch []byte
			reader, nextScratch, err := open(scratch)
			if err != nil {
				b.Fatalf("OpenSegment() warmup error = %v", err)
			}
			scratch = nextScratch
			if reader.Directory().Rows != rows {
				b.Fatalf("rows = %d, want %d", reader.Directory().Rows, rows)
			}

			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				reader, nextScratch, err = open(scratch)
				if err != nil {
					b.Fatalf("OpenSegment() error = %v", err)
				}
				scratch = nextScratch
				if reader.dir.Rows != rows {
					b.Fatalf("rows = %d, want %d", reader.dir.Rows, rows)
				}
			}
		})
	}
}

func openSegmentBenchmarkSource(b *testing.B, name string, data []byte) func([]byte) (Reader, []byte, error) {
	b.Helper()
	switch name {
	case "bytes_view":
		return func(scratch []byte) (Reader, []byte, error) {
			reader, err := OpenSegmentBytes(data)
			return reader, scratch, err
		}
	case "bytes_reader_at":
		source := bytes.NewReader(data)
		return func(scratch []byte) (Reader, []byte, error) {
			return OpenSegment(source, 0, int64(len(data)), scratch)
		}
	case "file_read_at":
		file := mustBenchmarkSegmentFile(b, data)
		return func(scratch []byte) (Reader, []byte, error) {
			return OpenSegment(file, 0, int64(len(data)), scratch)
		}
	case "file_seek_read":
		file := mustBenchmarkSegmentFile(b, data)
		source := seekReadAt{r: file}
		return func(scratch []byte) (Reader, []byte, error) {
			return OpenSegment(source, 0, int64(len(data)), scratch)
		}
	default:
		b.Fatalf("unknown benchmark source %q", name)
		return nil
	}
}

func benchReaderSources(b *testing.B, data []byte, bench func(*testing.B, Reader, []byte)) {
	b.Helper()
	for _, source := range []struct {
		name string
		open func(*testing.B) (Reader, []byte)
	}{
		{
			name: "bytes_view",
			open: func(b *testing.B) (Reader, []byte) {
				reader, err := OpenSegmentBytes(data)
				if err != nil {
					b.Fatalf("OpenSegmentBytes() error = %v", err)
				}
				return reader, nil
			},
		},
		{
			name: "bytes_reader_at",
			open: func(b *testing.B) (Reader, []byte) {
				reader, scratch, err := OpenSegment(bytes.NewReader(data), 0, int64(len(data)), nil)
				if err != nil {
					b.Fatalf("OpenSegment() error = %v", err)
				}
				return reader, scratch
			},
		},
		{
			name: "file_read_at",
			open: func(b *testing.B) (Reader, []byte) {
				file := mustBenchmarkSegmentFile(b, data)
				reader, scratch, err := OpenSegment(file, 0, int64(len(data)), nil)
				if err != nil {
					b.Fatalf("OpenSegment() error = %v", err)
				}
				return reader, scratch
			},
		},
		{
			name: "file_seek_read",
			open: func(b *testing.B) (Reader, []byte) {
				file := mustBenchmarkSegmentFile(b, data)
				reader, scratch, err := OpenSegment(seekReadAt{r: file}, 0, int64(len(data)), nil)
				if err != nil {
					b.Fatalf("OpenSegment() error = %v", err)
				}
				return reader, scratch
			},
		},
	} {
		source := source
		b.Run(source.name, func(b *testing.B) {
			reader, scratch := source.open(b)
			bench(b, reader, scratch)
		})
	}
}

func mustBenchmarkSegmentFile(b *testing.B, data []byte) *os.File {
	b.Helper()
	path := filepath.Join(b.TempDir(), "segment.drip")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		b.Fatalf("WriteFile() error = %v", err)
	}
	file, err := os.Open(path)
	if err != nil {
		b.Fatalf("Open() error = %v", err)
	}
	b.Cleanup(func() {
		_ = file.Close()
	})
	return file
}

type seekReadAt struct {
	r io.ReadSeeker
}

func (r seekReadAt) ReadAt(buf []byte, off int64) (int, error) {
	if _, err := r.r.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}
	return io.ReadFull(r.r, buf)
}
