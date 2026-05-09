package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/explain"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

// benchReport bundles every section the text and JSON renderers consume.
type benchReport struct {
	Env        envInfo                `json:"env"`
	Dir        string                 `json:"dir"`
	Profile    string                 `json:"profile"`
	Rows       int64                  `json:"rows"`
	Runs       int                    `json:"runs"`
	LoadStats  loadStats              `json:"load"`
	TableStats storage.TableStats     `json:"storage"`
	Queries    []*explain.QueryReport `json:"queries"`
}

func emitJSON(w io.Writer, report benchReport) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(report)
}

func emitText(w io.Writer, report benchReport) error {
	bw := &textWriter{w: w}
	writeHeader(bw, report)
	bw.line("")
	writeLoadSection(bw, report.LoadStats)
	bw.line("")
	writeStorageSection(bw, report.TableStats)
	bw.line("")
	writeColumnsSection(bw, report.TableStats)
	bw.line("")
	writeQueryBenchmark(bw, report.Queries)
	bw.line("")
	writeFindings(bw, report.Queries)
	return bw.err
}

func writeHeader(w *textWriter, r benchReport) {
	w.line(fmt.Sprintf("Benchmark: %s rows, %s data", commas(r.Rows), r.Profile))
	w.line("")
	w.line("DripSQL Benchmark")
	w.line("Directory:    " + r.Dir)
	w.line("Data:         " + r.Profile)
	w.line("Rows:         " + commas(r.Rows))
	w.line("Segments:     " + strconv.Itoa(r.LoadStats.Segments))
	w.line("Query runs:   " + strconv.Itoa(r.Runs))
	w.line("")
	w.line("Environment:")
	if r.Env.Commit != "unknown" {
		dirty := ""
		if r.Env.Dirty {
			dirty = " (dirty)"
		} else {
			dirty = " (clean)"
		}
		w.line("Commit:       " + r.Env.Commit + dirty)
	}
	w.line("Go:           " + r.Env.Go + " " + r.Env.OS + "/" + r.Env.Arch)
	w.line("CPUs:         " + strconv.Itoa(r.Env.CPUs))
	w.line("Storage:      " + storageProfileLine())
}

func writeLoadSection(w *textWriter, l loadStats) {
	w.line("Load Benchmark")
	w.line("Elapsed:      " + formatDuration(l.Elapsed))
	if l.Elapsed > 0 {
		throughput := float64(l.Rows) / l.Elapsed.Seconds()
		w.line("Throughput:   " + commasFloat(throughput, 0) + " rows/sec")
		mibPerSec := float64(l.BytesTotal) / l.Elapsed.Seconds() / (1024 * 1024)
		w.line("Write:        " + strconv.FormatFloat(mibPerSec, 'f', 2, 64) + " MiB/sec")
	}
	w.line("Generate:     " + formatDuration(time.Duration(l.GenerateNs)))
	w.line("Append:       " + formatDuration(time.Duration(l.AppendNs)))
}

func writeStorageSection(w *textWriter, t storage.TableStats) {
	w.line("Storage")
	w.line("Table size:        " + humanBytes(uint64(t.TableBytes)))
	w.line("Column payload:    " + humanBytes(uint64(t.ColumnPayloadBytes)))
	w.line("Storage overhead:  " + humanBytes(uint64(t.StorageOverhead)))
	w.line("Plain estimate:    " + humanBytes(uint64(t.PlainEstimate)))
	if t.ColumnPayloadBytes > 0 {
		ratio := float64(t.PlainEstimate) / float64(t.ColumnPayloadBytes)
		w.line("Table compression: " + strconv.FormatFloat(ratio, 'f', 2, 64) + "x")
	}
	if t.Rows > 0 {
		w.line("Bytes / row:       " + strconv.FormatFloat(float64(t.TableBytes)/float64(t.Rows), 'f', 2, 64))
	}
}

func writeColumnsSection(w *textWriter, t storage.TableStats) {
	w.line("Columns")
	w.line(fmt.Sprintf("%-12s %-10s %-18s %-12s %-10s %-10s %-7s %s",
		"Column", "Type", "Encoding", "Compression", "Plain", "Stored", "Ratio", "Metadata"))
	for _, col := range t.Columns {
		ratio := 1.0
		if col.StoredBytes > 0 {
			ratio = float64(col.PlainEstimate) / float64(col.StoredBytes)
		}
		encoding, compression := codecLabels(col.Codec, col.Type.Kind.String())
		w.line(fmt.Sprintf("%-12s %-10s %-18s %-12s %-10s %-10s %-7s %s",
			col.Name,
			strings.ToUpper(col.Type.String()),
			encoding,
			compression,
			humanBytes(uint64(col.PlainEstimate)),
			humanBytes(uint64(col.StoredBytes)),
			strconv.FormatFloat(ratio, 'f', 2, 64)+"x",
			columnMetadataSummary(col)))
	}
}

func columnMetadataSummary(col storage.ColumnStats) string {
	parts := make([]string, 0, 3)
	if col.HasMinMax {
		parts = append(parts, "min/max")
	}
	if col.HasTextSummary {
		parts = append(parts, "text summary")
	}
	if col.NullCount > 0 {
		parts = append(parts, "nulls")
	}
	if len(parts) == 0 {
		return "—"
	}
	return strings.Join(parts, ", ")
}

// storageProfileLine describes the on-disk shape (segment rows, page rows)
// so the bench reader knows what columnar layout produced the numbers.
func storageProfileLine() string {
	return fmt.Sprintf("columnar segments, %s rows/page, %s rows/segment",
		commas(int64(storage.DefaultPageRows)),
		commas(int64(storage.DefaultSegmentRows)))
}

func codecLabels(codec storage.Codec, typeName string) (encoding string, compression string) {
	switch codec {
	case storage.CodecPlain:
		switch typeName {
		case "text", "bytes", "json":
			return "plain varlen", "none"
		case "uuid":
			return "plain uuid16", "none"
		default:
			return "plain " + typeName, "none"
		}
	case storage.CodecConstant:
		return "constant", "none"
	case storage.CodecRLE:
		return "rle", "rle"
	case storage.CodecBitPacked:
		return "bitpacked", "bitpack"
	case storage.CodecFrameOfReference:
		return "frame_of_reference", "for"
	case storage.CodecDictionary:
		switch typeName {
		case "text", "bytes", "json":
			return "dictionary varlen", "none"
		}
		return "dictionary " + typeName, "none"
	}
	return "unknown", "none"
}

func writeQueryBenchmark(w *textWriter, queries []*explain.QueryReport) {
	w.line("Query Benchmark")
	for _, q := range queries {
		w.line("")
		if err := explain.Render(w.w, q, explain.RenderOptions{IncludeSQL: true}); err != nil {
			w.err = err
			return
		}
	}
}

func writeFindings(w *textWriter, queries []*explain.QueryReport) {
	w.line("Findings")
	if len(queries) == 0 {
		w.line("(no queries)")
		return
	}
	var slowest *explain.QueryReport
	for _, q := range queries {
		if q.Timing == nil {
			continue
		}
		if slowest == nil || q.Timing.AvgMs > slowest.Timing.AvgMs {
			slowest = q
		}
	}
	if slowest != nil && slowest.Timing != nil {
		w.line(fmt.Sprintf("Slowest:      %s (%s avg)", slowest.Name, formatMsField(slowest.Timing.AvgMs)))
	}
}

func formatMsField(ms float64) string {
	if ms >= 1000 {
		return strconv.FormatFloat(ms/1000, 'f', 2, 64) + "s"
	}
	if ms >= 1 {
		return strconv.FormatFloat(ms, 'f', 1, 64) + "ms"
	}
	if ms >= 0.001 {
		return strconv.FormatFloat(ms*1000, 'f', 1, 64) + "us"
	}
	return strconv.FormatFloat(ms*1_000_000, 'f', 0, 64) + "ns"
}

type textWriter struct {
	w   io.Writer
	err error
}

func (w *textWriter) line(s string) {
	if w.err != nil {
		return
	}
	_, w.err = fmt.Fprintln(w.w, s)
}

// commas formats an int64 with thousands separators. Mirrors the same logic
// in internal/explain so the bench shell and explain reports look uniform.
func commas(n int64) string {
	if n < 0 {
		return "-" + commas(-n)
	}
	s := strconv.FormatInt(n, 10)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	pre := len(s) % 3
	if pre > 0 {
		b.WriteString(s[:pre])
		if len(s) > pre {
			b.WriteByte(',')
		}
	}
	for i := pre; i < len(s); i += 3 {
		if i > pre {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

// commasFloat is commas with a fixed precision, used for throughput numbers.
func commasFloat(v float64, prec int) string {
	whole := int64(v)
	frac := strconv.FormatFloat(v-float64(whole), 'f', prec, 64)
	if prec == 0 {
		return commas(whole)
	}
	if len(frac) > 1 {
		return commas(whole) + frac[1:]
	}
	return commas(whole)
}

func humanBytes(b uint64) string {
	const (
		kib = 1024
		mib = 1024 * kib
		gib = 1024 * mib
	)
	switch {
	case b >= gib:
		return strconv.FormatFloat(float64(b)/gib, 'f', 2, 64) + " GiB"
	case b >= mib:
		return strconv.FormatFloat(float64(b)/mib, 'f', 2, 64) + " MiB"
	case b >= kib:
		return strconv.FormatFloat(float64(b)/kib, 'f', 2, 64) + " KiB"
	}
	return strconv.FormatUint(b, 10) + " B"
}

func formatDuration(d time.Duration) string {
	if d == 0 {
		return "0s"
	}
	if d >= time.Second {
		return strconv.FormatFloat(d.Seconds(), 'f', 2, 64) + "s"
	}
	if d >= time.Millisecond {
		return strconv.FormatFloat(float64(d.Microseconds())/1000, 'f', 1, 64) + "ms"
	}
	if d >= time.Microsecond {
		return strconv.FormatFloat(float64(d.Nanoseconds())/1000, 'f', 1, 64) + "us"
	}
	return strconv.FormatInt(d.Nanoseconds(), 10) + "ns"
}
