package main

import (
	"fmt"
	"os"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/table"
)

func printBenchmarkRun(index int, total int, rows int64) {
	if index > 1 {
		fmt.Println()
	}
	fmt.Printf("Benchmark %d/%d: %s rows\n", index, total, commas(rows))
}

func printSummary(dir string, mode string, rows int64, segmentRows int, segmentRowsSource string, workers int, data string, seed uint64, segments int) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "DripSQL Benchmark")
	fmt.Fprintln(w)
	fmt.Fprintf(w, "Directory:\t%s\n", dir)
	fmt.Fprintf(w, "Mode:\t%s\n", mode)
	fmt.Fprintf(w, "Data:\t%s\n", formatData(data, seed))
	fmt.Fprintf(w, "Rows:\t%s\n", commas(rows))
	fmt.Fprintf(w, "Segments:\t%s\n", commas(segments))
	fmt.Fprintf(w, "Segment rows:\t%s\n", formatSegmentRows(segmentRows, segmentRowsSource))
	if mode == "load" {
		fmt.Fprintln(w, "Load workers:\t1 (serial)")
	}
	fmt.Fprintf(w, "Query workers:\t%s\n", commas(workers))
	if segments >= 10_000 {
		fmt.Fprintln(w, "Note:\tlarge segment count; rebuilding with current automatic row groups reduces per-segment scan overhead")
	}
	w.Flush()
}

func printLoadBenchmark(rows int64, tableBytes int64, loadElapsed time.Duration, stats loadStats) {
	const mib = 1024 * 1024
	rowsPerSec := 0.0
	mibPerSec := 0.0
	if loadElapsed > 0 {
		rowsPerSec = float64(rows) / loadElapsed.Seconds()
		mibPerSec = float64(tableBytes) / mib / loadElapsed.Seconds()
	}
	fmt.Println()
	fmt.Println("Load Benchmark")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "Elapsed:\t%s\n", loadElapsed)
	fmt.Fprintf(w, "Throughput:\t%s rows/sec\n", commas(int64(rowsPerSec)))
	fmt.Fprintf(w, "Write:\t%.2f MiB/sec\n", mibPerSec)
	if stats.Segments > 0 {
		fmt.Fprintf(w, "Generate:\t%s (%s/segment avg, %s max)\n", stats.Generate, avgDuration(stats.Generate, stats.Segments), stats.MaxGenerate)
		fmt.Fprintf(w, "Append/encode/write:\t%s (%s/segment avg, %s max)\n", stats.Append, avgDuration(stats.Append, stats.Segments), stats.MaxAppend)
		fmt.Fprintf(w, "Close/manifest:\t%s\n", stats.Close)
	}
	w.Flush()
}

func printStorage(rows int64, tableBytes int64, stats []table.ColumnStorageStats) {
	plainBytes, encodedBytes := storagePayloadBytes(stats)
	overheadBytes := tableBytes - encodedBytes
	if overheadBytes < 0 {
		overheadBytes = 0
	}

	fmt.Println()
	fmt.Println("Storage")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "Table size:\t%s\n", formatBytes(tableBytes))
	fmt.Fprintf(w, "Column payload:\t%s\n", formatBytes(encodedBytes))
	if bloomBytes := storageBloomBytes(stats); bloomBytes > 0 {
		fmt.Fprintf(w, "Value indexes:\t%s\n", formatBytes(bloomBytes))
	}
	fmt.Fprintf(w, "Storage overhead:\t%s\n", formatBytes(overheadBytes))
	fmt.Fprintf(w, "Plain estimate:\t%s\n", formatBytes(plainBytes))
	fmt.Fprintf(w, "Column compression:\t%s\n", formatRatio(ratio(plainBytes, encodedBytes)))
	fmt.Fprintf(w, "Table compression:\t%s\n", formatRatio(ratio(plainBytes, tableBytes)))
	if rows > 0 {
		fmt.Fprintf(w, "Bytes / row:\t%.2f\n", float64(tableBytes)/float64(rows))
	}
	w.Flush()
}

func storagePayloadBytes(stats []table.ColumnStorageStats) (plainBytes int64, encodedBytes int64) {
	for _, col := range stats {
		plainBytes += col.PlainBytes
		encodedBytes += col.EncodedBytes
	}
	return plainBytes, encodedBytes
}

func storageBloomBytes(stats []table.ColumnStorageStats) int64 {
	var bloomBytes int64
	for _, col := range stats {
		bloomBytes += col.BloomBytes
	}
	return bloomBytes
}

func ratio(numerator int64, denominator int64) float64 {
	if denominator == 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func printColumns(stats []table.ColumnStorageStats) {
	fmt.Println()
	fmt.Println("Columns")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Column\tType\tCodec\tPlain\tEncoded\tRatio\tDict\tMin/max\tFeatures")
	for _, col := range stats {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			col.Name, col.Kind, formatCodec(col.Codec), formatBytes(col.PlainBytes), formatBytes(col.EncodedBytes),
			formatRatio(col.CompressionRatio()), formatDictionaryValues(col), formatMinMax(col), formatColumnFeatures(col))
	}
	w.Flush()
}

func printQueries(results []benchResult) {
	fmt.Println()
	fmt.Println("Queries")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Query\tResult\tSamples\tFirst\tBest\tAvg\tMiB/sec\tScan\tSegments\tRows skipped")
	for _, result := range results {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%.2f\t%s\t%d/%d\t%s\n",
			result.Name, commas(result.Count), commas(result.Samples), formatDuration(result.First), formatDuration(result.Best),
			formatDuration(result.Avg), queryMiBPerSec(result), formatBytes(result.Stats.BytesScanned),
			result.Stats.SegmentsScanned, result.Stats.SegmentsTotal, commas(result.Stats.RowsSkipped))
	}
	w.Flush()
}

func queryMiBPerSec(result benchResult) float64 {
	if result.Avg <= 0 {
		return 0
	}
	const mib = 1024 * 1024
	return float64(result.Stats.BytesScanned) / mib / result.Avg.Seconds()
}

func printGroups(results []groupResult) {
	if len(results) == 0 {
		return
	}
	fmt.Println()
	fmt.Println("Group Results")
	for _, result := range results {
		keys := make([]string, 0, len(result.Counts))
		for key := range result.Counts {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintf(w, "%s\n", result.Name)
		for _, key := range keys {
			fmt.Fprintf(w, "%s:\t%s\n", key, commas(result.Counts[key]))
		}
		w.Flush()
	}
}

func formatDuration(duration time.Duration) string {
	if duration == 0 {
		return "<1ns"
	}
	return duration.String()
}

func avgDuration(total time.Duration, count int) time.Duration {
	if count <= 0 {
		return 0
	}
	return total / time.Duration(count)
}

func formatSegmentRows(segmentRows int, source string) string {
	if source == "" {
		return commas(segmentRows)
	}
	return fmt.Sprintf("%s (%s)", commas(segmentRows), source)
}

func formatBytes(bytes int64) string {
	const (
		kib = 1024
		mib = 1024 * kib
		gib = 1024 * mib
	)
	if bytes < kib {
		return fmt.Sprintf("%d B", bytes)
	}
	if bytes < mib {
		return fmt.Sprintf("%.2f KiB", float64(bytes)/kib)
	}
	if bytes < gib {
		return fmt.Sprintf("%.2f MiB", float64(bytes)/mib)
	}
	return fmt.Sprintf("%.2f GiB", float64(bytes)/gib)
}

func formatRatio(ratio float64) string {
	if ratio == 0 {
		return "-"
	}
	return fmt.Sprintf("%.2fx", ratio)
}

func formatCodec(codec string) string {
	if codec == "dictionary" {
		return "dict"
	}
	return codec
}

func formatData(data string, seed uint64) string {
	if data == string(dataModeStructured) {
		return data
	}
	return fmt.Sprintf("%s (seed %d)", data, seed)
}

func formatDictionaryValues(stats table.ColumnStorageStats) string {
	if stats.DictionarySegments == 0 {
		return "-"
	}
	if stats.MinDictionaryValues == stats.MaxDictionaryValues {
		return fmt.Sprintf("%s", commas(stats.MinDictionaryValues))
	}
	return fmt.Sprintf("%.0f avg (%s-%s)", stats.AverageDictionaryValues(), commas(stats.MinDictionaryValues), commas(stats.MaxDictionaryValues))
}

func formatMinMax(stats table.ColumnStorageStats) string {
	if stats.MinMaxSegments == 0 {
		return "-"
	}
	return fmt.Sprintf("%d..%d", stats.MinInt64, stats.MaxInt64)
}

func formatColumnFeatures(stats table.ColumnStorageStats) string {
	features := make([]string, 0, 3)
	if stats.MinMaxSegments > 0 {
		features = append(features, "range skip")
	}
	if stats.DictionarySegments > 0 {
		features = append(features, "fast equality")
		if stats.GroupPath == "dict-counts" {
			features = append(features, "fast group")
		}
	}
	if stats.FilterPath == "sequence" {
		features = append(features, "fast equality")
	}
	if stats.BloomSegments > 0 {
		features = append(features, "value skip")
	}
	if len(features) == 0 {
		return "-"
	}
	return strings.Join(features, ", ")
}

func commas[T ~int | ~int64](n T) string {
	s := fmt.Sprint(n)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}
