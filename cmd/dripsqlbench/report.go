package main

import (
	"fmt"
	"os"
	"slices"
	"text/tabwriter"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/table"
)

func printSummary(dir string, mode string, rows int64, segmentRows int, segmentRowsSource string, workers int, segments int) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "DripSQL Benchmark")
	fmt.Fprintln(w)
	fmt.Fprintf(w, "Directory:\t%s\n", dir)
	fmt.Fprintf(w, "Mode:\t%s\n", mode)
	fmt.Fprintf(w, "Rows:\t%s\n", commas(rows))
	fmt.Fprintf(w, "Segments:\t%s\n", commas(segments))
	fmt.Fprintf(w, "Segment rows:\t%s\n", formatSegmentRows(segmentRows, segmentRowsSource))
	fmt.Fprintf(w, "Count workers:\t%s\n", commas(workers))
	if segments >= 10_000 {
		fmt.Fprintln(w, "Note:\tlarge segment count; rebuilding with current automatic row groups reduces per-segment scan overhead")
	}
	w.Flush()
}

func printLoadBenchmark(rows int64, tableBytes int64, loadElapsed time.Duration) {
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
	fmt.Fprintf(w, "Table size:\t%s\n", formatMiB(tableBytes))
	fmt.Fprintf(w, "Column payload:\t%s\n", formatMiB(encodedBytes))
	fmt.Fprintf(w, "Storage overhead:\t%s\n", formatBytes(overheadBytes))
	fmt.Fprintf(w, "Plain estimate:\t%s\n", formatMiB(plainBytes))
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
	fmt.Fprintln(w, "Column\tType\tCodec\tPlain\tEncoded\tRatio\tDict\tMin/max")
	for _, col := range stats {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			col.Name, col.Kind, formatCodec(col.Codec), formatMiB(col.PlainBytes), formatMiB(col.EncodedBytes),
			formatRatio(col.CompressionRatio()), formatDictionaryValues(col), formatMinMax(col))
	}
	w.Flush()
}

func printQueries(results []benchResult, rows int64) {
	const mib = 1024 * 1024
	fmt.Println()
	fmt.Println("Queries")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "Query\tResult\tSamples\tFirst\tBest\tAvg\tRows/sec\tMiB/sec\tScan\tSegments\tRows skipped")
	for _, result := range results {
		rowsScanned := result.Stats.RowsScanned
		if rowsScanned == 0 && result.Stats.RowsSkipped == 0 {
			rowsScanned = rows
		}
		rowsPerSec, mibPerSec := queryRates(result, rowsScanned)
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%.2f\t%.2f MiB\t%d/%d\t%s\n",
			result.Name, commas(result.Count), commas(result.Samples), formatDuration(result.First), formatDuration(result.Best),
			formatDuration(result.Avg), commas(int64(rowsPerSec)), mibPerSec, float64(result.Stats.BytesScanned)/mib,
			result.Stats.SegmentsScanned, result.Stats.SegmentsTotal, commas(result.Stats.RowsSkipped))
	}
	w.Flush()
}

func queryRates(result benchResult, rowsScanned int64) (float64, float64) {
	if result.Avg <= 0 {
		return 0, 0
	}
	const mib = 1024 * 1024
	return float64(rowsScanned) / result.Avg.Seconds(), float64(result.Stats.BytesScanned) / mib / result.Avg.Seconds()
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

func formatSegmentRows(segmentRows int, source string) string {
	if source == "" {
		return commas(segmentRows)
	}
	return fmt.Sprintf("%s (%s)", commas(segmentRows), source)
}

func formatMiB(bytes int64) string {
	return fmt.Sprintf("%.2f MiB", float64(bytes)/(1024*1024))
}

func formatBytes(bytes int64) string {
	if bytes < 1024*1024 {
		return fmt.Sprintf("%.2f KiB", float64(bytes)/1024)
	}
	return formatMiB(bytes)
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

func commas[T ~int | ~int64](n T) string {
	s := fmt.Sprint(n)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}
