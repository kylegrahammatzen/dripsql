package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/storage/codec"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

var (
	storageWriteBenchMetaSink  SegmentMeta
	storageWriteBenchPageSink  codec.Page
	storageWriteBenchBatchSink types.Batch
)

func BenchmarkStorageWritePipeline(b *testing.B) {
	for _, rows := range []int{DefaultSegmentRows, 4 * DefaultSegmentRows, 16 * DefaultSegmentRows} {
		b.Run(fmt.Sprintf("rows_%d", rows), func(b *testing.B) {
			batches := pipelineBenchStructuredBatches(b, rows)
			dir := b.TempDir()
			path := filepath.Join(dir, "segment.dsv3")
			meta, err := WriteSegment(path, 0, batches)
			if err != nil {
				b.Fatalf("warm WriteSegment: %v", err)
			}
			size := pipelineBenchFileSize(b, path)
			b.SetBytes(size)
			b.ReportAllocs()

			var sink SegmentMeta
			b.ResetTimer()
			start := time.Now()
			for i := 0; i < b.N; i++ {
				sink, err = WriteSegment(path, SegmentID(i+1), batches)
				if err != nil {
					b.Fatalf("WriteSegment: %v", err)
				}
			}
			elapsed := time.Since(start)
			b.StopTimer()
			b.ReportMetric(float64(totalBatchRows(batches)), "rows/op")
			b.ReportMetric(float64(totalColumnPages(meta)), "pages/op")
			if elapsed > 0 {
				b.ReportMetric(float64(rows)*float64(b.N)/elapsed.Seconds(), "rows/s")
			}
			storageWriteBenchMetaSink = sink
		})
	}
}

func BenchmarkStorageEncodePage(b *testing.B) {
	for _, tc := range pipelineBenchColumns(b) {
		b.Run(tc.name, func(b *testing.B) {
			page, _, err := encodeSegmentPageInto(tc.column.V, nil)
			if err != nil {
				b.Fatalf("warm encodeSegmentPageInto: %v", err)
			}
			b.SetBytes(pipelineBenchVecPlainBytes(tc.column.V))
			b.ReportAllocs()

			var sink codec.Page
			scratch := make([]byte, 0, len(page.Payload))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sink, scratch, err = encodeSegmentPageInto(tc.column.V, scratch)
				if err != nil {
					b.Fatalf("encodeSegmentPageInto: %v", err)
				}
			}
			b.ReportMetric(float64(len(sink.Payload)), "payload_B/op")
			storageWriteBenchPageSink = sink
		})
	}
}

func BenchmarkStorageDecodePage(b *testing.B) {
	for _, tc := range pipelineBenchColumns(b) {
		page, _, err := encodeSegmentPageInto(tc.column.V, nil)
		if err != nil {
			b.Fatalf("encode %s: %v", tc.name, err)
		}
		colMeta := ColumnMeta{Name: tc.column.Name, Type: tc.column.Type, Rows: uint32(page.Rows)}
		pageMeta := PageMeta{Rows: uint32(page.Rows), NullCount: uint32(page.NullCount), Kind: page.Kind, Encoding: page.Encoding, Length: uint64(len(page.Payload))}
		b.Run(tc.name+"/alloc", func(b *testing.B) {
			b.SetBytes(int64(len(page.Payload)))
			b.ReportAllocs()
			var sink types.Batch
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				col, err := decodeColumnPageInto(colMeta, pageMeta, page.Payload, nil)
				if err != nil {
					b.Fatalf("decodeColumnPageInto: %v", err)
				}
				sink, err = types.NewBatchNoClone([]types.Column{col})
				if err != nil {
					b.Fatalf("NewBatchNoClone: %v", err)
				}
			}
			storageWriteBenchBatchSink = sink
		})
		b.Run(tc.name+"/reuse", func(b *testing.B) {
			b.SetBytes(int64(len(page.Payload)))
			b.ReportAllocs()
			var dst types.Vec
			var sink types.Batch
			if _, err := decodeColumnPageInto(colMeta, pageMeta, page.Payload, &dst); err != nil {
				b.Fatalf("warm decodeColumnPageInto: %v", err)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				col, err := decodeColumnPageInto(colMeta, pageMeta, page.Payload, &dst)
				if err != nil {
					b.Fatalf("decodeColumnPageInto: %v", err)
				}
				sink, err = types.NewBatchNoClone([]types.Column{col})
				if err != nil {
					b.Fatalf("NewBatchNoClone: %v", err)
				}
			}
			storageWriteBenchBatchSink = sink
		})
	}
}

func BenchmarkStorageReadSegmentDecode(b *testing.B) {
	rows := DefaultSegmentRows
	batches := pipelineBenchStructuredBatches(b, rows)
	path := filepath.Join(b.TempDir(), "segment.dsv3")
	meta, err := WriteSegment(path, 1, batches)
	if err != nil {
		b.Fatalf("WriteSegment: %v", err)
	}
	size := pipelineBenchFileSize(b, path)
	infos, err := buildSegmentPageInfos(meta, nil)
	if err != nil {
		b.Fatalf("buildSegmentPageInfos: %v", err)
	}
	for _, tc := range []struct {
		name    string
		columns []string
	}{
		{name: "all_columns"},
		{name: "flat_text", columns: []string{"event_uuid", "url"}},
		{name: "encoded_numeric", columns: []string{"tenant_id", "user_id", "created_at", "amount"}},
		{name: "dictionary_text", columns: []string{"status", "path", "event_type", "country"}},
	} {
		b.Run(tc.name, func(b *testing.B) {
			cache := newSegmentFileCache()
			defer func() {
				if err := cache.Close(); err != nil {
					b.Fatalf("cache.Close: %v", err)
				}
			}()
			plan, err := newSegmentReadPlan(path, &meta, tc.columns, size, infos, cache)
			if err != nil {
				b.Fatalf("newSegmentReadPlan: %v", err)
			}
			bytesPerSegment := pipelineBenchPageInfoBytes(plan.PageInfos)
			b.SetBytes(bytesPerSegment)
			b.ReportAllocs()

			var sink types.Batch
			if sink, _, err = plan.ReadBatchReused(0); err != nil {
				b.Fatalf("warm ReadBatchReused: %v", err)
			}
			b.ResetTimer()
			start := time.Now()
			for i := 0; i < b.N; i++ {
				for pageIndex := range plan.PageInfos {
					sink, _, err = plan.ReadBatchReused(pageIndex)
					if err != nil {
						b.Fatalf("ReadBatchReused: %v", err)
					}
				}
			}
			elapsed := time.Since(start)
			b.StopTimer()
			b.ReportMetric(float64(rows), "rows/op")
			if elapsed > 0 {
				b.ReportMetric(float64(rows)*float64(b.N)/elapsed.Seconds(), "rows/s")
			}
			storageWriteBenchBatchSink = sink
		})
	}
}

type pipelineBenchColumn struct {
	name   string
	column types.Column
}

func pipelineBenchColumns(tb testing.TB) []pipelineBenchColumn {
	tb.Helper()
	n := types.StandardBatchRows
	batch := pipelineBenchStructuredBatch(0, n)
	return []pipelineBenchColumn{
		{name: "tenant_id_for", column: batch.Columns[0]},
		{name: "event_uuid_flat_text", column: batch.Columns[3]},
		{name: "status_dictionary_text", column: batch.Columns[4]},
		{name: "url_flat_text", column: batch.Columns[6]},
		{name: "amount_for", column: batch.Columns[7]},
	}
}

func pipelineBenchStructuredBatches(tb testing.TB, rows int) []types.Batch {
	tb.Helper()
	if rows <= 0 {
		tb.Fatalf("rows must be positive")
	}
	batches := make([]types.Batch, 0, (rows+types.StandardBatchRows-1)/types.StandardBatchRows)
	for start := 0; start < rows; start += types.StandardBatchRows {
		n := types.StandardBatchRows
		if start+n > rows {
			n = rows - start
		}
		batches = append(batches, pipelineBenchStructuredBatch(int64(start), n))
	}
	return batches
}

func pipelineBenchStructuredBatch(start int64, n int) types.Batch {
	batch, err := types.NewBatchNoClone([]types.Column{
		pipelineBenchInt64Column("tenant_id", start, n, func(row int64) int64 { return row%1024 + 1 }),
		pipelineBenchInt64Column("user_id", start, n, func(row int64) int64 { return row + 1 }),
		pipelineBenchInt64Column("created_at", start, n, func(row int64) int64 { return 1_700_000_000_000 + row*1000 }),
		pipelineBenchTextColumn("event_uuid", start, n, 40, pipelineBenchUUID),
		pipelineBenchTextColumn("status", start, n, 8, func(row int64) string { return pipelineBenchStatuses[row%int64(len(pipelineBenchStatuses))] }),
		pipelineBenchTextColumn("path", start, n, 18, func(row int64) string { return pipelineBenchPaths[row%int64(len(pipelineBenchPaths))] }),
		pipelineBenchTextColumn("url", start, n, 64, pipelineBenchURL),
		pipelineBenchInt64Column("amount", start, n, func(row int64) int64 { return row % 100 }),
		pipelineBenchTextColumn("event_type", start, n, 10, func(row int64) string { return pipelineBenchEventTypes[row%int64(len(pipelineBenchEventTypes))] }),
		pipelineBenchTextColumn("country", start, n, 2, func(row int64) string { return pipelineBenchCountries[row%int64(len(pipelineBenchCountries))] }),
	})
	if err != nil {
		panic(err)
	}
	return batch
}

func pipelineBenchInt64Column(name string, start int64, n int, value func(int64) int64) types.Column {
	values := make([]int64, n)
	for i := range n {
		values[i] = value(start + int64(i))
	}
	return types.Column{Name: name, Type: types.Int64, V: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: n, I64: values}}
}

func pipelineBenchTextColumn(name string, start int64, n int, bytesPerRow int, value func(int64) string) types.Column {
	varBytes := types.NewVarBytes(n, n*bytesPerRow)
	for i := range n {
		varBytes.AppendString(i, value(start+int64(i)))
	}
	return types.Column{Name: name, Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: n, Var: varBytes}}
}

func pipelineBenchVecPlainBytes(v types.Vec) int64 {
	switch v.Kind {
	case types.VecInt64, types.VecTimestamp, types.VecTime:
		return int64(v.Len * 8)
	case types.VecText:
		return int64(len(v.Var.Offsets)*4 + len(v.Var.Data))
	default:
		return int64(v.Len)
	}
}

func pipelineBenchPageInfoBytes(infos []SegmentPageInfo) int64 {
	var total int64
	for _, info := range infos {
		total += int64(info.PayloadBytes)
	}
	return total
}

func totalColumnPages(meta SegmentMeta) int {
	total := 0
	for _, column := range meta.Columns {
		total += len(column.Pages)
	}
	return total
}

func pipelineBenchFileSize(tb testing.TB, path string) int64 {
	tb.Helper()
	info, err := os.Stat(path)
	if err != nil {
		tb.Fatalf("Stat: %v", err)
	}
	return info.Size()
}

func pipelineBenchURL(row int64) string {
	return "/users/" + strconv.FormatInt(row%100_000, 10) + "/events/" + strconv.FormatInt(row, 10) + "/" + strconv.FormatUint(pipelineBenchMix64(row, pipelineBenchSaltURL), 36)
}

func pipelineBenchUUID(row int64) string {
	left := pipelineBenchMix64(row, pipelineBenchSaltUUID)
	right := pipelineBenchMix64(row, pipelineBenchSaltUUID+1)
	return strconv.FormatUint(left>>32, 16) + "-" + strconv.FormatUint(left&0xffffffff, 16) + "-" + strconv.FormatUint(right>>32, 16) + "-" + strconv.FormatUint(right&0xffffffff, 16)
}

func pipelineBenchMix64(row int64, salt uint64) uint64 {
	x := uint64(row) ^ salt
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

const (
	pipelineBenchSaltURL  uint64 = 0x27d4eb2f165667c5
	pipelineBenchSaltUUID uint64 = 0x319642b2d24d8ec3
)

var pipelineBenchEventTypes = []string{"signup", "checkout", "page_view", "cancel"}
var pipelineBenchCountries = []string{"US", "CA", "GB", "DE", "FR", "JP", "BR", "AU"}
var pipelineBenchStatuses = []string{"new", "queued", "paid", "failed", "refunded", "archived"}
var pipelineBenchPaths = []string{"/", "/products", "/products/detail", "/cart", "/checkout/start", "/checkout/confirm", "/account", "/support", "/pricing", "/search", "/docs", "/logout"}
