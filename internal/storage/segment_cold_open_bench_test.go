package storage

import (
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

// BenchmarkColdOpenReadFooters measures the parallel-read cost that
// dominates cold-open after Phase A's lazy decode. It writes N small
// segments to a temp dir, then in each iteration reads every footer in
// parallel. Regressions here flag added work in ReadSegmentFooter or its
// allocations; the 100M cold-ish bench is the end-to-end signal.
func BenchmarkColdOpenReadFooters(b *testing.B) {
	const segments = 64
	dir := b.TempDir()
	paths := make([]string, segments)
	batch := coldOpenBenchBatch(b)
	for i := range paths {
		paths[i] = filepath.Join(dir, "seg-"+strconv.Itoa(i)+".dsv3")
		if _, err := WriteSegment(paths[i], SegmentID(i+1), []types.Batch{batch}); err != nil {
			b.Fatalf("WriteSegment: %v", err)
		}
	}

	workers := runtime.GOMAXPROCS(0)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var wg sync.WaitGroup
		next := make(chan int)
		for range workers {
			wg.Go(func() {
				for idx := range next {
					if _, _, err := ReadSegmentFooter(paths[idx]); err != nil {
						b.Errorf("ReadSegmentFooter: %v", err)
						return
					}
				}
			})
		}
		for i := range paths {
			next <- i
		}
		close(next)
		wg.Wait()
	}
}

func coldOpenBenchBatch(b *testing.B) types.Batch {
	b.Helper()
	const rows = types.StandardBatchRows
	ids := make([]int64, rows)
	events := make([]string, rows)
	for i := range ids {
		ids[i] = int64(i)
		events[i] = "checkout"
		if i%5 == 0 {
			events[i] = "login"
		} else if i%7 == 0 {
			events[i] = "signup"
		}
	}
	varText := types.NewVarBytes(rows, rows*8)
	for i, v := range events {
		varText.AppendString(i, v)
	}
	batch, err := types.NewBatch([]types.Column{
		{Name: "tenant_id", Type: types.Int64, V: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: rows, I64: ids}},
		{Name: "event_type", Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: rows, Var: varText}},
	})
	if err != nil {
		b.Fatalf("NewBatch: %v", err)
	}
	return batch
}
