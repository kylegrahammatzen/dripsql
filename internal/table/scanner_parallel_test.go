package table

import "testing"

func TestScannerParallelCountsMatchSequential(t *testing.T) {
	tbl := createEventsTable(t, t.TempDir())
	appendBatch(t, tbl, []int64{1, 2, 3}, []string{"signup", "signup", "signup"})
	appendBatch(t, tbl, []int64{99, 99}, []string{"checkout", "checkout"})
	appendBatch(t, tbl, []int64{7, 99, 7}, []string{"cancel", "checkout", "checkout"})

	sequential := tbl.NewScanner()
	t.Cleanup(func() { _ = sequential.Close() })
	parallel := tbl.NewScanner()
	t.Cleanup(func() { _ = parallel.Close() })

	seqCount, err := sequential.CountInt64Equal("tenant_id", 99)
	if err != nil {
		t.Fatal(err)
	}
	seqStats := sequential.Stats()
	parallelCount, err := parallel.CountInt64EqualParallel("tenant_id", 99, 4)
	if err != nil {
		t.Fatal(err)
	}
	if parallelCount != seqCount {
		t.Fatalf("parallel int count = %d, want %d", parallelCount, seqCount)
	}
	if parallel.Stats() != seqStats {
		t.Fatalf("parallel int stats = %+v, want %+v", parallel.Stats(), seqStats)
	}

	seqCount, err = sequential.CountStringEqual("event_type", "checkout")
	if err != nil {
		t.Fatal(err)
	}
	seqStats = sequential.Stats()
	parallelCount, err = parallel.CountStringEqualParallel("event_type", "checkout", 0)
	if err != nil {
		t.Fatal(err)
	}
	if parallelCount != seqCount {
		t.Fatalf("parallel string count = %d, want %d", parallelCount, seqCount)
	}
	if parallel.Stats() != seqStats {
		t.Fatalf("parallel string stats = %+v, want %+v", parallel.Stats(), seqStats)
	}
}
