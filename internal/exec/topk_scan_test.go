package exec

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestTopKScan_MaterializesOnlyWinningProjectionPages(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "topk.dsv4")
	if err := storage.WriteSegment(path, []vector.Batch{
		makeTopKScanBatch(t, []string{"winner", "cold"}, []int64{100, 0}),
		makeTopKScanBatch(t, []string{"loser-a", "loser-b"}, []int64{99, 98}),
	}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, err := storage.OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	namePage := seg.Cols[0].Pages[1]
	if err := seg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	corruptPagePayload(t, path, namePage)
	seg, err = storage.OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment after corrupt: %v", err)
	}
	defer seg.Close()

	op := &TopKScanOp{
		Segments: []*storage.Segment{seg},
		Columns:  []string{"name", "age"},
		Kinds:    []vector.VecKind{vector.VecText, vector.VecInt64},
		Key:      "age",
		Desc:     true,
		K:        1,
	}
	if err := op.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	out, ok, err := op.Next()
	if err != nil || !ok {
		t.Fatalf("Next ok=%v err=%v", ok, err)
	}
	if got := out.Columns[0].V.Var().String(0); got != "winner" {
		t.Fatalf("name got %q want winner", got)
	}
	if got := out.Columns[1].V.I64()[0]; got != 100 {
		t.Fatalf("age got %d want 100", got)
	}
}

func makeTopKScanBatch(t *testing.T, names []string, ages []int64) vector.Batch {
	t.Helper()
	nameVec := vector.NewVarVec(vector.VecText, len(names), 0)
	ageVec := vector.NewVec(vector.VecInt64, len(ages))
	for i, name := range names {
		nameVec.Var().AppendString(i, name)
		ageVec.I64()[i] = ages[i]
	}
	b, err := vector.NewBatch([]vector.Column{
		{Name: "name", Type: schema.Text, V: nameVec},
		{Name: "age", Type: schema.Int64, V: ageVec},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return b
}

func corruptPagePayload(t *testing.T, path string, page storage.Page) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer f.Close()
	zeros := make([]byte, page.PayloadLength)
	if _, err := f.WriteAt(zeros, int64(page.PayloadOffset)); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
}
