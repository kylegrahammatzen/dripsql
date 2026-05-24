// ScanOpts.ReadTs filters segments whose CommitTs exceeds the cutoff. The check happens
// before predicate binding so it composes with the existing prune fast path.
package storage

import (
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestScanOpts_ReadTs_SkipsNewerSegments(t *testing.T) {
	tmp := t.TempDir()
	mk := func(name string, val int64) *Segment {
		path := filepath.Join(tmp, name)
		v := types.NewVec(types.VecInt64, 1)
		v.I64()[0] = val
		batch, _ := types.NewBatch([]types.Column{{Name: "id", Type: types.Int64, V: v}})
		if _, err := WriteSegment(path, []types.Batch{batch}, nil); err != nil {
			t.Fatalf("WriteSegment: %v", err)
		}
		s, err := OpenSegment(path)
		if err != nil {
			t.Fatalf("OpenSegment: %v", err)
		}
		return s
	}
	older := mk("older.dsv4", 10)
	older.CommitTs = 5
	newer := mk("newer.dsv4", 20)
	newer.CommitTs = 15
	defer older.Close()
	defer newer.Close()

	type row struct {
		commitTs uint64
		val      int64
	}
	collect := func(readTs uint64) []row {
		var got []row
		err := Scan(ScanOpts{
			Segments: []*Segment{older, newer},
			ReadTs:   readTs,
		}, func(batch types.Batch, sel *types.SelectionMask) error {
			col, _ := batch.ColumnByName("id")
			sel.IterSet(func(r int) {
				got = append(got, row{val: col.V.I64()[r]})
			})
			return nil
		})
		if err != nil {
			t.Fatalf("Scan(readTs=%d): %v", readTs, err)
		}
		return got
	}

	// ReadTs above both: sees both.
	if got := collect(100); len(got) != 2 {
		t.Errorf("ReadTs=100 returned %d rows, want 2", len(got))
	}
	// ReadTs strictly above the older only: sees only older.
	if got := collect(10); len(got) != 1 || got[0].val != 10 {
		t.Errorf("ReadTs=10 returned %v, want [{val:10}]", got)
	}
	// ReadTs below all: sees nothing.
	if got := collect(1); len(got) != 0 {
		t.Errorf("ReadTs=1 returned %v, want []", got)
	}
	// ReadTs == 0 (sentinel "no cutoff"): sees both -- preserves pre-MVCC behavior.
	if got := collect(0); len(got) != 2 {
		t.Errorf("ReadTs=0 returned %d rows, want 2 (no cutoff)", len(got))
	}
}
