// Ingest must accumulate every batch of one call into a single multi-page segment.
package engine

import (
	"context"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// Ports the bulk accumulation and chunk-shape contracts, a full page plus two partial
// batches must land as exactly those three pages inside one segment.
func TestEngine_Ingest_BatchesAccumulateIntoOneSegment(t *testing.T) {
	db := openTestDB(t)
	mustExec(t, db, "CREATE TABLE t (id int64 NOT NULL)")

	sizes := []int{vector.StandardBatchRows, 452, 600}
	var batches []vector.Batch
	next := int64(0)
	var wantSum int64
	for _, n := range sizes {
		v := vector.NewVec(vector.VecInt64, n)
		dst := v.I64()
		for i := range n {
			dst[i] = next
			wantSum += next
			next++
		}
		b, err := vector.NewBatch([]vector.Column{{Name: "id", Type: schema.Int64, V: v}})
		if err != nil {
			t.Fatal(err)
		}
		batches = append(batches, b)
	}

	got, err := db.Ingest(context.Background(), IngestConfig{Table: "t", Batches: batches})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if got != next {
		t.Fatalf("ingested rows = %d, want %d", got, next)
	}
	wantRows(t, mustValues(t, db, "SELECT count(*), sum(id) FROM t"), [][]any{{next, wantSum}})
	shapes := activeSegmentShapes(t, db, "t")
	if len(shapes) != 1 {
		t.Fatalf("active segments = %d, want 1", len(shapes))
	}
	if pr := shapes[0].pageRows; len(pr) != 3 || pr[0] != uint32(vector.StandardBatchRows) || pr[1] != 452 || pr[2] != 600 {
		t.Fatalf("page rows = %v, want [%d 452 600]", pr, vector.StandardBatchRows)
	}
}
