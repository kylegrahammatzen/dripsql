// Verifies the maphash.Bytes + encodeRow path actually disperses keys across buckets.
// A regression here (constant seed swap, broken encoding) would funnel rows to one bucket.
package exec

import (
	"hash/maphash"
	"math/rand"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestHashJoinKey_DispersionAcrossBuckets(t *testing.T) {
	const batchRows = 2048
	const batches = 50
	const n = batchRows * batches
	cols := []joinKeyCol{{idx: 0, kind: vector.VecInt64}}
	enc := keyEncoder{seed: maphash.MakeSeed()}
	build := hashJoinBuild{index: make(map[uint64][]hashBucket, n)}
	r := rand.New(rand.NewSource(1))
	for bi := range batches {
		v := vector.NewVec(vector.VecInt64, batchRows)
		vals := v.I64()
		for i := range batchRows {
			vals[i] = r.Int63()
		}
		batch, err := vector.NewBatch([]vector.Column{{Name: "k", Type: schema.Int64, V: v}})
		if err != nil {
			t.Fatalf("NewBatch: %v", err)
		}
		for i := range batchRows {
			key, sum, ok, err := enc.encodeRow(batch, cols, i)
			if err != nil || !ok {
				t.Fatalf("encodeRow batch %d row %d: ok=%v err=%v", bi, i, ok, err)
			}
			build.add(key, sum, rightRowRef{batch: bi, row: i})
		}
	}

	distinctHashes := len(build.index)
	if distinctHashes < n*99/100 {
		t.Fatalf("dispersion: %d distinct hashes for %d keys (want >= %d for 99%%)", distinctHashes, n, n*99/100)
	}
	maxBucket := 0
	for _, bucket := range build.index {
		if len(bucket) > maxBucket {
			maxBucket = len(bucket)
		}
	}
	if maxBucket > 4 {
		t.Fatalf("max collision chain %d (>4) suggests degenerate hashing", maxBucket)
	}
}
