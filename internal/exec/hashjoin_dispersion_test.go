// Verifies the maphash.Bytes + appendJoinKey path actually disperses keys across buckets.
// A regression here (constant seed swap, broken encoding) would funnel rows to one bucket.
package exec

import (
	"hash/maphash"
	"math/rand"
	"testing"
)

func TestHashJoinKey_DispersionAcrossBuckets(t *testing.T) {
	const n = 100_000
	seed := maphash.MakeSeed()
	build := hashJoinBuild{index: make(map[uint64][]hashBucket, n)}

	r := rand.New(rand.NewSource(1))
	var buf []byte
	for i := range n {
		buf = buf[:0]
		var err error
		buf, err = appendJoinKey(buf, r.Int63())
		if err != nil {
			t.Fatalf("appendJoinKey: %v", err)
		}
		build.add(buf, maphash.Bytes(seed, buf), rightRowRef{batch: 0, row: i})
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
