package storage

import (
	"os"
	"sync/atomic"
)

type ReadStats struct {
	Calls int64
	Bytes int64
}

var (
	physicalReadCalls atomic.Int64
	physicalReadBytes atomic.Int64
)

// readAt wraps file.ReadAt and counts every physical read against the
// process-wide stats so cmd/bench can prove a cache change actually moved I/O.
func readAt(file *os.File, buf []byte, off int64) (int, error) {
	n, err := file.ReadAt(buf, off)
	physicalReadCalls.Add(1)
	physicalReadBytes.Add(int64(n))
	return n, err
}

func currentReadStats() ReadStats {
	return ReadStats{Calls: physicalReadCalls.Load(), Bytes: physicalReadBytes.Load()}
}

func ResetReadStats() {
	physicalReadCalls.Store(0)
	physicalReadBytes.Store(0)
}

func (s *Store) ReadStats() ReadStats {
	return currentReadStats()
}
