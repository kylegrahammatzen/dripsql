// Process-wide IO and decode timing counters used by the bench harness.
package storage

import (
	"sync/atomic"
	"time"
)

func nowNanos() int64 { return time.Now().UnixNano() }

var (
	timingIOReadNs int64
	timingDecodeNs int64
)

func ResetTimings() {
	atomic.StoreInt64(&timingIOReadNs, 0)
	atomic.StoreInt64(&timingDecodeNs, 0)
}

func ReadTimings() (ioNs, decodeNs int64) {
	return atomic.LoadInt64(&timingIOReadNs), atomic.LoadInt64(&timingDecodeNs)
}

func addIORead(ns int64) { atomic.AddInt64(&timingIOReadNs, ns) }
func addDecode(ns int64) { atomic.AddInt64(&timingDecodeNs, ns) }
