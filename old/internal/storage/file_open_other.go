//go:build !windows

package storage

import "os"

// openSegmentForRead opens a segment file for random-access reads. On Unix
// the equivalent posix_fadvise(POSIX_FADV_RANDOM) hint lives in
// golang.org/x/sys/unix; adding that dependency is deferred until we have a
// Unix benchmark to validate the win. Plain os.Open is correct, just leaves
// the readahead behavior up to the kernel default.
func openSegmentForRead(path string) (*os.File, error) {
	return os.Open(path)
}
