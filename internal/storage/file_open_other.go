// Non-Windows, non-Linux fallback that opens read-only with no kernel hint.
// macOS would use fcntl F_RDADVISE which is range-based rather than a mode flag.
//go:build !windows && !linux

package storage

import "os"

// OpenRandomAccess opens path read-only without any platform-specific hint.
func OpenRandomAccess(path string) (*os.File, error) {
	return os.Open(path)
}
