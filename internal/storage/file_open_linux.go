// Linux targeted-read open where posix_fadvise(POSIX_FADV_RANDOM) tells the kernel to skip
// read-ahead so cold-open issues only the bytes ReadAt actually requests.
//go:build linux

package storage

import (
	"os"
	"syscall"
)

const posixFadvRandom = 1

// OpenRandomAccess opens path read-only and hints POSIX_FADV_RANDOM to the kernel.
func OpenRandomAccess(path string) (*os.File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	_, _, errno := syscall.Syscall6(
		syscall.SYS_FADVISE64,
		f.Fd(),
		0, 0,
		posixFadvRandom,
		0, 0,
	)
	if errno != 0 {
		// Hint failure is non-fatal because the file is still readable.
		_ = errno
	}
	return f, nil
}
