// Windows targeted-read open passes FILE_FLAG_RANDOM_ACCESS to CreateFile so the
// cache manager skips read-ahead and tracks footer and page hits as random accesses.
//go:build windows

package storage

import (
	"os"
	"syscall"
)

const fileFlagRandomAccess = 0x10000000

// OpenRandomAccess opens path read-only with FILE_FLAG_RANDOM_ACCESS.
// Use this for segment readers that issue scattered ReadAt calls.
func OpenRandomAccess(path string) (*os.File, error) {
	utf16, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	h, err := syscall.CreateFile(
		utf16,
		syscall.GENERIC_READ,
		syscall.FILE_SHARE_READ,
		nil,
		syscall.OPEN_EXISTING,
		syscall.FILE_ATTRIBUTE_NORMAL|fileFlagRandomAccess,
		0,
	)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	return os.NewFile(uintptr(h), path), nil
}
