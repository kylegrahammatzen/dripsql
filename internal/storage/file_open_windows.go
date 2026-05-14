package storage

import (
	"os"
	"syscall"
)

// fileFlagRandomAccess hints to the Windows cache manager that we will seek
// rather than stream, so it should not pull megabyte-sized readahead chunks.
// Cold-ish 100M profiling showed 61% of CPU in syscall.ReadFile from the
// readahead of segment heads we never touched, so this flag is the dominant
// PR 1 win.
const fileFlagRandomAccess = 0x10000000

func openSegmentForRead(path string) (*os.File, error) {
	pathPtr, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	handle, err := syscall.CreateFile(
		pathPtr,
		syscall.GENERIC_READ,
		syscall.FILE_SHARE_READ,
		nil,
		syscall.OPEN_EXISTING,
		fileFlagRandomAccess,
		0,
	)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	return os.NewFile(uintptr(handle), path), nil
}
