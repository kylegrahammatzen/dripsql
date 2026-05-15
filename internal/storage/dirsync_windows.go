// syncDir is a no-op on Windows. NTFS journals metadata so a rename's parent dir
// does not need an explicit flush to survive a crash.
//go:build windows

package storage

func syncDir(path string) error { return nil }
