// syncDir fsyncs the directory so a rename inside it is durable across a crash.
// Required on POSIX. NTFS journals metadata so the Windows variant is a no-op.
//go:build !windows

package storage

import "os"

func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
