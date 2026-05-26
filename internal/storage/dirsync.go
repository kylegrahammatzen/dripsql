// syncDir fsyncs a directory so a rename inside it survives a crash; NTFS journals metadata so Windows is a no-op.
package storage

import (
	"os"
	"runtime"
)

func syncDir(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
