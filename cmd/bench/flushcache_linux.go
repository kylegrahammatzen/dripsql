// drop_caches=3 writes via root to flush page cache + dentries + inodes between runs.
// Returns a descriptive error when not running as root; caller decides whether to abort.
//go:build linux

package main

import (
	"fmt"
	"os"
)

func flushOSPageCache() error {
	if os.Geteuid() != 0 {
		return fmt.Errorf("cold-hard on linux requires root (drop_caches needs uid=0); rerun under sudo or use cold-soft")
	}
	if err := writeSyncControl(); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	f, err := os.OpenFile("/proc/sys/vm/drop_caches", os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("open drop_caches: %w", err)
	}
	defer f.Close()
	if _, err := f.Write([]byte("3\n")); err != nil {
		return fmt.Errorf("write drop_caches: %w", err)
	}
	return nil
}

func writeSyncControl() error {
	f, err := os.OpenFile("/proc/sys/vm/drop_caches", os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	_ = f.Close()
	return nil
}
