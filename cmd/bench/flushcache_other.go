// Fallback for platforms with no documented page-cache flush path. cold-hard is unavailable.
//go:build !linux && !windows

package main

import "fmt"

func flushOSPageCache() error {
	return fmt.Errorf("cold-hard mode is not implemented on this platform; use cold-soft")
}
