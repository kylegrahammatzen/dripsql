// SetSystemFileCacheSize via kernel32 is the documented programmatic path. We instead shell to
// `EmptyStandbyList.exe standbylist` if it is on PATH (Sysinternals or Sergei Beziuk's tool).
//go:build windows

package main

import (
	"fmt"
	"os/exec"
)

func flushOSPageCache() error {
	path, err := exec.LookPath("EmptyStandbyList.exe")
	if err != nil {
		return fmt.Errorf("cold-hard on windows needs EmptyStandbyList.exe on PATH (run elevated) or use cold-soft")
	}
	cmd := exec.Command(path, "standbylist")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("EmptyStandbyList: %w (output: %s)", err, string(out))
	}
	return nil
}
