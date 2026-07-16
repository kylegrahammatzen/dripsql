//go:build !windows

// Non-windows stub so -mode cold falls back to cold-soft with a clear message.
package main

import "errors"

func purgeStandbyList() error {
	return errors.New("standby list purge is only implemented on windows")
}
