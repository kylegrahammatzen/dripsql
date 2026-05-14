//go:build !windows

package main

// envOnBattery is a no-op on non-Windows platforms; the Linux equivalent
// would query /sys/class/power_supply/AC*/online but adding that
// dependency is deferred until we run benches on Linux dev boxes.
func envOnBattery() (bool, bool) {
	return false, false
}
