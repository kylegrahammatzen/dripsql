package main

import (
	"syscall"
	"unsafe"
)

var (
	kernel32                 = syscall.NewLazyDLL("kernel32.dll")
	procGetSystemPowerStatus = kernel32.NewProc("GetSystemPowerStatus")
)

// systemPowerStatus mirrors the Win32 SYSTEM_POWER_STATUS struct.
type systemPowerStatus struct {
	ACLineStatus        byte
	BatteryFlag         byte
	BatteryLifePercent  byte
	SystemStatusFlag    byte
	BatteryLifeTime     uint32
	BatteryFullLifeTime uint32
}

// envOnBattery returns true when the laptop is running on battery, which
// is enough to invalidate any cold-cache or thermal-sensitive bench
// number on Windows.
func envOnBattery() (bool, bool) {
	var status systemPowerStatus
	ret, _, _ := procGetSystemPowerStatus.Call(uintptr(unsafe.Pointer(&status)))
	if ret == 0 {
		return false, false
	}
	return status.ACLineStatus == 0, true
}
