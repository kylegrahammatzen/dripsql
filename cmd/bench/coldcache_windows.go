//go:build windows

// True cold mode support that evicts the Windows standby list so reopened segments hit disk.
package main

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Values from the SYSTEM_MEMORY_LIST_INFORMATION class of NtSetSystemInformation.
const (
	systemMemoryListInformation = 80
	memoryPurgeStandbyList      = uint32(4)
)

var procNtSetSystemInformation = windows.NewLazySystemDLL("ntdll.dll").NewProc("NtSetSystemInformation")

// purgeStandbyList enables SeProfileSingleProcessPrivilege and drops cached standby pages, failing cleanly when not elevated.
func purgeStandbyList() error {
	var token windows.Token
	if err := windows.OpenProcessToken(windows.CurrentProcess(), windows.TOKEN_ADJUST_PRIVILEGES|windows.TOKEN_QUERY, &token); err != nil {
		return fmt.Errorf("open process token: %w", err)
	}
	defer token.Close()
	name, err := windows.UTF16PtrFromString("SeProfileSingleProcessPrivilege")
	if err != nil {
		return err
	}
	var luid windows.LUID
	if err := windows.LookupPrivilegeValue(nil, name, &luid); err != nil {
		return fmt.Errorf("lookup SeProfileSingleProcessPrivilege: %w", err)
	}
	tp := windows.Tokenprivileges{PrivilegeCount: 1}
	tp.Privileges[0] = windows.LUIDAndAttributes{Luid: luid, Attributes: windows.SE_PRIVILEGE_ENABLED}
	// A filtered non-elevated token lacks the privilege, which surfaces here as ERROR_NOT_ALL_ASSIGNED.
	if err := windows.AdjustTokenPrivileges(token, false, &tp, 0, nil, nil); err != nil {
		return fmt.Errorf("enable SeProfileSingleProcessPrivilege: %w", err)
	}
	cmd := memoryPurgeStandbyList
	status, _, _ := procNtSetSystemInformation.Call(systemMemoryListInformation, uintptr(unsafe.Pointer(&cmd)), unsafe.Sizeof(cmd))
	if status != 0 {
		return fmt.Errorf("NtSetSystemInformation returned status 0x%08x", status)
	}
	return nil
}
