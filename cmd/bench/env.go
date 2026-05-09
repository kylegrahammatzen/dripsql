package main

import (
	"runtime"
	"runtime/debug"
)

// envInfo captures the run environment so each report is reproducible.
// Commit/Dirty come from debug.ReadBuildInfo's vcs.* settings; outside a
// build (go run) those are empty and we fall back to "unknown".
type envInfo struct {
	Commit string
	Dirty  bool
	Go     string
	OS     string
	Arch   string
	CPUs   int
}

func captureEnv() envInfo {
	env := envInfo{
		Go:   runtime.Version(),
		OS:   runtime.GOOS,
		Arch: runtime.GOARCH,
		CPUs: runtime.NumCPU(),
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		env.Commit = "unknown"
		return env
	}
	env.Commit = "unknown"
	for _, setting := range info.Settings {
		switch setting.Key {
		case "vcs.revision":
			if len(setting.Value) >= 8 {
				env.Commit = setting.Value[:8]
			} else if setting.Value != "" {
				env.Commit = setting.Value
			}
		case "vcs.modified":
			env.Dirty = setting.Value == "true"
		}
	}
	return env
}
