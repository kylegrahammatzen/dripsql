# Bench artifacts

This directory captures the pre-recode benchmark baseline plus per-phase artifacts produced by the recode.

## Capture conditions (T00 baseline)

- Date: 2026-05-25
- Host CPU: AMD Ryzen 7 3700X 8-Core (16 logical processors, SMT on)
- OS: Microsoft Windows 11 Home, build 26200
- Go: go1.26.0 windows/amd64
- Power plan: unknown (admin-only query failed, plan defaults to Balanced unless changed)
- Git SHA at micro-bench capture: 1a7622b (post Phase 1 ceremony sweep)
- Bench protocol: -count=10 -benchtime=1s, benchstat-readable

## Files

- baseline.txt: micro-bench output (`go test ./internal/exec ./internal/storage ./internal/storage/codec ./internal/types -bench=. -benchmem`)
- baseline_workload.json: workload bench JSONL (one object per query+rows pair from `cmd/bench`)
- phase<N>.txt: per-phase verification snapshot, captured at each Phase Verify task

## Sequencing note

T00a (micro-bench baseline) ran on the pre-Phase-1 tree but its launch raced the Phase 1 ceremony commits, so the captured binary reflects pre-Phase-1 code while the working tree has moved on. Phase 1 is provably zero-behavior-change so this is not load-bearing.

T00b (workload baseline) ran after Phase 1 commits because the bench harness rebuilds on each invocation, again Phase 1 being zero-behavior keeps the numbers comparable to a strict pre-recode capture.

Skipped queries in T00b are logged at the bottom of baseline_workload.json or noted here if they exceeded the 240s per-call timeout.

## Variance gate

Per `drip_recode.md` §9: comparison via `benchstat`, variance ≤10% on this Windows host.
