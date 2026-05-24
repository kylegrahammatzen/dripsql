// Default build: spans capture wall time and counters.
// Build with `-tags dripsql_spans_off` to elide the timing path entirely.

//go:build !dripsql_spans_off

package storage

import "time"

const SpansEnabled = true

func NewSpan(name string) *Span {
	return &Span{Name: name, start: time.Now()}
}
