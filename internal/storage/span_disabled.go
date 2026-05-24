// Disabled-spans build: NewSpan returns nil so every Span method becomes a nil-receiver
// no-op. Use `-tags dripsql_spans_off` on hot prod paths where the per-phase timing
// overhead matters more than EXPLAIN ANALYZE introspection.

//go:build dripsql_spans_off

package storage

const SpansEnabled = false

func NewSpan(name string) *Span { return nil }
