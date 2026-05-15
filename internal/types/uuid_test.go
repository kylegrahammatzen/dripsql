// UUID invariant tests: canonical, bare-32 hex, and uppercase all round-trip.
// Parse and String are the boundary between SQL literals and the 16-byte value.
package types

import (
	"strings"
	"testing"
)

func TestUUID_StringRoundTrip(t *testing.T) {
	canonical := "550e8400-e29b-41d4-a716-446655440000"
	u, err := ParseUUID(canonical)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if u.String() != canonical {
		t.Fatalf("round-trip: got %q want %q", u.String(), canonical)
	}
	upper, err := ParseUUID(strings.ToUpper(canonical))
	if err != nil {
		t.Fatalf("uppercase parse: %v", err)
	}
	if upper != u {
		t.Fatal("uppercase parse must match canonical")
	}
	bare, err := ParseUUID("550e8400e29b41d4a716446655440000")
	if err != nil {
		t.Fatalf("bare parse: %v", err)
	}
	if bare != u {
		t.Fatal("bare hex must match canonical")
	}
}
