package types

import (
	"encoding/hex"
	"fmt"
	"strings"
)

// UUID16 is the 16-byte UUID representation used everywhere a UUID value lives.
type UUID16 [16]byte

// ParseUUID accepts the canonical 36-character hyphenated form and a lenient 32-character bare hex form.
func ParseUUID(s string) (UUID16, error) {
	switch len(s) {
	case 36:
		if s[8] != '-' || s[13] != '-' || s[18] != '-' || s[23] != '-' {
			return UUID16{}, fmt.Errorf("uuid %q has misplaced hyphens", s)
		}
		bare := s[0:8] + s[9:13] + s[14:18] + s[19:23] + s[24:]
		return parseUUIDHex(bare)
	case 32:
		return parseUUIDHex(s)
	default:
		return UUID16{}, fmt.Errorf("uuid %q has invalid length", s)
	}
}

func parseUUIDHex(bare string) (UUID16, error) {
	var u UUID16
	if _, err := hex.Decode(u[:], []byte(strings.ToLower(bare))); err != nil {
		return UUID16{}, fmt.Errorf("uuid %q: %w", bare, err)
	}
	return u, nil
}

// FormatUUID returns the canonical 36-character hyphenated form.
func FormatUUID(u UUID16) string {
	var buf [36]byte
	hex.Encode(buf[0:8], u[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], u[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], u[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], u[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], u[10:16])
	return string(buf[:])
}
