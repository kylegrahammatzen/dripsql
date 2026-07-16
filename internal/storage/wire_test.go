// Wire format tests, Page entry round-trip and footer suffix magic check.
package storage

import (
	"testing"
	"unsafe"
)

func TestPage_EntrySize_32(t *testing.T) {
	if got := unsafe.Sizeof(Page{}); got != PageEntrySize {
		t.Fatalf("sizeof Page = %d, want %d", got, PageEntrySize)
	}
}

func TestPage_WriteReadRoundTrip(t *testing.T) {
	p := Page{
		PayloadOffset: 0x1122334455667788,
		PayloadLength: 0x99AABBCCDDEEFF00,
		RowStart:      0xDEADBEEF,
		Rows:          2048,
		NullCount:     17,
		Kind:          5,
		Encoding:      3,
		Flags:         PageFlagAllValid | PageFlagInMembership,
		Reserved:      0,
	}
	var buf [PageEntrySize]byte
	WritePage(buf[:], p)
	got := DecodePageEntry(buf[:])
	if got != p {
		t.Fatalf("round-trip mismatch: got %+v want %+v", got, p)
	}
}

func TestPage_FlagBits(t *testing.T) {
	cases := []uint8{PageFlagAllValid, PageFlagAllNull, PageFlagInMembership, PageFlagEncodedEvalOK}
	seen := make(map[uint8]bool, len(cases))
	for _, f := range cases {
		if seen[f] {
			t.Fatalf("duplicate flag bit %b", f)
		}
		seen[f] = true
	}
}

func TestFooterSuffix_RoundTrip(t *testing.T) {
	var buf [FooterSuffixSize]byte
	WriteFooterSuffix(buf[:], 12345, 6789)
	version, length, sidecar, err := ReadFooterSuffix(buf[:])
	if err != nil {
		t.Fatalf("round-trip: %v", err)
	}
	if version != SegmentFormatVersion {
		t.Fatalf("version = %d, want %d", version, SegmentFormatVersion)
	}
	if length != 12345 {
		t.Fatalf("length = %d, want 12345", length)
	}
	if sidecar != 6789 {
		t.Fatalf("sidecar = %d, want 6789", sidecar)
	}
}

func TestFooterSuffix_RejectsBadMagic(t *testing.T) {
	var buf [FooterSuffixSize]byte
	WriteFooterSuffix(buf[:], 1, 0)
	buf[FooterSuffixSize-1] = 'X'
	if _, _, _, err := ReadFooterSuffix(buf[:]); err == nil {
		t.Fatal("bad magic must be rejected")
	}
}

func TestFooterSuffix_RejectsReservedFlagsAndBadVersion(t *testing.T) {
	var buf [FooterSuffixSize]byte
	WriteFooterSuffix(buf[:], 1, 0)
	buf[4] = 1
	if _, _, _, err := ReadFooterSuffix(buf[:]); err == nil {
		t.Fatal("nonzero reserved flags must be rejected")
	}
	WriteFooterSuffix(buf[:], 1, 0)
	buf[0] = SegmentFormatVersion + 1
	if _, _, _, err := ReadFooterSuffix(buf[:]); err == nil {
		t.Fatal("unknown newer format version must be rejected")
	}
}

func TestMagic_Current(t *testing.T) {
	if Magic != "DRIPV4S4" {
		t.Fatalf("magic = %q, want DRIPV4S4", Magic)
	}
	if len(Magic) != MagicLen || len(MagicV3) != MagicLen {
		t.Fatalf("magic lengths %d/%d != %d", len(Magic), len(MagicV3), MagicLen)
	}
}
