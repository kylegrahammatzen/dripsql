package types

import "testing"

func TestAndBoolMasksLastWord(t *testing.T) {
	left := []uint64{^uint64(0)}
	right := []uint64{^uint64(0)}

	got := AndBool(nil, left, right, 3)

	if got[0] != 0b111 {
		t.Fatalf("AndBool = %064b, want last word masked to 111", got[0])
	}
}

func TestOrBoolMasksLastWord(t *testing.T) {
	left := []uint64{0}
	right := []uint64{^uint64(0)}

	got := OrBool(nil, left, right, 3)

	if got[0] != 0b111 {
		t.Fatalf("OrBool = %064b, want last word masked to 111", got[0])
	}
}
