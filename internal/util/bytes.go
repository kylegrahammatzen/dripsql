package util

// BytesEqualString compares bytes with a string without allocating.
func BytesEqualString(buf []byte, value string) bool {
	if len(buf) != len(value) {
		return false
	}
	for i, b := range buf {
		if b != value[i] {
			return false
		}
	}
	return true
}
