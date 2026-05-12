package format

import "strconv"

// Bytes renders a byte count using binary IEC suffixes (KiB/MiB/GiB/TiB).
// Negative values are formatted with a leading minus. Whole-value inputs in
// the byte range render without decimals; everything else uses two decimals.
func Bytes(n float64) string {
	if n < 0 {
		return "-" + Bytes(-n)
	}
	const unit = 1024
	if n < unit {
		if n == float64(int64(n)) {
			return strconv.FormatInt(int64(n), 10) + " B"
		}
		return strconv.FormatFloat(n, 'f', 2, 64) + " B"
	}
	value := n
	units := []string{"KiB", "MiB", "GiB", "TiB"}
	for i, suffix := range units {
		value /= unit
		if value < unit || i == len(units)-1 {
			return strconv.FormatFloat(value, 'f', 2, 64) + " " + suffix
		}
	}
	return strconv.FormatFloat(n, 'f', 2, 64) + " B"
}
