package format

import (
	"strconv"
	"time"
)

func Duration(d time.Duration) string {
	if d < 0 {
		return "-" + Duration(-d)
	}
	if d < time.Microsecond {
		return strconv.FormatInt(d.Nanoseconds(), 10) + "ns"
	}
	if d < time.Millisecond {
		return strconv.FormatFloat(float64(d.Nanoseconds())/1_000, 'f', 1, 64) + "us"
	}
	if d < 10*time.Millisecond {
		return strconv.FormatFloat(float64(d.Nanoseconds())/1_000_000, 'f', 3, 64) + "ms"
	}
	if d < time.Second {
		return strconv.FormatFloat(float64(d.Nanoseconds())/1_000_000, 'f', 1, 64) + "ms"
	}
	return strconv.FormatFloat(d.Seconds(), 'f', 2, 64) + "s"
}

func Milliseconds(ms float64) string {
	return Duration(time.Duration(ms * float64(time.Millisecond)))
}
