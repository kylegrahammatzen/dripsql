package types

import "fmt"

func ParseUUID(value string) (UUID16, error) {
	if len(value) != 36 {
		return UUID16{}, fmt.Errorf("invalid UUID literal %q", value)
	}
	if value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return UUID16{}, fmt.Errorf("invalid UUID literal %q", value)
	}
	var out UUID16
	byteIndex := 0
	for i := 0; i < len(value); {
		if value[i] == '-' {
			i++
			continue
		}
		if i+1 >= len(value) {
			return UUID16{}, fmt.Errorf("invalid UUID literal %q", value)
		}
		hi, ok := hexNibble(value[i])
		if !ok {
			return UUID16{}, fmt.Errorf("invalid UUID literal %q", value)
		}
		lo, ok := hexNibble(value[i+1])
		if !ok {
			return UUID16{}, fmt.Errorf("invalid UUID literal %q", value)
		}
		if byteIndex >= len(out) {
			return UUID16{}, fmt.Errorf("invalid UUID literal %q", value)
		}
		out[byteIndex] = hi<<4 | lo
		byteIndex++
		i += 2
	}
	if byteIndex != len(out) {
		return UUID16{}, fmt.Errorf("invalid UUID literal %q", value)
	}
	return out, nil
}

func FormatUUID(value UUID16) string {
	const digits = "0123456789abcdef"
	out := make([]byte, 36)
	byteIndex := 0
	for i := range out {
		switch i {
		case 8, 13, 18, 23:
			out[i] = '-'
		default:
			b := value[byteIndex/2]
			if byteIndex%2 == 0 {
				out[i] = digits[b>>4]
			} else {
				out[i] = digits[b&0x0f]
			}
			byteIndex++
		}
	}
	return string(out)
}

func hexNibble(value byte) (byte, bool) {
	switch {
	case value >= '0' && value <= '9':
		return value - '0', true
	case value >= 'a' && value <= 'f':
		return value - 'a' + 10, true
	case value >= 'A' && value <= 'F':
		return value - 'A' + 10, true
	default:
		return 0, false
	}
}
