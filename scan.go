// Typed Scan destination dispatch for Rows.Scan.
package dripsql

import (
	"fmt"
)

func scanInto(dst, src any) error {
	if dst == nil {
		return fmt.Errorf("destination pointer is nil")
	}
	switch d := dst.(type) {
	case *any:
		*d = src
		return nil
	case *int:
		return intoInt(d, src)
	case *int8:
		return intoInt(d, src)
	case *int16:
		return intoInt(d, src)
	case *int32:
		return intoInt(d, src)
	case *int64:
		return intoInt(d, src)
	case *uint:
		return intoInt(d, src)
	case *uint8:
		return intoInt(d, src)
	case *uint16:
		return intoInt(d, src)
	case *uint32:
		return intoInt(d, src)
	case *uint64:
		return intoInt(d, src)
	case *float32:
		return intoFloat(d, src)
	case *float64:
		return intoFloat(d, src)
	case *bool:
		if src == nil {
			*d = false
			return nil
		}
		b, ok := src.(bool)
		if !ok {
			return fmt.Errorf("cannot scan %T into *bool", src)
		}
		*d = b
		return nil
	case *string:
		if src == nil {
			*d = ""
			return nil
		}
		s, ok := src.(string)
		if !ok {
			return fmt.Errorf("cannot scan %T into *string", src)
		}
		*d = s
		return nil
	case *[]byte:
		if src == nil {
			*d = nil
			return nil
		}
		switch v := src.(type) {
		case string:
			*d = []byte(v)
		case []byte:
			out := make([]byte, len(v))
			copy(out, v)
			*d = out
		default:
			return fmt.Errorf("cannot scan %T into *[]byte", src)
		}
		return nil
	}
	return fmt.Errorf("unsupported destination type %T", dst)
}

func intoInt[T ~int | ~int8 | ~int16 | ~int32 | ~int64 |
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64](d *T, src any) error {
	v, err := toInt64(src)
	if err != nil {
		return err
	}
	*d = T(v)
	return nil
}

func intoFloat[T ~float32 | ~float64](d *T, src any) error {
	switch v := src.(type) {
	case nil:
		*d = 0
	case float64:
		*d = T(v)
	case float32:
		*d = T(v)
	case int64:
		*d = T(v)
	case int:
		*d = T(v)
	default:
		return fmt.Errorf("cannot convert %T to float", src)
	}
	return nil
}

func toInt64(src any) (int64, error) {
	if src == nil {
		return 0, nil
	}
	switch v := src.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case uint64:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case float64:
		return int64(v), nil
	}
	return 0, fmt.Errorf("cannot convert %T to integer", src)
}
