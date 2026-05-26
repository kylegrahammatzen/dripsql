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
		v, err := toInt64(src)
		if err != nil {
			return err
		}
		*d = int(v)
		return nil
	case *int8:
		v, err := toInt64(src)
		if err != nil {
			return err
		}
		*d = int8(v)
		return nil
	case *int16:
		v, err := toInt64(src)
		if err != nil {
			return err
		}
		*d = int16(v)
		return nil
	case *int32:
		v, err := toInt64(src)
		if err != nil {
			return err
		}
		*d = int32(v)
		return nil
	case *int64:
		v, err := toInt64(src)
		if err != nil {
			return err
		}
		*d = v
		return nil
	case *uint:
		v, err := toInt64(src)
		if err != nil {
			return err
		}
		*d = uint(v)
		return nil
	case *uint8:
		v, err := toInt64(src)
		if err != nil {
			return err
		}
		*d = uint8(v)
		return nil
	case *uint16:
		v, err := toInt64(src)
		if err != nil {
			return err
		}
		*d = uint16(v)
		return nil
	case *uint32:
		v, err := toInt64(src)
		if err != nil {
			return err
		}
		*d = uint32(v)
		return nil
	case *uint64:
		v, err := toInt64(src)
		if err != nil {
			return err
		}
		*d = uint64(v)
		return nil
	case *float32:
		v, err := toFloat64(src)
		if err != nil {
			return err
		}
		*d = float32(v)
		return nil
	case *float64:
		v, err := toFloat64(src)
		if err != nil {
			return err
		}
		*d = v
		return nil
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

func toFloat64(src any) (float64, error) {
	if src == nil {
		return 0, nil
	}
	switch v := src.(type) {
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case int:
		return float64(v), nil
	}
	return 0, fmt.Errorf("cannot convert %T to float", src)
}
