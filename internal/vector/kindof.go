package vector

import (
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
)

func KindOf(t sqltype.Type) (Kind, error) {
	switch t.Kind {
	case sqltype.KindBool:
		return Bool, nil
	case sqltype.KindInt16:
		return Int16, nil
	case sqltype.KindInt32:
		return Int32, nil
	case sqltype.KindInt64:
		return Int64, nil
	case sqltype.KindFloat32:
		return Float32, nil
	case sqltype.KindFloat64:
		return Float64, nil
	case sqltype.KindDecimal:
		return Decimal64, nil
	case sqltype.KindText:
		return Text, nil
	case sqltype.KindBytes:
		return Bytes, nil
	case sqltype.KindUUID:
		return UUID, nil
	case sqltype.KindTimestamp:
		return Timestamp, nil
	case sqltype.KindTime:
		return Time, nil
	case sqltype.KindDate:
		return Date, nil
	case sqltype.KindJSON:
		return JSON, nil
	case sqltype.KindNamed:
		if t.Name != "" {
			return Enum32, nil
		}
	}
	return Invalid, fmt.Errorf("unsupported vector type %s", t)
}
