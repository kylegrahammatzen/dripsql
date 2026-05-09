// Package sqltype contains DripSQL logical SQL types.
package sqltype

type Kind uint8

const (
	KindInvalid Kind = iota
	KindBool
	KindInt16
	KindInt32
	KindInt64
	KindFloat32
	KindFloat64
	KindDecimal
	KindText
	KindBytes
	KindUUID
	KindTimestamp
	KindTime
	KindDate
	KindJSON
	KindNamed
)

var (
	Bool      = Type{Kind: KindBool}
	Int16     = Type{Kind: KindInt16}
	Int32     = Type{Kind: KindInt32}
	Int64     = Type{Kind: KindInt64}
	Float32   = Type{Kind: KindFloat32}
	Float64   = Type{Kind: KindFloat64}
	Decimal   = Type{Kind: KindDecimal}
	Text      = Type{Kind: KindText}
	Bytes     = Type{Kind: KindBytes}
	UUID      = Type{Kind: KindUUID}
	Timestamp = Type{Kind: KindTimestamp}
	Time      = Type{Kind: KindTime}
	Date      = Type{Kind: KindDate}
	JSON      = Type{Kind: KindJSON}
)

type Type struct {
	Kind Kind
	Name string
}

func Named(name string) Type {
	return Type{Kind: KindNamed, Name: name}
}

func Parse(name string) Type {
	switch name {
	case "bool":
		return Bool
	case "int16":
		return Int16
	case "int32":
		return Int32
	case "int64":
		return Int64
	case "float32":
		return Float32
	case "float64":
		return Float64
	case "decimal":
		return Decimal
	case "text":
		return Text
	case "bytes":
		return Bytes
	case "uuid":
		return UUID
	case "timestamp":
		return Timestamp
	case "time":
		return Time
	case "date":
		return Date
	case "json":
		return JSON
	default:
		return Named(name)
	}
}

func (t Type) Valid() bool {
	return t.Kind != KindInvalid && (t.Kind != KindNamed || t.Name != "")
}

func (t Type) IsNamed() bool {
	return t.Kind == KindNamed
}

func (t Type) String() string {
	if t.Name != "" {
		return t.Name
	}
	return t.Kind.String()
}

func (k Kind) String() string {
	if int(k) < len(kindNames) {
		return kindNames[k]
	}
	return "invalid"
}

var kindNames = [...]string{
	KindInvalid:   "invalid",
	KindBool:      "bool",
	KindInt16:     "int16",
	KindInt32:     "int32",
	KindInt64:     "int64",
	KindFloat32:   "float32",
	KindFloat64:   "float64",
	KindDecimal:   "decimal",
	KindText:      "text",
	KindBytes:     "bytes",
	KindUUID:      "uuid",
	KindTimestamp: "timestamp",
	KindTime:      "time",
	KindDate:      "date",
	KindJSON:      "json",
	KindNamed:     "named",
}
