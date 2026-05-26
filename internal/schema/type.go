// Type is the SQL-side data type referenced by parser, binder, and exec.
// Logical-kind name, parse, and physical-kind mapping all run as single switches.
package schema

import "strings"

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

type Type struct {
	Kind Kind
	Name string
}

func (k Kind) String() string {
	switch k {
	case KindBool:
		return "bool"
	case KindInt16:
		return "int16"
	case KindInt32:
		return "int32"
	case KindInt64:
		return "int64"
	case KindFloat32:
		return "float32"
	case KindFloat64:
		return "float64"
	case KindDecimal:
		return "decimal"
	case KindText:
		return "text"
	case KindBytes:
		return "bytes"
	case KindUUID:
		return "uuid"
	case KindTimestamp:
		return "timestamp"
	case KindTime:
		return "time"
	case KindDate:
		return "date"
	case KindJSON:
		return "json"
	case KindNamed:
		return "named"
	}
	return "invalid"
}

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

func Named(name string) Type {
	return Type{Kind: KindNamed, Name: name}
}

func Parse(name string) Type {
	switch strings.ToLower(strings.TrimSpace(name)) {
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
	}
	return Named(strings.ToLower(strings.TrimSpace(name)))
}

func (t Type) Valid() bool {
	if t.Kind == KindInvalid {
		return false
	}
	if t.Kind == KindNamed {
		return t.Name != ""
	}
	return t.Name == ""
}

func (t Type) IsNamed() bool { return t.Kind == KindNamed }

func (t Type) String() string {
	if t.Kind == KindNamed && t.Name != "" {
		return t.Name
	}
	return t.Kind.String()
}
