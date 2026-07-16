// Type is the SQL-side data type referenced by parser, binder, and exec.
// Logical-kind name, parse, and physical-kind mapping all run as single switches.
package schema

import (
	"fmt"
	"strings"
)

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

func (t Type) String() string {
	if t.Kind == KindNamed && t.Name != "" {
		return t.Name
	}
	return t.Kind.String()
}

// ParseKindStrict accepts only the canonical short name and rejects unknown values. The
// catalog loader uses it to fail fast on hand-edited or future-format files instead of
// silently downgrading to KindInvalid.
func ParseKindStrict(name string) (Kind, bool) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "bool":
		return KindBool, true
	case "int16":
		return KindInt16, true
	case "int32":
		return KindInt32, true
	case "int64":
		return KindInt64, true
	case "float32":
		return KindFloat32, true
	case "float64":
		return KindFloat64, true
	case "decimal":
		return KindDecimal, true
	case "text":
		return KindText, true
	case "bytes":
		return KindBytes, true
	case "uuid":
		return KindUUID, true
	case "timestamp":
		return KindTimestamp, true
	case "time":
		return KindTime, true
	case "date":
		return KindDate, true
	case "json":
		return KindJSON, true
	case "named":
		return KindNamed, true
	}
	return KindInvalid, false
}

// TypeString renders a Type into its canonical persisted form. Primitives serialize as
// the short kind name. Named (user-defined) types serialize as "named:<name>" so the
// "named:" prefix can never collide with a future primitive name.
func TypeString(t Type) (string, error) {
	if t.Kind == KindNamed {
		if t.Name == "" {
			return "", fmt.Errorf("schema: named type missing name")
		}
		return "named:" + t.Name, nil
	}
	if t.Kind == KindInvalid {
		return "", fmt.Errorf("schema: cannot serialize invalid type")
	}
	s := t.Kind.String()
	if s == "invalid" {
		return "", fmt.Errorf("schema: unknown kind %d", uint8(t.Kind))
	}
	if t.Name != "" {
		return "", fmt.Errorf("schema: non-named type %q must not carry a name", s)
	}
	return s, nil
}

// ParseType parses the canonical persisted form back into a Type, validating only the
// grammar. The caller resolves named types against the catalog's registered types.
func ParseType(s string) (Type, error) {
	trimmed := strings.ToLower(strings.TrimSpace(s))
	if trimmed == "" {
		return Type{}, fmt.Errorf("schema: empty type")
	}
	if strings.HasPrefix(trimmed, "named:") {
		name := strings.TrimSpace(trimmed[len("named:"):])
		if name == "" {
			return Type{}, fmt.Errorf("schema: named: requires a name")
		}
		return Named(name), nil
	}
	k, ok := ParseKindStrict(trimmed)
	if !ok {
		return Type{}, fmt.Errorf("schema: unknown type %q", s)
	}
	if k == KindNamed {
		return Type{}, fmt.Errorf("schema: named type must use the named:<name> form")
	}
	return Type{Kind: k}, nil
}
