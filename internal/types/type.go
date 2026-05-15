// Type is the SQL-side data type referenced by parser, binder, and exec.
// One sqlKindTable drives Parse, String, and the VecKindOf mapping to physical kinds.
package types

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

type sqlKindInfo struct {
	name    string
	vecKind VecKind
}

var sqlKindTable = [...]sqlKindInfo{
	KindInvalid:   {name: "invalid"},
	KindBool:      {name: "bool", vecKind: VecBool},
	KindInt16:     {name: "int16", vecKind: VecInt16},
	KindInt32:     {name: "int32", vecKind: VecInt32},
	KindInt64:     {name: "int64", vecKind: VecInt64},
	KindFloat32:   {name: "float32", vecKind: VecFloat32},
	KindFloat64:   {name: "float64", vecKind: VecFloat64},
	KindDecimal:   {name: "decimal", vecKind: VecDecimal64},
	KindText:      {name: "text", vecKind: VecText},
	KindBytes:     {name: "bytes", vecKind: VecBytes},
	KindUUID:      {name: "uuid", vecKind: VecUUID},
	KindTimestamp: {name: "timestamp", vecKind: VecTimestamp},
	KindTime:      {name: "time", vecKind: VecTime},
	KindDate:      {name: "date", vecKind: VecDate},
	KindJSON:      {name: "json", vecKind: VecJSON},
	KindNamed:     {name: "named", vecKind: VecEnum32},
}

func (k Kind) String() string {
	if int(k) < len(sqlKindTable) {
		if n := sqlKindTable[k].name; n != "" {
			return n
		}
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

var sqlNameToKind = func() map[string]Kind {
	m := make(map[string]Kind, len(sqlKindTable))
	for i, info := range sqlKindTable {
		k := Kind(i)
		if k == KindInvalid || k == KindNamed || info.name == "" {
			continue
		}
		m[info.name] = k
	}
	return m
}()

func Parse(name string) Type {
	canon := strings.ToLower(strings.TrimSpace(name))
	if k, ok := sqlNameToKind[canon]; ok {
		return Type{Kind: k}
	}
	return Named(canon)
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

// Inverse of VecKindOf, used when reconstructing logical Type from segment metadata.
// VecEnum32 maps to a Named type carrying enumName; other kinds ignore it.
func TypeFromVecKind(k VecKind, enumName string) (Type, error) {
	switch k {
	case VecBool:
		return Bool, nil
	case VecInt16:
		return Int16, nil
	case VecInt32:
		return Int32, nil
	case VecInt64:
		return Int64, nil
	case VecFloat32:
		return Float32, nil
	case VecFloat64:
		return Float64, nil
	case VecDecimal64:
		return Decimal, nil
	case VecText:
		return Text, nil
	case VecBytes:
		return Bytes, nil
	case VecUUID:
		return UUID, nil
	case VecTimestamp:
		return Timestamp, nil
	case VecTime:
		return Time, nil
	case VecDate:
		return Date, nil
	case VecJSON:
		return JSON, nil
	case VecEnum32:
		if enumName == "" {
			return Type{}, fmt.Errorf("TypeFromVecKind: enum without name")
		}
		return Named(enumName), nil
	}
	return Type{}, fmt.Errorf("TypeFromVecKind: unknown VecKind %v", k)
}

func VecKindOf(t Type) (VecKind, error) {
	if !t.Valid() {
		return VecInvalid, fmt.Errorf("VecKindOf: invalid type %v", t)
	}
	if int(t.Kind) >= len(sqlKindTable) {
		return VecInvalid, fmt.Errorf("VecKindOf: unknown kind %d", t.Kind)
	}
	vk := sqlKindTable[t.Kind].vecKind
	if vk == VecInvalid {
		return VecInvalid, fmt.Errorf("VecKindOf: kind %v has no physical mapping", t.Kind)
	}
	return vk, nil
}
