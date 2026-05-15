// VecKind tags every Vec with the physical kind of data it holds.
// One metadata table drives name, fixed-width discriminator, and FOR-packable bit.
package types

import "fmt"

const StandardBatchRows = 2048

type VecKind uint8

const (
	VecInvalid VecKind = iota
	VecBool
	VecInt16
	VecInt32
	VecInt64
	VecFloat32
	VecFloat64
	VecDecimal64
	VecText
	VecBytes
	VecUUID
	VecTimestamp
	VecTime
	VecDate
	VecJSON
	VecEnum32
)

type Width int

const (
	WidthVarBytes Width = -1
	WidthBool     Width = -2
)

type vecKindInfo struct {
	name        string
	width       Width
	forPackable bool
}

var vecKindTable = [...]vecKindInfo{
	VecInvalid:   {name: "invalid"},
	VecBool:      {name: "bool", width: WidthBool},
	VecInt16:     {name: "int16", width: 2, forPackable: true},
	VecInt32:     {name: "int32", width: 4, forPackable: true},
	VecInt64:     {name: "int64", width: 8, forPackable: true},
	VecFloat32:   {name: "float32", width: 4},
	VecFloat64:   {name: "float64", width: 8},
	VecDecimal64: {name: "decimal64", width: 8, forPackable: true},
	VecText:      {name: "text", width: WidthVarBytes},
	VecBytes:     {name: "bytes", width: WidthVarBytes},
	VecUUID:      {name: "uuid", width: 16},
	VecTimestamp: {name: "timestamp", width: 8, forPackable: true},
	VecTime:      {name: "time", width: 8, forPackable: true},
	VecDate:      {name: "date", width: 4, forPackable: true},
	VecJSON:      {name: "json", width: WidthVarBytes},
	VecEnum32:    {name: "enum32", width: 4, forPackable: true},
}

func (k VecKind) String() string {
	if int(k) < len(vecKindTable) {
		if name := vecKindTable[k].name; name != "" {
			return name
		}
	}
	return fmt.Sprintf("vec_kind(%d)", k)
}

func (k VecKind) FixedWidth() Width {
	if int(k) < len(vecKindTable) {
		return vecKindTable[k].width
	}
	return 0
}

func (k VecKind) IsVarBytes() bool {
	return k.FixedWidth() == WidthVarBytes
}

func (k VecKind) IsFORPackable() bool {
	return int(k) < len(vecKindTable) && vecKindTable[k].forPackable
}
