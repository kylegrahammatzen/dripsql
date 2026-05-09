// Package vector contains DripSQL's dependency-free columnar execution buffers.
package vector

import "fmt"

const (
	// StandardBatchRows keeps row IDs compact enough for uint16 selection vectors.
	StandardBatchRows = 2048
	BatchSize         = StandardBatchRows
)

type Row uint16
type Sel []Row

type Kind uint8

const (
	Invalid Kind = iota
	Bool
	Int16
	Int32
	Int64
	Float32
	Float64
	Decimal64
	Text
	Bytes
	UUID
	Timestamp
	Time
	Date
	JSON
	Enum32
)

type UUID16 [16]byte

func (k Kind) String() string {
	if int(k) < len(kindNames) {
		if name := kindNames[k]; name != "" {
			return name
		}
	}
	return fmt.Sprintf("kind(%d)", k)
}

var kindNames = [...]string{
	Invalid:   "invalid",
	Bool:      "bool",
	Int16:     "int16",
	Int32:     "int32",
	Int64:     "int64",
	Float32:   "float32",
	Float64:   "float64",
	Decimal64: "decimal64",
	Text:      "text",
	Bytes:     "bytes",
	UUID:      "uuid",
	Timestamp: "timestamp",
	Time:      "time",
	Date:      "date",
	JSON:      "json",
	Enum32:    "enum32",
}
