// Package vector contains DripSQL's in-memory columnar execution primitives.
package vector

import (
	"fmt"
	"math"
	"slices"
	"unsafe"
)

// Kind identifies the physical type stored by a vector.
type Kind uint8

const (
	KindInt64 Kind = iota + 1
	KindFloat64
	KindString
)

func (k Kind) String() string {
	switch k {
	case KindInt64:
		return "int64"
	case KindFloat64:
		return "float64"
	case KindString:
		return "string"
	default:
		return fmt.Sprintf("kind(%d)", k)
	}
}

// Vector is implemented by typed column vectors.
type Vector interface {
	Kind() Kind
	Len() int
	Take(sel []uint32) (Vector, error)
}

// Int64 stores int64 values without per-row allocation or boxing.
type Int64 struct {
	Values []int64
}

// NewInt64 returns an int64 vector with its own backing storage.
func NewInt64(values []int64) Int64 {
	return Int64{Values: slices.Clone(values)}
}

// FromInt64 returns an int64 vector that owns values without copying them.
func FromInt64(values []int64) Int64 {
	return Int64{Values: values}
}

func (v Int64) Kind() Kind { return KindInt64 }

func (v Int64) Len() int { return len(v.Values) }

func (v Int64) Take(sel []uint32) (Vector, error) {
	if len(sel) == 0 {
		return Int64{}, nil
	}
	maxRow := slices.Max(sel)
	if uint64(maxRow) >= uint64(len(v.Values)) {
		return nil, fmt.Errorf("selection row %d out of range for int64 vector length %d", maxRow, len(v.Values))
	}

	out := make([]int64, len(sel))
	for i, row := range sel {
		out[i] = v.Values[row]
	}
	return Int64{Values: out}, nil
}

func (v Int64) MinMax() (min, max int64, ok bool) {
	if len(v.Values) == 0 {
		return 0, 0, false
	}

	min = v.Values[0]
	max = v.Values[0]
	for _, value := range v.Values[1:] {
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}
	return min, max, true
}

// Float64 stores float64 values without per-row allocation or boxing.
type Float64 struct {
	Values []float64
}

// NewFloat64 returns a float64 vector with its own backing storage.
func NewFloat64(values []float64) Float64 {
	return Float64{Values: slices.Clone(values)}
}

// FromFloat64 returns a float64 vector that owns values without copying them.
func FromFloat64(values []float64) Float64 {
	return Float64{Values: values}
}

func (v Float64) Kind() Kind { return KindFloat64 }

func (v Float64) Len() int { return len(v.Values) }

func (v Float64) Take(sel []uint32) (Vector, error) {
	if len(sel) == 0 {
		return Float64{}, nil
	}
	maxRow := slices.Max(sel)
	if uint64(maxRow) >= uint64(len(v.Values)) {
		return nil, fmt.Errorf("selection row %d out of range for float64 vector length %d", maxRow, len(v.Values))
	}

	out := make([]float64, len(sel))
	for i, row := range sel {
		out[i] = v.Values[row]
	}
	return Float64{Values: out}, nil
}

func (v Float64) MinMax() (min, max float64, ok bool) {
	if len(v.Values) == 0 || math.IsNaN(v.Values[0]) {
		return 0, 0, false
	}

	min = v.Values[0]
	max = v.Values[0]
	for _, value := range v.Values[1:] {
		if math.IsNaN(value) {
			return 0, 0, false
		}
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}
	return min, max, true
}

// String stores string values without boxing each row.
type String struct {
	Values []string
	Data   []byte
	Ranges []uint64
}

// NewString returns a string vector with its own backing storage.
func NewString(values []string) String {
	return String{Values: slices.Clone(values)}
}

// FromString returns a string vector that owns values without copying them.
func FromString(values []string) String {
	return String{Values: values}
}

// StringRange packs one string's start and length inside a backing byte slice.
func StringRange(start, length uint32) uint64 {
	return uint64(start)<<32 | uint64(length)
}

// FromStringData returns a string vector backed by immutable data and packed ranges.
// Callers must not mutate data while the returned vector exists.
func FromStringData(data []byte, ranges []uint64) String {
	return String{Data: data, Ranges: ranges}
}

func (v String) Kind() Kind { return KindString }

func (v String) Len() int {
	if v.Values != nil || v.Data == nil {
		return len(v.Values)
	}
	return len(v.Ranges)
}

func (v String) Value(row int) string {
	if v.Values != nil || v.Data == nil {
		return v.Values[row]
	}
	packed := v.Ranges[row]
	start := int(packed >> 32)
	length := int(uint32(packed))
	if length == 0 {
		return ""
	}
	data := v.Data[start : start+length]
	return unsafe.String(unsafe.SliceData(data), len(data))
}

func (v String) Take(sel []uint32) (Vector, error) {
	if len(sel) == 0 {
		return String{}, nil
	}
	maxRow := slices.Max(sel)
	if uint64(maxRow) >= uint64(v.Len()) {
		return nil, fmt.Errorf("selection row %d out of range for string vector length %d", maxRow, v.Len())
	}

	out := make([]string, len(sel))
	for i, row := range sel {
		out[i] = v.Value(int(row))
	}
	return String{Values: out}, nil
}

// Column names a typed vector inside a batch.
type Column struct {
	Name   string
	Vector Vector
}

// Batch is the unit passed between vectorized execution operators.
type Batch struct {
	Columns []Column
	Count   int
	Sel     []uint32
}

// VisibleCount returns the number of rows currently visible through the batch selection.
func (b Batch) VisibleCount() int {
	if b.Sel != nil {
		return len(b.Sel)
	}
	return b.Count
}

// HasSelection reports whether this batch is filtered through a selection vector.
func (b Batch) HasSelection() bool {
	return b.Sel != nil
}

// NewBatch validates column lengths and creates an execution batch.
func NewBatch(columns ...Column) (Batch, error) {
	if len(columns) == 0 {
		return Batch{}, nil
	}

	count := -1
	for _, col := range columns {
		if col.Name == "" {
			return Batch{}, fmt.Errorf("column name is required")
		}
		if col.Vector == nil {
			return Batch{}, fmt.Errorf("column %q has no vector", col.Name)
		}

		if count == -1 {
			count = col.Vector.Len()
			continue
		}
		if col.Vector.Len() != count {
			return Batch{}, fmt.Errorf("column %q length %d does not match batch length %d", col.Name, col.Vector.Len(), count)
		}
	}
	if len(columns) > 1 {
		for i := 0; i < len(columns); i++ {
			for j := i + 1; j < len(columns); j++ {
				if columns[i].Name == columns[j].Name {
					return Batch{}, fmt.Errorf("duplicate column %q", columns[i].Name)
				}
			}
		}
	}

	return Batch{Columns: slices.Clone(columns), Count: count}, nil
}

// Column returns a batch column by name.
func (b Batch) Column(name string) (Column, bool) {
	index, ok := b.ColumnIndex(name)
	if !ok {
		return Column{}, false
	}
	return b.Columns[index], true
}

// ColumnIndex returns a batch column index by name.
func (b Batch) ColumnIndex(name string) (int, bool) {
	for index, col := range b.Columns {
		if col.Name == name {
			return index, true
		}
	}
	return 0, false
}
