// Package vector contains DripSQL's in-memory columnar execution primitives.
package vector

import "fmt"

// Kind identifies the physical type stored by a vector.
type Kind uint8

const (
	KindInt64 Kind = iota + 1
	KindString
)

func (k Kind) String() string {
	switch k {
	case KindInt64:
		return "int64"
	case KindString:
		return "string"
	default:
		return fmt.Sprintf("kind(%d)", k)
	}
}

// Values is implemented by typed column vectors.
type Values interface {
	Len() int
	Kind() Kind
}

// Int64 stores int64 values without per-row interface allocation.
type Int64 struct {
	Values []int64
}

// NewInt64 returns an int64 vector with its own backing storage.
func NewInt64(values []int64) Int64 {
	return Int64{Values: append([]int64(nil), values...)}
}

func (v Int64) Len() int { return len(v.Values) }

func (v Int64) Kind() Kind { return KindInt64 }

func (v Int64) At(i int) int64 { return v.Values[i] }

func (v Int64) Take(sel []uint32) (Int64, error) {
	out := make([]int64, len(sel))
	for i, row := range sel {
		if int(row) >= len(v.Values) {
			return Int64{}, fmt.Errorf("selection row %d out of range for int64 vector length %d", row, len(v.Values))
		}
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

// String stores string values without boxing each row as an interface{}.
type String struct {
	Values []string
}

// NewString returns a string vector with its own backing storage.
func NewString(values []string) String {
	return String{Values: append([]string(nil), values...)}
}

func (v String) Len() int { return len(v.Values) }

func (v String) Kind() Kind { return KindString }

func (v String) At(i int) string { return v.Values[i] }

func (v String) Take(sel []uint32) (String, error) {
	out := make([]string, len(sel))
	for i, row := range sel {
		if int(row) >= len(v.Values) {
			return String{}, fmt.Errorf("selection row %d out of range for string vector length %d", row, len(v.Values))
		}
		out[i] = v.Values[row]
	}
	return String{Values: out}, nil
}

// Column names a typed vector inside a batch.
type Column struct {
	Name   string
	Values Values
}

// Batch is the unit passed between vectorized execution operators.
type Batch struct {
	Columns []Column
	Count   int
	Sel     []uint32
}

// NewBatch validates column lengths and creates an execution batch.
func NewBatch(columns ...Column) (Batch, error) {
	if len(columns) == 0 {
		return Batch{}, nil
	}

	count := -1
	seen := make(map[string]struct{}, len(columns))
	for _, col := range columns {
		if col.Name == "" {
			return Batch{}, fmt.Errorf("column name is required")
		}
		if _, ok := seen[col.Name]; ok {
			return Batch{}, fmt.Errorf("duplicate column %q", col.Name)
		}
		seen[col.Name] = struct{}{}

		if col.Values == nil {
			return Batch{}, fmt.Errorf("column %q has nil values", col.Name)
		}
		if count == -1 {
			count = col.Values.Len()
			continue
		}
		if col.Values.Len() != count {
			return Batch{}, fmt.Errorf("column %q length %d does not match batch length %d", col.Name, col.Values.Len(), count)
		}
	}

	return Batch{Columns: append([]Column(nil), columns...), Count: count}, nil
}

// Column returns a batch column by name.
func (b Batch) Column(name string) (Column, bool) {
	for _, col := range b.Columns {
		if col.Name == name {
			return col, true
		}
	}
	return Column{}, false
}
