// Batch is a row-aligned set of named typed columns.
// All columns share Len rows. Sel is an optional row-selection bitmap.
package vector

import (
	"fmt"
	"slices"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
)

type Column struct {
	Name       string
	Type       schema.Type
	EnumLabels []string
	V          Vec
}

type Batch struct {
	Len     int
	Columns []Column
	Sel     *SelectionMask
}

func NewBatch(cols []Column) (Batch, error) {
	if len(cols) == 0 {
		return Batch{}, nil
	}
	n := int(cols[0].V.Len)
	if n > StandardBatchRows {
		return Batch{}, fmt.Errorf("batch length %d exceeds %d", n, StandardBatchRows)
	}
	seen := make(map[string]struct{}, len(cols))
	out := make([]Column, len(cols))
	for i, col := range cols {
		name := strings.TrimSpace(col.Name)
		if name == "" {
			return Batch{}, fmt.Errorf("column name is required")
		}
		key := strings.ToLower(name)
		if _, dup := seen[key]; dup {
			return Batch{}, fmt.Errorf("duplicate column %q", name)
		}
		seen[key] = struct{}{}
		if int(col.V.Len) != n {
			return Batch{}, fmt.Errorf("column %q length %d does not match batch length %d", name, col.V.Len, n)
		}
		if err := col.V.Validate(); err != nil {
			return Batch{}, fmt.Errorf("column %q: %w", name, err)
		}
		vk, err := VecKindOf(col.Type)
		if err != nil {
			return Batch{}, fmt.Errorf("column %q: %w", name, err)
		}
		if col.V.Kind != vk {
			return Batch{}, fmt.Errorf("column %q vec kind %v does not match type %v", name, col.V.Kind, col.Type)
		}
		col.Name = name
		col.EnumLabels = slices.Clone(col.EnumLabels)
		out[i] = col
	}
	return Batch{Len: n, Columns: out}, nil
}

func (b Batch) VisibleLen() int {
	if b.Sel != nil {
		return b.Sel.PopCount()
	}
	return b.Len
}

func (b *Batch) SetSel(sel *SelectionMask) error {
	if sel == nil {
		b.Sel = nil
		return nil
	}
	if sel.Rows() != b.Len {
		return fmt.Errorf("selection rows %d does not match batch length %d", sel.Rows(), b.Len)
	}
	b.Sel = sel
	return nil
}

func (b Batch) ColumnByName(name string) (*Column, bool) {
	trimmed := strings.TrimSpace(name)
	for i := range b.Columns {
		if strings.EqualFold(b.Columns[i].Name, trimmed) {
			return &b.Columns[i], true
		}
	}
	return nil, false
}
