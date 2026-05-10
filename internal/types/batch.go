package types

import (
	"fmt"
	"slices"
	"strings"
)

type Column struct {
	Name       string
	Type       Type
	EnumLabels []string
	V          Vec
}

type Batch struct {
	Columns []Column
	Len     int
	Sel     Sel
}

func NewBatch(cols []Column) (Batch, error) {
	if len(cols) == 0 {
		return Batch{}, nil
	}
	n := cols[0].V.Len
	if n > StandardBatchRows {
		return Batch{}, fmt.Errorf("batch length %d exceeds %d", n, StandardBatchRows)
	}
	seen := make(map[string]struct{}, len(cols))
	normalized := make([]Column, len(cols))
	for i, col := range cols {
		name := strings.TrimSpace(col.Name)
		if name == "" {
			return Batch{}, fmt.Errorf("column name is required")
		}
		if _, ok := seen[name]; ok {
			return Batch{}, fmt.Errorf("duplicate column %q", name)
		}
		seen[name] = struct{}{}

		if col.V.Len != n {
			return Batch{}, fmt.Errorf("column %q length %d does not match batch length %d", name, col.V.Len, n)
		}
		if err := col.V.validate(); err != nil {
			return Batch{}, fmt.Errorf("column %q: %w", name, err)
		}
		kind, err := VecKindOf(col.Type)
		if err != nil {
			return Batch{}, fmt.Errorf("column %q: %w", name, err)
		}
		if col.V.Kind != kind {
			return Batch{}, fmt.Errorf("column %q vector kind %s does not match type %s", name, col.V.Kind, col.Type)
		}
		col.Name = name
		col.EnumLabels = slices.Clone(col.EnumLabels)
		normalized[i] = col
	}
	return Batch{Columns: normalized, Len: n}, nil
}

// NewBatchNoClone validates cols and returns a batch that directly owns the
// provided column slice. It is intended for trusted internal paths that have
// already built fresh column descriptors for a batch.
func NewBatchNoClone(cols []Column) (Batch, error) {
	if len(cols) == 0 {
		return Batch{}, nil
	}
	n := cols[0].V.Len
	if n > StandardBatchRows {
		return Batch{}, fmt.Errorf("batch length %d exceeds %d", n, StandardBatchRows)
	}
	for i := range cols {
		name := strings.TrimSpace(cols[i].Name)
		if name == "" {
			return Batch{}, fmt.Errorf("column name is required")
		}
		for j := 0; j < i; j++ {
			if cols[j].Name == name {
				return Batch{}, fmt.Errorf("duplicate column %q", name)
			}
		}
		if cols[i].V.Len != n {
			return Batch{}, fmt.Errorf("column %q length %d does not match batch length %d", name, cols[i].V.Len, n)
		}
		if err := cols[i].V.validate(); err != nil {
			return Batch{}, fmt.Errorf("column %q: %w", name, err)
		}
		kind, err := VecKindOf(cols[i].Type)
		if err != nil {
			return Batch{}, fmt.Errorf("column %q: %w", name, err)
		}
		if cols[i].V.Kind != kind {
			return Batch{}, fmt.Errorf("column %q vector kind %s does not match type %s", name, cols[i].V.Kind, cols[i].Type)
		}
		cols[i].Name = name
	}
	return Batch{Columns: cols, Len: n}, nil
}

func (b Batch) VisibleLen() int {
	if b.Sel != nil {
		return len(b.Sel)
	}
	return b.Len
}

func (b *Batch) SetSel(sel Sel) error {
	if err := ValidateSel(sel, b.Len); err != nil {
		return err
	}
	b.Sel = sel
	return nil
}

func ValidateSel(sel Sel, n int) error {
	for _, row := range sel {
		if int(row) >= n {
			return fmt.Errorf("selection row %d exceeds batch length %d", row, n)
		}
	}
	return nil
}
