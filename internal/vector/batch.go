package vector

import (
	"fmt"
	"slices"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
)

type Column struct {
	Name       string
	Type       sqltype.Type
	EnumLabels []string
	V          Vec
}

type Batch struct {
	Columns []Column
	Len     int
	// Sel is public for hot-path readers; use SetSel when assigning a new selection.
	Sel Sel
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
		kind, err := KindOf(col.Type)
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

	return Batch{Columns: slices.Clone(normalized), Len: n}, nil
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
