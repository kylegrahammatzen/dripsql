// Predicate is a sealed tagged union of filter ops. Bind resolves columns and kinds,
// Eval fills a SelectionMask, PruneSegment reads stats to skip non-matching segments.
package storage

import (
	"fmt"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type Predicate interface {
	isPredicate()
}

type EqInt64 struct {
	Column string
	Value  int64
}
type LtInt64 struct {
	Column string
	Value  int64
}
type GtInt64 struct {
	Column string
	Value  int64
}
type EqBytes struct {
	Column string
	Value  []byte
}
type IsNull struct {
	Column string
}
type And struct {
	Children []Predicate
}
type Or struct {
	Children []Predicate
}
type Not struct {
	Child Predicate
}

func (EqInt64) isPredicate() {}
func (LtInt64) isPredicate() {}
func (GtInt64) isPredicate() {}
func (EqBytes) isPredicate() {}
func (IsNull) isPredicate()  {}
func (And) isPredicate()     {}
func (Or) isPredicate()      {}
func (Not) isPredicate()     {}

type BoundPredicate interface {
	Eval(batch types.Batch, sel *types.SelectionMask)
	PruneSegment(seg *Segment) bool
	PrunePage(seg *Segment, pageIdx int) bool
}

type SchemaLookup func(name string) (types.VecKind, bool)

func BatchSchema(batch types.Batch) SchemaLookup {
	return func(name string) (types.VecKind, bool) {
		c, ok := batch.ColumnByName(name)
		if !ok {
			return 0, false
		}
		return c.V.Kind, true
	}
}

func SegmentSchema(seg *Segment) SchemaLookup {
	return func(name string) (types.VecKind, bool) {
		for i := range seg.Cols {
			if strings.EqualFold(seg.Cols[i].Name, name) {
				return seg.Cols[i].Kind, true
			}
		}
		return 0, false
	}
}

func ColumnsSchema(cols []types.Column) SchemaLookup {
	return func(name string) (types.VecKind, bool) {
		for i := range cols {
			if strings.EqualFold(cols[i].Name, name) {
				k, err := types.VecKindOf(cols[i].Type)
				if err != nil {
					return 0, false
				}
				return k, true
			}
		}
		return 0, false
	}
}

func BindPredicate(p Predicate, lookup SchemaLookup) (BoundPredicate, error) {
	switch p := p.(type) {
	case EqInt64:
		if err := checkKind(lookup, p.Column, types.VecInt64); err != nil {
			return nil, err
		}
		return boundEqInt64{column: p.Column, value: p.Value}, nil
	case LtInt64:
		if err := checkKind(lookup, p.Column, types.VecInt64); err != nil {
			return nil, err
		}
		return boundLtInt64{column: p.Column, value: p.Value}, nil
	case GtInt64:
		if err := checkKind(lookup, p.Column, types.VecInt64); err != nil {
			return nil, err
		}
		return boundGtInt64{column: p.Column, value: p.Value}, nil
	case EqBytes:
		kind, ok := lookup(p.Column)
		if !ok {
			return nil, fmt.Errorf("column %q not in schema", p.Column)
		}
		if !kind.IsVarBytes() {
			return nil, fmt.Errorf("column %q kind %v is not varbytes", p.Column, kind)
		}
		clone := append([]byte(nil), p.Value...)
		return boundEqBytes{column: p.Column, value: clone}, nil
	case IsNull:
		if _, ok := lookup(p.Column); !ok {
			return nil, fmt.Errorf("column %q not in schema", p.Column)
		}
		return boundIsNull{column: p.Column}, nil
	case And:
		children, err := bindChildren(p.Children, lookup)
		if err != nil {
			return nil, err
		}
		return &boundAnd{children: children}, nil
	case Or:
		children, err := bindChildren(p.Children, lookup)
		if err != nil {
			return nil, err
		}
		return &boundOr{children: children}, nil
	case Not:
		child, err := BindPredicate(p.Child, lookup)
		if err != nil {
			return nil, err
		}
		return boundNot{child: child}, nil
	}
	return nil, fmt.Errorf("BindPredicate: unknown predicate %T", p)
}

// Returns column names in first-encountered order so scan's decode-set union is deterministic.
func PredicateColumns(p Predicate) []string {
	if p == nil {
		return nil
	}
	seen := map[string]struct{}{}
	var out []string
	var walk func(p Predicate)
	walk = func(p Predicate) {
		var name string
		switch p := p.(type) {
		case EqInt64:
			name = p.Column
		case LtInt64:
			name = p.Column
		case GtInt64:
			name = p.Column
		case EqBytes:
			name = p.Column
		case IsNull:
			name = p.Column
		case And:
			for _, c := range p.Children {
				walk(c)
			}
			return
		case Or:
			for _, c := range p.Children {
				walk(c)
			}
			return
		case Not:
			walk(p.Child)
			return
		default:
			return
		}
		key := strings.ToLower(name)
		if _, dup := seen[key]; dup {
			return
		}
		seen[key] = struct{}{}
		out = append(out, name)
	}
	walk(p)
	return out
}

func bindChildren(preds []Predicate, lookup SchemaLookup) ([]BoundPredicate, error) {
	out := make([]BoundPredicate, len(preds))
	for i, p := range preds {
		b, err := BindPredicate(p, lookup)
		if err != nil {
			return nil, fmt.Errorf("child %d: %w", i, err)
		}
		out[i] = b
	}
	return out, nil
}

func checkKind(lookup SchemaLookup, name string, want types.VecKind) error {
	kind, ok := lookup(name)
	if !ok {
		return fmt.Errorf("column %q not in schema", name)
	}
	if kind != want {
		return fmt.Errorf("column %q kind %v != predicate kind %v", name, kind, want)
	}
	return nil
}

func ensureMaskSize(sel *types.SelectionMask, rows int) {
	if sel.Rows() != rows {
		sel.Resize(rows)
		return
	}
	sel.Clear()
}

func findSegmentColumn(seg *Segment, name string) (*SegmentColumn, bool) {
	for i := range seg.Cols {
		if strings.EqualFold(seg.Cols[i].Name, name) {
			return &seg.Cols[i], true
		}
	}
	return nil, false
}

func numericStatsFromCol(c *SegmentColumn) (NumericStats[int64], bool) {
	if c.Rows == 0 {
		return NumericStats[int64]{}, false
	}
	return UnmarshalNumericStats[int64](c.Stats[:], true), true
}

// pageMinMaxInt returns the int64 min/max for a page when the column kind populates
// page stats with integral values. int16/int32/date are stored as NumericStats[int32]
// in 16B (4+4+pad), int64/timestamp/time/decimal64 as NumericStats[int64] (8+8).
// Reading the wrong shape would silently misinterpret bytes, so kind dispatch matters.
func pageMinMaxInt(c *SegmentColumn, pageIdx int) (min, max int64, ok bool) {
	if pageIdx < 0 || pageIdx >= len(c.PageStats) {
		return 0, 0, false
	}
	page := c.Pages[pageIdx]
	if page.Rows == 0 || page.Flags&PageFlagAllNull != 0 {
		return 0, 0, false
	}
	switch c.Kind {
	case types.VecInt16, types.VecInt32, types.VecDate:
		s := UnmarshalNumericStats[int32](c.PageStats[pageIdx][:], true)
		return int64(s.Min), int64(s.Max), true
	case types.VecInt64, types.VecTimestamp, types.VecTime, types.VecDecimal64:
		s := UnmarshalNumericStats[int64](c.PageStats[pageIdx][:], true)
		return s.Min, s.Max, true
	}
	return 0, 0, false
}

type boundEqInt64 struct {
	column string
	value  int64
}

func (b boundEqInt64) Eval(batch types.Batch, sel *types.SelectionMask) {
	ensureMaskSize(sel, batch.Len)
	col, ok := batch.ColumnByName(b.column)
	if !ok {
		return
	}
	sel.FillAll()
	types.FilterOrdered(col.V.I64(), col.V.Valid, b.value, types.FilterEqual, *sel, sel)
}

func (b boundEqInt64) PruneSegment(seg *Segment) bool {
	c, ok := findSegmentColumn(seg, b.column)
	if !ok {
		return false
	}
	stats, present := numericStatsFromCol(c)
	if !present {
		return true
	}
	if !stats.HasNonNull {
		return false
	}
	if b.value < stats.Min || b.value > stats.Max {
		return true
	}
	blooms, err := seg.IntBloomFilters()
	if err != nil {
		return false
	}
	if bloom, ok := blooms[types.NormalizeName(b.column)]; ok && !bloom.Contains(b.value) {
		return true
	}
	return false
}

func (b boundEqInt64) PrunePage(seg *Segment, pageIdx int) bool {
	c, ok := findSegmentColumn(seg, b.column)
	if !ok {
		return false
	}
	min, max, ok := pageMinMaxInt(c, pageIdx)
	if !ok {
		return false
	}
	return b.value < min || b.value > max
}

type boundLtInt64 struct {
	column string
	value  int64
}

func (b boundLtInt64) Eval(batch types.Batch, sel *types.SelectionMask) {
	ensureMaskSize(sel, batch.Len)
	col, ok := batch.ColumnByName(b.column)
	if !ok {
		return
	}
	sel.FillAll()
	types.FilterOrdered(col.V.I64(), col.V.Valid, b.value, types.FilterLess, *sel, sel)
}

func (b boundLtInt64) PruneSegment(seg *Segment) bool {
	c, ok := findSegmentColumn(seg, b.column)
	if !ok {
		return false
	}
	stats, present := numericStatsFromCol(c)
	if !present {
		return true
	}
	if !stats.HasNonNull {
		return false
	}
	return stats.Min >= b.value
}

func (b boundLtInt64) PrunePage(seg *Segment, pageIdx int) bool {
	c, ok := findSegmentColumn(seg, b.column)
	if !ok {
		return false
	}
	min, _, ok := pageMinMaxInt(c, pageIdx)
	if !ok {
		return false
	}
	return min >= b.value
}

type boundGtInt64 struct {
	column string
	value  int64
}

func (b boundGtInt64) Eval(batch types.Batch, sel *types.SelectionMask) {
	ensureMaskSize(sel, batch.Len)
	col, ok := batch.ColumnByName(b.column)
	if !ok {
		return
	}
	sel.FillAll()
	types.FilterOrdered(col.V.I64(), col.V.Valid, b.value, types.FilterGreater, *sel, sel)
}

func (b boundGtInt64) PruneSegment(seg *Segment) bool {
	c, ok := findSegmentColumn(seg, b.column)
	if !ok {
		return false
	}
	stats, present := numericStatsFromCol(c)
	if !present {
		return true
	}
	if !stats.HasNonNull {
		return false
	}
	return stats.Max <= b.value
}

func (b boundGtInt64) PrunePage(seg *Segment, pageIdx int) bool {
	c, ok := findSegmentColumn(seg, b.column)
	if !ok {
		return false
	}
	_, max, ok := pageMinMaxInt(c, pageIdx)
	if !ok {
		return false
	}
	return max <= b.value
}

type boundEqBytes struct {
	column string
	value  []byte
}

func (b boundEqBytes) Eval(batch types.Batch, sel *types.SelectionMask) {
	ensureMaskSize(sel, batch.Len)
	col, ok := batch.ColumnByName(b.column)
	if !ok {
		return
	}
	sel.FillAll()
	types.FilterBytes(col.V.Var(), col.V.Valid, b.value, types.FilterEqual, *sel, sel)
}

func (b boundEqBytes) PruneSegment(seg *Segment) bool {
	c, ok := findSegmentColumn(seg, b.column)
	if !ok {
		return false
	}
	return c.Rows == 0
}

func (b boundEqBytes) PrunePage(seg *Segment, pageIdx int) bool {
	c, ok := findSegmentColumn(seg, b.column)
	if !ok {
		return false
	}
	if pageIdx < 0 || pageIdx >= len(c.Pages) {
		return false
	}
	page := c.Pages[pageIdx]
	if page.Flags&PageFlagAllNull != 0 {
		return true
	}
	return false
}

type boundIsNull struct{ column string }

func (b boundIsNull) Eval(batch types.Batch, sel *types.SelectionMask) {
	ensureMaskSize(sel, batch.Len)
	col, ok := batch.ColumnByName(b.column)
	if !ok || col.V.Valid == nil {
		return
	}
	rows := int(col.V.Len)
	valid := col.V.Valid
	for i := range rows {
		if !valid.IsValid(i) {
			sel.Set(i)
		}
	}
}

func (b boundIsNull) PruneSegment(seg *Segment) bool {
	c, ok := findSegmentColumn(seg, b.column)
	if !ok {
		return false
	}
	return c.NullCount == 0
}

func (b boundIsNull) PrunePage(seg *Segment, pageIdx int) bool {
	c, ok := findSegmentColumn(seg, b.column)
	if !ok {
		return false
	}
	if pageIdx < 0 || pageIdx >= len(c.Pages) {
		return false
	}
	return c.Pages[pageIdx].NullCount == 0
}

type boundAnd struct {
	children []BoundPredicate
	tmp      types.SelectionMask
}

func (b *boundAnd) Eval(batch types.Batch, sel *types.SelectionMask) {
	if len(b.children) == 0 {
		ensureMaskSize(sel, batch.Len)
		sel.FillAll()
		return
	}
	b.children[0].Eval(batch, sel)
	for _, c := range b.children[1:] {
		c.Eval(batch, &b.tmp)
		sel.AndCount(b.tmp)
	}
}

func (b *boundAnd) PruneSegment(seg *Segment) bool {
	for _, c := range b.children {
		if c.PruneSegment(seg) {
			return true
		}
	}
	return false
}

func (b *boundAnd) PrunePage(seg *Segment, pageIdx int) bool {
	for _, c := range b.children {
		if c.PrunePage(seg, pageIdx) {
			return true
		}
	}
	return false
}

type boundOr struct {
	children []BoundPredicate
	tmp      types.SelectionMask
}

func (b *boundOr) Eval(batch types.Batch, sel *types.SelectionMask) {
	if len(b.children) == 0 {
		ensureMaskSize(sel, batch.Len)
		return
	}
	b.children[0].Eval(batch, sel)
	for _, c := range b.children[1:] {
		c.Eval(batch, &b.tmp)
		sel.OrCount(b.tmp)
	}
}

func (b boundOr) PruneSegment(seg *Segment) bool {
	for _, c := range b.children {
		if !c.PruneSegment(seg) {
			return false
		}
	}
	return true
}

func (b boundOr) PrunePage(seg *Segment, pageIdx int) bool {
	for _, c := range b.children {
		if !c.PrunePage(seg, pageIdx) {
			return false
		}
	}
	return true
}

type boundNot struct{ child BoundPredicate }

func (b boundNot) Eval(batch types.Batch, sel *types.SelectionMask) {
	b.child.Eval(batch, sel)
	sel.NotCount()
}

func (b boundNot) PruneSegment(seg *Segment) bool {
	// child Prune means no rows match it, which says nothing about NOT's matches.
	_ = seg
	return false
}

func (b boundNot) PrunePage(seg *Segment, pageIdx int) bool {
	_ = seg
	_ = pageIdx
	return false
}
