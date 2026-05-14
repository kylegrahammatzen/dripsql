package exec

import (
	"context"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

// TextGroupCountSumState holds the per-group accumulator for a
// GROUP BY <text>, COUNT(*), SUM(int) aggregate.
type TextGroupCountSumState struct {
	Count int64
	Sum   int64
}

// TextGroupCountSumSink runs the dict-/flat-text fused GROUP BY pipeline.
// It is the specialized sink invoked by groupedTextCountSumBatch when the
// plan shape is one text group key and exactly one COUNT(*) + one SUM(int).
type TextGroupCountSumSink struct {
	Group  string
	SumCol string
	Groups map[string]TextGroupCountSumState
}

func (c *TextGroupCountSumSink) Open(context.Context) error { return nil }

func (c *TextGroupCountSumSink) Push(batch types.Batch, sel types.SelectionMask) error {
	if sel.Rows != batch.Len {
		return fmt.Errorf("selection rows %d do not match batch length %d", sel.Rows, batch.Len)
	}
	groupCol, ok := columnByName(batch, c.Group)
	if !ok {
		return fmt.Errorf("missing GROUP BY column %q", c.Group)
	}
	sumCol, ok := columnByName(batch, c.SumCol)
	if !ok {
		return fmt.Errorf("missing aggregate column %q", c.SumCol)
	}
	if groupCol.V.Kind != types.VecText {
		return fmt.Errorf("GROUP BY column %q has kind %s, want text", groupCol.Name, groupCol.V.Kind)
	}
	switch groupCol.V.Encoding {
	case types.EncodingDictionary:
		return c.pushDict(batch.Len, groupCol.V, sumCol.V, sel)
	case types.EncodingFlat:
		return c.pushFlat(batch.Len, groupCol.V, sumCol.V, sel)
	default:
		return fmt.Errorf("GROUP BY column %q has unsupported text encoding %s", groupCol.Name, groupCol.V.Encoding)
	}
}

func (c *TextGroupCountSumSink) Close() error { return nil }

// pushDict and pushFlat keep their loop bodies inline rather than sharing a
// helper. cmd/bench showed a closure-passed-across-a-function-boundary version
// regressed `country aggregate summary` by ~60% at 100M rows because the
// passed callback couldn't be inlined and forced the captured locals to
// escape. The duplication is the price of keeping the hot path tight.
func (c *TextGroupCountSumSink) pushDict(rows int, group types.Vec, sum types.Vec, sel types.SelectionMask) error {
	if group.Encoded == nil {
		return fmt.Errorf("dictionary group vector missing encoded state")
	}
	enc := group.Encoded
	if len(enc.DictIDs) < rows {
		return fmt.Errorf("dictionary ids length %d is shorter than rows %d", len(enc.DictIDs), rows)
	}
	if sum.Kind != types.VecInt32 && sum.Kind != types.VecInt64 {
		return fmt.Errorf("aggregate column %q has kind %s, want int32 or int64", c.SumCol, sum.Kind)
	}
	dictRows := enc.DictValues.Rows()
	if dictRows > 256 {
		return fmt.Errorf("dictionary group has %d values, max 256", dictRows)
	}
	var counts [256]int64
	var sums [256]int64

	fullPage := sel.Rows == 0 || sel.PopCount() == sel.Rows
	allValid := group.Valid == nil && sum.Valid == nil

	if fullPage && allValid {
		// Hot path for the bench workload: full page, no nulls.
		// The sum.Kind switch is hoisted out of the row loop and the
		// per-row dict-bounds check is replaced with one post-loop scan
		// over the [dictRows, 256) phantom range.
		ids := enc.DictIDs[:rows]
		switch sum.Kind {
		case types.VecInt64:
			src := sum.I64[:rows]
			for row := range rows {
				id := int(ids[row])
				counts[id]++
				next, ok := AddInt64(sums[id], src[row])
				if !ok {
					return ErrSumOverflow
				}
				sums[id] = next
			}
		case types.VecInt32:
			src := sum.I32[:rows]
			for row := range rows {
				id := int(ids[row])
				counts[id]++
				next, ok := AddInt64(sums[id], int64(src[row]))
				if !ok {
					return ErrSumOverflow
				}
				sums[id] = next
			}
		}
		for id := dictRows; id < 256; id++ {
			if counts[id] != 0 {
				return fmt.Errorf("dictionary id %d exceeds dictionary size %d", id, dictRows)
			}
		}
	} else {
		addRow := func(row int) error {
			if !types.IsValid(group.Valid, row) {
				return nil
			}
			id := int(enc.DictIDs[row])
			if id >= dictRows {
				return fmt.Errorf("dictionary id %d exceeds dictionary size %d", id, dictRows)
			}
			counts[id]++
			if !types.IsValid(sum.Valid, row) {
				return nil
			}
			var value int64
			if sum.Kind == types.VecInt32 {
				value = int64(sum.I32[row])
			} else {
				value = sum.I64[row]
			}
			next, ok := AddInt64(sums[id], value)
			if !ok {
				return ErrSumOverflow
			}
			sums[id] = next
			return nil
		}
		if fullPage {
			for row := range rows {
				if err := addRow(row); err != nil {
					return err
				}
			}
		} else {
			var pushErr error
			sel.IterSet(func(row int) {
				if pushErr != nil {
					return
				}
				pushErr = addRow(row)
			})
			if pushErr != nil {
				return pushErr
			}
		}
	}
	for id := range dictRows {
		if counts[id] == 0 {
			continue
		}
		key := enc.DictValues.String(id)
		state, ok := c.Groups[key]
		if !ok {
			key = enc.DictValues.StringCopy(id)
		}
		if err := c.mergeGroup(key, state, counts[id], sums[id]); err != nil {
			return err
		}
	}
	return nil
}

func (c *TextGroupCountSumSink) pushFlat(rows int, group types.Vec, sum types.Vec, sel types.SelectionMask) error {
	if sum.Kind != types.VecInt32 && sum.Kind != types.VecInt64 {
		return fmt.Errorf("aggregate column %q has kind %s, want int32 or int64", c.SumCol, sum.Kind)
	}
	addRow := func(row int) error {
		if !types.IsValid(group.Valid, row) {
			return nil
		}
		key := group.Var.String(row)
		state, ok := c.Groups[key]
		if !ok {
			key = group.Var.StringCopy(row)
		}
		var rowSum int64
		if types.IsValid(sum.Valid, row) {
			if sum.Kind == types.VecInt32 {
				rowSum = int64(sum.I32[row])
			} else {
				rowSum = sum.I64[row]
			}
		}
		return c.mergeGroup(key, state, 1, rowSum)
	}
	if sel.Rows == 0 || sel.PopCount() == sel.Rows {
		for row := range rows {
			if err := addRow(row); err != nil {
				return err
			}
		}
		return nil
	}
	var pushErr error
	sel.IterSet(func(row int) {
		if pushErr != nil {
			return
		}
		pushErr = addRow(row)
	})
	return pushErr
}

func (c *TextGroupCountSumSink) mergeGroup(key string, state TextGroupCountSumState, count int64, sum int64) error {
	next, ok := AddInt64(state.Sum, sum)
	if !ok {
		return ErrSumOverflow
	}
	state.Count += count
	state.Sum = next
	c.Groups[key] = state
	return nil
}
