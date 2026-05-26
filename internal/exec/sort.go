// SortOp buffers selected rows, sorts an index permutation by the bound keys, and emits
// chunks of StandardBatchRows. K > 0 with a single int-like column key takes the streaming
// top-K path: only K+Offset heap items live in sort metadata, though source batches are
// still retained for materialization so total memory remains O(input payload).
package exec

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type SortOp struct {
	Source Operator
	Keys   []sql.SortKey
	K      int64
	Offset int64

	state  operatorState
	built  bool
	cursor int

	bufs []bufferedBatch

	wideRefs []rowRef32
	refs     []rowRef16
	useWide  bool

	order []uint32

	keys  []int64
	nulls []uint64

	textVars []*vector.VarBytes
	textRows []uint32

	slowRows []sortRow
	slowKeys []any
}

type bufferedBatch struct {
	batch vector.Batch
	sel   vector.SelectionMask
}

type rowRef16 struct {
	bufIdx uint16
	row    uint16
}

type rowRef32 struct {
	bufIdx uint32
	row    uint32
}

type sortRow struct {
	bufIdx int
	row    int
	keyOff int
}

const topKInt64InsertCapMax = 256

// selectionForBatch returns a read-only mask view for iteration.
// nil Sel means all rows visible, so it materializes an all-set mask.
func selectionForBatch(batch vector.Batch) vector.SelectionMask {
	if batch.Sel != nil {
		return *batch.Sel
	}
	sel := vector.NewSelectionMask(batch.Len)
	sel.FillAll()
	return sel
}

func (s *SortOp) Open(ctx context.Context) error {
	prev := s.state
	if err := s.state.open(); err != nil {
		return err
	}
	if err := s.Source.Open(ctx); err != nil {
		s.state = prev
		return err
	}
	s.built = false
	s.cursor = 0
	s.bufs = nil
	s.refs = nil
	s.wideRefs = nil
	s.useWide = false
	s.order = nil
	s.keys = nil
	s.nulls = nil
	s.slowRows = nil
	s.slowKeys = nil
	s.textVars = nil
	s.textRows = nil
	return nil
}

func (s *SortOp) Next() (vector.Batch, bool, error) {
	if err := s.state.requireOpen(); err != nil {
		return vector.Batch{}, false, err
	}
	if !s.built {
		if err := s.build(); err != nil {
			return vector.Batch{}, false, err
		}
		s.built = true
	}
	if s.cursor >= len(s.order) {
		return vector.Batch{}, false, nil
	}
	end := min(s.cursor+vector.StandardBatchRows, len(s.order))
	batch, sel, err := s.materializeChunk(s.cursor, end)
	if err != nil {
		return vector.Batch{}, false, err
	}
	s.cursor = end
	if err := batch.SetSel(sel); err != nil {
		return vector.Batch{}, false, fmt.Errorf("sort: %w", err)
	}
	return batch, true, nil
}

func (s *SortOp) build() error {
	if s.K > 0 && s.canFastInt64() {
		if capN := s.heapCapUnknown(); capN > 0 {
			return s.buildTopKInt64Streaming(capN, s.Keys[0].Desc)
		}
	}
	for {
		batch, ok, err := s.Source.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		s.bufs = append(s.bufs, bufferedBatch{batch: batch, sel: selectionForBatch(batch)})
	}
	if s.canFastInt64() {
		return s.buildFastInt64()
	}
	if s.canFastText() {
		return s.buildFastText()
	}
	return s.buildSlow()
}

func (s *SortOp) buildSlow() error {
	total := s.countSelectedRows()
	s.slowRows = make([]sortRow, 0, total)
	s.slowKeys = make([]any, 0, total*len(s.Keys))
	for bi, bb := range s.bufs {
		ctx := newEvalCtx(bb.batch)
		var loopErr error
		bb.sel.IterSet(func(row int) {
			if loopErr != nil {
				return
			}
			keyOff := len(s.slowKeys)
			for _, key := range s.Keys {
				v, err := ctx.eval(key.Expr, row)
				if err != nil {
					loopErr = err
					return
				}
				s.slowKeys = append(s.slowKeys, v)
			}
			s.slowRows = append(s.slowRows, sortRow{bufIdx: bi, row: row, keyOff: keyOff})
		})
		if loopErr != nil {
			return loopErr
		}
	}

	s.order = make([]uint32, len(s.slowRows))
	for i := range s.order {
		s.order[i] = uint32(i)
	}
	var cmpErr error
	slices.SortFunc(s.order, func(a, b uint32) int {
		if cmpErr != nil {
			return 0
		}
		ra, rb := &s.slowRows[a], &s.slowRows[b]
		for ki, key := range s.Keys {
			c, err := compareNullable(s.slowKeys[ra.keyOff+ki], s.slowKeys[rb.keyOff+ki])
			if err != nil {
				cmpErr = err
				return 0
			}
			if c == 0 {
				continue
			}
			if key.Desc {
				return -c
			}
			return c
		}
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	})
	if cmpErr != nil {
		return cmpErr
	}
	s.applyLimitOffset()
	return nil
}

func (s *SortOp) canFastInt64() bool {
	if len(s.Keys) != 1 {
		return false
	}
	k := s.Keys[0].Expr
	if k.Op != sql.ExprColumn {
		return false
	}
	switch k.Type.Kind {
	case schema.KindInt16, schema.KindInt32, schema.KindInt64, schema.KindDate, schema.KindTimestamp:
		return true
	}
	return false
}

func (s *SortOp) buildFastInt64() error {
	total := s.countSelectedRows()
	if total == 0 {
		s.order = []uint32{}
		return nil
	}

	useWide := s.refsMustBeWide()
	s.useWide = useWide
	if useWide {
		s.wideRefs = make([]rowRef32, 0, total)
	} else {
		s.refs = make([]rowRef16, 0, total)
	}
	s.keys = make([]int64, total)
	s.nulls = make([]uint64, (total+63)/64)

	keyCol := s.Keys[0].Expr.Column

	idx := uint32(0)
	for bi, bb := range s.bufs {
		col, ok := bb.batch.ColumnByName(keyCol)
		if !ok {
			return fmt.Errorf("sort: column %q not in batch", keyCol)
		}
		valid := col.V.Valid
		bb.sel.IterSet(func(row int) {
			if useWide {
				s.wideRefs = append(s.wideRefs, rowRef32{bufIdx: uint32(bi), row: uint32(row)})
			} else {
				s.refs = append(s.refs, rowRef16{bufIdx: uint16(bi), row: uint16(row)})
			}
			if valid != nil && !valid.IsValid(row) {
				bitSet(s.nulls, idx)
			} else {
				s.keys[idx] = readInt64SortKey(col.V, row)
			}
			idx++
		})
	}

	desc := s.Keys[0].Desc
	s.fullSortFastInt64(int(idx), desc)
	return nil
}

func (s *SortOp) fullSortFastInt64(total int, desc bool) {
	s.order = make([]uint32, total)
	for i := range s.order {
		s.order[i] = uint32(i)
	}
	slices.SortFunc(s.order, func(a, b uint32) int {
		return cmpInt64Sort(s.keys[a], bitIsSet(s.nulls, a), a, s.keys[b], bitIsSet(s.nulls, b), b, desc)
	})
}

func (s *SortOp) canFastText() bool {
	if len(s.Keys) != 1 {
		return false
	}
	k := s.Keys[0].Expr
	if k.Op != sql.ExprColumn {
		return false
	}
	switch k.Type.Kind {
	case schema.KindText, schema.KindBytes, schema.KindJSON:
		return true
	}
	return false
}

// Comparator reads bytes live from each batch's VarBytes, so s.bufs must retain every
// source batch until the sort completes.
func (s *SortOp) buildFastText() error {
	total := s.countSelectedRows()
	if total == 0 {
		s.order = []uint32{}
		return nil
	}

	useWide := s.refsMustBeWide()
	s.useWide = useWide
	if useWide {
		s.wideRefs = make([]rowRef32, 0, total)
	} else {
		s.refs = make([]rowRef16, 0, total)
	}
	s.nulls = make([]uint64, (total+63)/64)
	s.textVars = make([]*vector.VarBytes, len(s.bufs))

	keyCol := s.Keys[0].Expr.Column
	idx := uint32(0)
	for bi, bb := range s.bufs {
		col, ok := bb.batch.ColumnByName(keyCol)
		if !ok {
			return fmt.Errorf("sort: column %q not in batch", keyCol)
		}
		s.textVars[bi] = col.V.Var()
		valid := col.V.Valid
		bb.sel.IterSet(func(row int) {
			if useWide {
				s.wideRefs = append(s.wideRefs, rowRef32{bufIdx: uint32(bi), row: uint32(row)})
			} else {
				s.refs = append(s.refs, rowRef16{bufIdx: uint16(bi), row: uint16(row)})
			}
			if valid != nil && !valid.IsValid(row) {
				bitSet(s.nulls, idx)
			}
			idx++
		})
	}

	desc := s.Keys[0].Desc
	s.order = make([]uint32, total)
	for i := range s.order {
		s.order[i] = uint32(i)
	}
	slices.SortFunc(s.order, func(a, b uint32) int {
		anull := bitIsSet(s.nulls, a)
		bnull := bitIsSet(s.nulls, b)
		if anull != bnull {
			c := 1
			if anull {
				c = -1
			}
			if desc {
				c = -c
			}
			return c
		}
		if !anull {
			abi, arow := s.resolveRef(a)
			bbi, brow := s.resolveRef(b)
			c := bytes.Compare(s.textVars[abi].Bytes(arow), s.textVars[bbi].Bytes(brow))
			if c != 0 {
				if desc {
					return -c
				}
				return c
			}
		}
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	})
	s.applyLimitOffset()
	return nil
}

func (s *SortOp) heapCapUnknown() int {
	if s.K <= 0 {
		return 0
	}
	want := s.Offset + s.K
	if want <= 0 || want > math.MaxInt32 {
		return 0
	}
	return int(want)
}

// buildTopKInt64Streaming reads source batches and pushes (key, null, seq, bufIdx, row)
// items into a bounded heap of capacity K+Offset. Memory cost is O(K+Offset) instead of
// O(total selected rows). Buffers are still retained for downstream materialization.
func (s *SortOp) buildTopKInt64Streaming(capN int, desc bool) error {
	keyCol := s.Keys[0].Expr.Column
	useInsert := capN <= topKInt64InsertCapMax
	data := make([]topKInt64Item, 0, capN)
	h := topKInt64Heap{data: data, desc: desc}
	var seq uint32

	for {
		batch, ok, err := s.Source.Next()
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		bi := uint32(len(s.bufs))
		s.bufs = append(s.bufs, bufferedBatch{batch: batch, sel: selectionForBatch(batch)})
		bb := &s.bufs[bi]
		col, ok := batch.ColumnByName(keyCol)
		if !ok {
			return fmt.Errorf("sort: column %q not in batch", keyCol)
		}
		valid := col.V.Valid
		bb.sel.IterSet(func(row int) {
			item := topKInt64Item{seq: seq, bufIdx: bi, row: uint32(row)}
			seq++
			if valid != nil && !valid.IsValid(row) {
				item.isNull = true
			} else {
				item.key = readInt64SortKey(col.V, row)
			}
			if useInsert {
				n := len(data)
				if n == capN && cmpInt64Sort(item.key, item.isNull, item.seq, data[n-1].key, data[n-1].isNull, data[n-1].seq, desc) >= 0 {
					return
				}
				lo, hi := 0, n
				for lo < hi {
					mid := int(uint(lo+hi) >> 1)
					if cmpInt64Sort(item.key, item.isNull, item.seq, data[mid].key, data[mid].isNull, data[mid].seq, desc) < 0 {
						hi = mid
					} else {
						lo = mid + 1
					}
				}
				if n < capN {
					data = append(data, topKInt64Item{})
					copy(data[lo+1:], data[lo:n])
				} else {
					copy(data[lo+1:], data[lo:n-1])
				}
				data[lo] = item
				return
			}
			if len(h.data) < capN {
				h.data = append(h.data, item)
				h.siftUp(len(h.data) - 1)
				return
			}
			if h.beats(item, h.data[0]) {
				h.data[0] = item
				h.siftDown(0)
			}
		})
	}

	if !useInsert {
		data = h.data
		slices.SortFunc(data, func(a, b topKInt64Item) int {
			return cmpInt64Sort(a.key, a.isNull, a.seq, b.key, b.isNull, b.seq, desc)
		})
	}
	start := int(s.Offset)
	if start > len(data) {
		start = len(data)
	}
	out := data[start:]
	s.useWide = true
	s.wideRefs = make([]rowRef32, len(out))
	for i, item := range out {
		s.wideRefs[i] = rowRef32{bufIdx: item.bufIdx, row: item.row}
	}
	s.order = make([]uint32, len(out))
	for i := range s.order {
		s.order[i] = uint32(i)
	}
	return nil
}

func (s *SortOp) applyLimitOffset() {
	if s.K <= 0 {
		return
	}
	n := len(s.order)
	start := 0
	if s.Offset > 0 {
		if s.Offset >= int64(n) {
			s.order = s.order[:0]
			return
		}
		start = int(s.Offset)
	}
	take := min(s.K, int64(n-start))
	s.order = s.order[start : start+int(take)]
}

func (s *SortOp) countSelectedRows() int {
	total := 0
	for _, bb := range s.bufs {
		total += bb.sel.PopCount()
	}
	return total
}

func (s *SortOp) refsMustBeWide() bool {
	if len(s.bufs) > math.MaxUint16 {
		return true
	}
	for _, bb := range s.bufs {
		if bb.batch.Len > math.MaxUint16 {
			return true
		}
	}
	return false
}

func (s *SortOp) materializeChunk(start, end int) (vector.Batch, *vector.SelectionMask, error) {
	if len(s.bufs) == 0 {
		empty := vector.NewSelectionMask(0)
		return vector.Batch{Len: 0}, &empty, nil
	}
	count := end - start
	tmpl := s.bufs[0].batch
	cols := make([]vector.Column, len(tmpl.Columns))
	for i, c := range tmpl.Columns {
		v, err := vector.NewVecForKind(c.V.Kind, count)
		if err != nil {
			return vector.Batch{}, nil, err
		}
		cols[i] = vector.Column{Name: c.Name, Type: c.Type, EnumLabels: c.EnumLabels, V: v}
	}

	for off := range count {
		permIdx := s.order[start+off]
		bufIdx, srcRow := s.resolveRef(permIdx)
		srcBatch := s.bufs[bufIdx].batch
		for ci, sc := range srcBatch.Columns {
			if err := vector.CopyVecRow(sc.V, srcRow, &cols[ci].V, off); err != nil {
				return vector.Batch{}, nil, err
			}
		}
	}

	out, err := vector.NewBatch(cols)
	if err != nil {
		return vector.Batch{}, nil, fmt.Errorf("sort: NewBatch: %w", err)
	}
	sel := vector.NewSelectionMask(count)
	sel.FillAll()
	return out, &sel, nil
}

func (s *SortOp) resolveRef(permIdx uint32) (int, int) {
	if len(s.slowRows) > 0 {
		r := s.slowRows[permIdx]
		return r.bufIdx, r.row
	}
	if s.useWide {
		r := s.wideRefs[permIdx]
		return int(r.bufIdx), int(r.row)
	}
	r := s.refs[permIdx]
	return int(r.bufIdx), int(r.row)
}

func readInt64SortKey(v vector.Vec, row int) int64 {
	switch v.Kind {
	case vector.VecInt16:
		return int64(v.I16()[row])
	case vector.VecInt32, vector.VecDate:
		return int64(v.I32()[row])
	case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
		return v.I64()[row]
	}
	return 0
}

func bitIsSet(bits []uint64, i uint32) bool {
	return bits[i>>6]&(uint64(1)<<uint(i&63)) != 0
}

func bitSet(bits []uint64, i uint32) {
	bits[i>>6] |= uint64(1) << uint(i&63)
}

// cmpInt64Sort returns < 0 if (key, null, seq) a sorts before b under SQL semantics.
// ASC: nulls first. DESC: nulls last. Tie-break on seq for stability under slices.SortFunc.
// Used by every int-key path (full sort, streaming top-K heap, final top-K sort).
func cmpInt64Sort(ak int64, anull bool, ai uint32, bk int64, bnull bool, bi uint32, desc bool) int {
	if anull != bnull {
		c := 1
		if anull {
			c = -1
		}
		if desc {
			c = -c
		}
		return c
	}
	if !anull {
		if ak < bk {
			if desc {
				return 1
			}
			return -1
		}
		if ak > bk {
			if desc {
				return -1
			}
			return 1
		}
	}
	if ai < bi {
		return -1
	}
	if ai > bi {
		return 1
	}
	return 0
}

type topKInt64Item struct {
	key    int64
	isNull bool
	seq    uint32
	bufIdx uint32
	row    uint32
}

type topKInt64Heap struct {
	data []topKInt64Item
	desc bool
}

// less ranks data[i] as weaker (more eligible for eviction) than data[j]. An item is
// weaker if it would sort LATER in the final SQL ordering, so the root is the next
// candidate to evict when a stronger one arrives.
func (h *topKInt64Heap) less(i, j int) bool {
	a, b := h.data[i], h.data[j]
	return cmpInt64Sort(a.key, a.isNull, a.seq, b.key, b.isNull, b.seq, h.desc) > 0
}

// beats reports whether candidate c would sort BEFORE the current heap root in the
// final SQL order, i.e. whether it deserves to replace the root.
func (h *topKInt64Heap) beats(c, root topKInt64Item) bool {
	return cmpInt64Sort(c.key, c.isNull, c.seq, root.key, root.isNull, root.seq, h.desc) < 0
}

func (h *topKInt64Heap) siftUp(start int) {
	i := start
	for i > 0 {
		parent := (i - 1) / 2
		if !h.less(i, parent) {
			return
		}
		h.data[i], h.data[parent] = h.data[parent], h.data[i]
		i = parent
	}
}

func (h *topKInt64Heap) siftDown(start int) {
	n := len(h.data)
	i := start
	for {
		l := 2*i + 1
		if l >= n {
			return
		}
		j := l
		if r := l + 1; r < n && h.less(r, l) {
			j = r
		}
		if !h.less(j, i) {
			return
		}
		h.data[i], h.data[j] = h.data[j], h.data[i]
		i = j
	}
}

func compareNullable(a, b any) (int, error) {
	if a == nil && b == nil {
		return 0, nil
	}
	if a == nil {
		return -1, nil
	}
	if b == nil {
		return 1, nil
	}
	return orderingCompare(a, b)
}

func (s *SortOp) Close() error {
	s.state.close()
	return s.Source.Close()
}
