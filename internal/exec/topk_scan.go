package exec

import (
	"context"
	"fmt"
	"math"
	"slices"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type TopKScanOp struct {
	Segments []*storage.Segment
	Columns  []string
	Kinds    []vector.VecKind
	Types    []schema.Type
	Labels   [][]string
	Key      string
	Desc     bool
	K        int64
	Offset   int64

	state operatorState
	built bool
	done  bool
	out   vector.Batch
}

type topKRowItem struct {
	key    int64
	isNull bool
	seq    uint32
	seg    uint32
	page   uint32
	row    uint32
}

type topKRowHeap struct {
	data []topKRowItem
	desc bool
}

func (t *TopKScanOp) Open(context.Context) error {
	if err := t.state.open(); err != nil {
		return err
	}
	t.built = false
	t.done = false
	t.out = vector.Batch{}
	return nil
}

func (t *TopKScanOp) Next() (vector.Batch, bool, error) {
	if err := t.state.requireOpen(); err != nil {
		return vector.Batch{}, false, err
	}
	if t.done {
		return vector.Batch{}, false, nil
	}
	if !t.built {
		batch, err := t.build()
		if err != nil {
			return vector.Batch{}, false, err
		}
		t.out = batch
		t.built = true
	}
	t.done = true
	return t.out, true, nil
}

func (t *TopKScanOp) Close() error {
	t.state.close()
	return nil
}

func (t *TopKScanOp) build() (vector.Batch, error) {
	capN, ok := t.heapCap()
	if !ok {
		return vector.Batch{}, fmt.Errorf("topk scan: invalid limit")
	}
	items, err := t.collect(capN)
	if err != nil {
		return vector.Batch{}, err
	}
	slices.SortFunc(items, func(a, b topKRowItem) int {
		return cmpInt64Sort(a.key, a.isNull, a.seq, b.key, b.isNull, b.seq, t.Desc)
	})
	start := min(int(t.Offset), len(items))
	end := min(start+int(t.K), len(items))
	return t.materialize(items[start:end])
}

func (t *TopKScanOp) heapCap() (int, bool) {
	if t.K <= 0 || t.Offset < 0 {
		return 0, false
	}
	want := t.K + t.Offset
	if want <= 0 || want > vector.StandardBatchRows || want > math.MaxInt32 {
		return 0, false
	}
	return int(want), true
}

func (t *TopKScanOp) collect(capN int) ([]topKRowItem, error) {
	data := make([]topKRowItem, 0, capN)
	h := topKRowHeap{data: data, desc: t.Desc}
	useInsert := capN <= topKInt64InsertCapMax
	pageMask := storage.SelectTopKPages(t.Segments, &storage.TopKPushdown{Column: t.Key, Desc: t.Desc, K: t.K, Offset: t.Offset})
	var seq uint32
	var scratch []byte
	var pageVec vector.Vec
	for si, seg := range t.Segments {
		keyIdx := findTopKColumn(seg, t.Key)
		if keyIdx < 0 {
			return nil, fmt.Errorf("topk scan: key column %q missing", t.Key)
		}
		noNullNoDV := seg.DV == nil && seg.Cols[keyIdx].NullCount == 0
		for pi, page := range seg.Cols[keyIdx].Pages {
			if pageMask != nil && !pageMask[[2]int{si, pi}] {
				continue
			}
			nextScratch, err := seg.ReadPageIntoVec(keyIdx, pi, scratch, &pageVec)
			scratch = nextScratch
			if err != nil {
				return nil, err
			}
			if noNullNoDV && page.NullCount == 0 {
				if vals, ok := topKInt64Values(pageVec); ok {
					base := seq
					seq += uint32(page.Rows)
					insert := func(key int64, row int) {
						item := topKRowItem{key: key, seq: base + uint32(row), seg: uint32(si), page: uint32(pi), row: uint32(row)}
						if useInsert {
							n := len(data)
							lo, hi := 0, n
							for lo < hi {
								mid := int(uint(lo+hi) >> 1)
								if topKNoNullBefore(item.key, item.seq, data[mid].key, data[mid].seq, t.Desc) {
									hi = mid
								} else {
									lo = mid + 1
								}
							}
							if n < capN {
								data = append(data, topKRowItem{})
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
					}
					// Rows arrive in seq order so every kept item precedes the current row and ties always lose.
					// That reduces rejection to one key compare against the cached worst, the hot case for K << rows.
					full := false
					var worst int64
					syncWorst := func() {
						if useInsert {
							if full = len(data) == capN; full {
								worst = data[capN-1].key
							}
							return
						}
						if full = len(h.data) == capN; full {
							worst = h.data[0].key
						}
					}
					syncWorst()
					vals = vals[:int(page.Rows)]
					if t.Desc {
						for row, key := range vals {
							if full && key <= worst {
								continue
							}
							insert(key, row)
							syncWorst()
						}
					} else {
						for row, key := range vals {
							if full && key >= worst {
								continue
							}
							insert(key, row)
							syncWorst()
						}
					}
					continue
				}
			}
			valid := pageVec.Valid
			for row := range int(page.Rows) {
				if seg.DV != nil && !seg.DV.IsValid(int(page.RowStart)+row) {
					continue
				}
				item := topKRowItem{seq: seq, seg: uint32(si), page: uint32(pi), row: uint32(row)}
				seq++
				if valid != nil && !valid.IsValid(row) {
					item.isNull = true
				} else {
					item.key = readInt64SortKey(pageVec, row)
				}
				if useInsert {
					n := len(data)
					if n == capN && cmpInt64Sort(item.key, item.isNull, item.seq, data[n-1].key, data[n-1].isNull, data[n-1].seq, t.Desc) >= 0 {
						continue
					}
					lo, hi := 0, n
					for lo < hi {
						mid := int(uint(lo+hi) >> 1)
						if cmpInt64Sort(item.key, item.isNull, item.seq, data[mid].key, data[mid].isNull, data[mid].seq, t.Desc) < 0 {
							hi = mid
						} else {
							lo = mid + 1
						}
					}
					if n < capN {
						data = append(data, topKRowItem{})
						copy(data[lo+1:], data[lo:n])
					} else {
						copy(data[lo+1:], data[lo:n-1])
					}
					data[lo] = item
					continue
				}
				if len(h.data) < capN {
					h.data = append(h.data, item)
					h.siftUp(len(h.data) - 1)
					continue
				}
				if h.beats(item, h.data[0]) {
					h.data[0] = item
					h.siftDown(0)
				}
			}
		}
	}
	if useInsert {
		return data, nil
	}
	return h.data, nil
}

func topKInt64Values(v vector.Vec) ([]int64, bool) {
	switch v.Kind {
	case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
		return v.I64(), true
	}
	return nil, false
}

func topKNoNullBefore(ak int64, ai uint32, bk int64, bi uint32, desc bool) bool {
	if ak != bk {
		if desc {
			return ak > bk
		}
		return ak < bk
	}
	return ai < bi
}

func (t *TopKScanOp) materialize(items []topKRowItem) (vector.Batch, error) {
	cols := make([]vector.Column, len(t.Columns))
	for ci, name := range t.Columns {
		kind := t.Kinds[ci]
		v, err := vector.NewVecForKind(kind, len(items))
		if err != nil {
			return vector.Batch{}, err
		}
		typ := schema.Type{}
		if ci < len(t.Types) {
			typ = t.Types[ci]
		}
		if !typ.Valid() {
			var err error
			typ, err = vector.TypeFromVecKind(kind, "")
			if err != nil {
				return vector.Batch{}, err
			}
		}
		cols[ci] = vector.Column{Name: name, Type: typ, V: v}
		if ci < len(t.Labels) {
			cols[ci].EnumLabels = t.Labels[ci]
		}
	}
	var scratch []byte
	for ci, name := range t.Columns {
		if sameFoldName(name, t.Key) {
			fillTopKKeyColumn(&cols[ci].V, items)
			continue
		}
		for outRow, item := range items {
			seg := t.Segments[item.seg]
			colIdx := findTopKColumn(seg, name)
			if colIdx < 0 {
				return vector.Batch{}, fmt.Errorf("topk scan: column %q missing", name)
			}
			pageVec, nextScratch, err := seg.ReadPageInto(colIdx, int(item.page), scratch)
			scratch = nextScratch
			if err != nil {
				return vector.Batch{}, err
			}
			if err := vector.CopyVecRow(pageVec, int(item.row), &cols[ci].V, outRow); err != nil {
				return vector.Batch{}, err
			}
		}
	}
	out, err := vector.NewBatch(cols)
	if err != nil {
		return vector.Batch{}, err
	}
	sel := vector.NewSelectionMask(out.Len)
	sel.FillAll()
	if err := out.SetSel(&sel); err != nil {
		return vector.Batch{}, err
	}
	return out, nil
}

func fillTopKKeyColumn(v *vector.Vec, items []topKRowItem) {
	validNeeded := false
	for _, item := range items {
		if item.isNull {
			validNeeded = true
			break
		}
	}
	if validNeeded {
		v.Valid = vector.NewAllValid(len(items))
	}
	for row, item := range items {
		if item.isNull {
			v.Valid.SetInvalid(row)
			continue
		}
		switch v.Kind {
		case vector.VecInt16:
			v.I16()[row] = int16(item.key)
		case vector.VecInt32, vector.VecDate:
			v.I32()[row] = int32(item.key)
		default:
			v.I64()[row] = item.key
		}
	}
}

func findTopKColumn(seg *storage.Segment, name string) int {
	return seg.ColumnIndex(name)
}

func (h *topKRowHeap) less(i, j int) bool {
	a, b := h.data[i], h.data[j]
	return cmpInt64Sort(a.key, a.isNull, a.seq, b.key, b.isNull, b.seq, h.desc) > 0
}

func (h *topKRowHeap) beats(c, root topKRowItem) bool {
	return cmpInt64Sort(c.key, c.isNull, c.seq, root.key, root.isNull, root.seq, h.desc) < 0
}

func (h *topKRowHeap) siftUp(start int) {
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

func (h *topKRowHeap) siftDown(start int) {
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
