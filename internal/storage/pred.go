// Pred is the storage-facing predicate IR with typed literal slots so the binder avoids any-boxing.
// BindPred returns a fresh tree so cached plans are never mutated by re-binding parameters.
package storage

import (
	"fmt"
	"math"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

type PredOp uint8

const (
	OpInvalid PredOp = iota
	OpEq
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
	OpIsNull
	OpIn
	OpAnd
	OpOr
	OpNot
)

func (op PredOp) String() string {
	switch op {
	case OpEq:
		return "="
	case OpNe:
		return "!="
	case OpLt:
		return "<"
	case OpLe:
		return "<="
	case OpGt:
		return ">"
	case OpGe:
		return ">="
	case OpIsNull:
		return "IS NULL"
	case OpIn:
		return "IN"
	case OpAnd:
		return "AND"
	case OpOr:
		return "OR"
	case OpNot:
		return "NOT"
	}
	return "invalid"
}

// Pred is the bound predicate tree; exactly one typed slot is live per leaf based on Kind.
type Pred struct {
	Op   PredOp
	Col  string
	// ColID is set by the engine so renames resolve by id when the segment carries identity.
	ColID uint64
	Kind  vector.VecKind

	I64   int64
	F64   float64
	Bytes []byte

	Children []Pred
	Set      []Pred
}

// BindPred resolves Col against the schema and returns a fresh Pred so cached plans are never mutated.
func BindPred(raw Pred, lookup SchemaLookup) (Pred, error) {
	switch raw.Op {
	case OpAnd, OpOr:
		if len(raw.Children) < 2 {
			return Pred{}, fmt.Errorf("Pred %v requires at least two children", raw.Op)
		}
		out := Pred{Op: raw.Op, Children: make([]Pred, len(raw.Children))}
		for i, c := range raw.Children {
			bound, err := BindPred(c, lookup)
			if err != nil {
				return Pred{}, err
			}
			out.Children[i] = bound
		}
		return out, nil
	case OpNot:
		if len(raw.Children) != 1 {
			return Pred{}, fmt.Errorf("Pred NOT requires exactly one child")
		}
		inner, err := BindPred(raw.Children[0], lookup)
		if err != nil {
			return Pred{}, err
		}
		return Pred{Op: OpNot, Children: []Pred{inner}}, nil
	case OpIsNull:
		k, ok := lookup(raw.Col)
		if !ok {
			return Pred{}, fmt.Errorf("column %q not in schema", raw.Col)
		}
		return Pred{Op: OpIsNull, Col: raw.Col, Kind: k}, nil
	case OpIn:
		if len(raw.Set) == 0 {
			return Pred{}, fmt.Errorf("Pred IN requires a non-empty set")
		}
		out := Pred{Op: OpIn, Col: raw.Col, Set: make([]Pred, len(raw.Set))}
		for i, e := range raw.Set {
			bound, err := BindPred(e, lookup)
			if err != nil {
				return Pred{}, err
			}
			out.Set[i] = bound
		}
		return out, nil
	case OpEq, OpNe, OpLt, OpLe, OpGt, OpGe:
		k, ok := lookup(raw.Col)
		if !ok {
			return Pred{}, fmt.Errorf("column %q not in schema", raw.Col)
		}
		return Pred{Op: raw.Op, Col: raw.Col, Kind: k, I64: raw.I64, F64: raw.F64, Bytes: cloneBytes(raw.Bytes)}, nil
	}
	return Pred{}, fmt.Errorf("BindPred: unknown op %v", raw.Op)
}

// Columns returns the column names referenced by p in first-encountered order.
func (p Pred) Columns() []string {
	names, _ := p.columnRefs()
	return names
}

// Returned ids are parallel to Columns, zero for leaves built without an id.
func (p Pred) ColumnIDs() []uint64 {
	_, ids := p.columnRefs()
	return ids
}

func (p Pred) columnRefs() ([]string, []uint64) {
	seen := map[string]struct{}{}
	var names []string
	var ids []uint64
	var walk func(p Pred)
	walk = func(p Pred) {
		switch p.Op {
		case OpAnd, OpOr:
			for _, c := range p.Children {
				walk(c)
			}
			return
		case OpNot:
			if len(p.Children) == 1 {
				walk(p.Children[0])
			}
			return
		case OpIn:
			for _, c := range p.Set {
				walk(c)
			}
			return
		}
		if p.Col == "" {
			return
		}
		key := schema.NormalizeName(p.Col)
		if _, dup := seen[key]; dup {
			return
		}
		seen[key] = struct{}{}
		names = append(names, p.Col)
		ids = append(ids, p.ColID)
	}
	walk(p)
	return names, ids
}

// validatePred asserts the per-op shape invariants for tests and transitional checks.
func validatePred(p Pred) error {
	switch p.Op {
	case OpEq, OpNe, OpLt, OpLe, OpGt, OpGe:
		if p.Col == "" {
			return fmt.Errorf("leaf op %v missing Col", p.Op)
		}
		if len(p.Children) != 0 || len(p.Set) != 0 {
			return fmt.Errorf("leaf op %v must have no Children or Set", p.Op)
		}
		return nil
	case OpIsNull:
		if p.Col == "" {
			return fmt.Errorf("IS NULL missing Col")
		}
		return nil
	case OpIn:
		if p.Col == "" || len(p.Set) == 0 {
			return fmt.Errorf("IN requires Col and non-empty Set")
		}
		return nil
	case OpNot:
		if len(p.Children) != 1 {
			return fmt.Errorf("NOT requires exactly one child")
		}
		return validatePred(p.Children[0])
	case OpAnd, OpOr:
		if len(p.Children) < 2 {
			return fmt.Errorf("%v requires at least two children", p.Op)
		}
		for _, c := range p.Children {
			if err := validatePred(c); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("validatePred: unknown op %v", p.Op)
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// CompiledPred caches one Pred -> BoundPredicate conversion so Scan can reuse it across every page and segment instead of paying for toBound on each call.
type CompiledPred struct {
	bp BoundPredicate
	ee EncodedEvaluator
}

func CompilePred(p Pred) (CompiledPred, error) {
	bp, err := p.toBound()
	if err != nil {
		return CompiledPred{}, err
	}
	ee, _ := bp.(EncodedEvaluator)
	return CompiledPred{bp: bp, ee: ee}, nil
}

func (c CompiledPred) Skips(seg *Segment) bool {
	return c.bp.PruneSegment(seg)
}

func (c CompiledPred) SkipsPage(seg *Segment, pageIdx int) bool {
	return c.bp.PrunePage(seg, pageIdx)
}

func (c CompiledPred) Apply(batch vector.Batch, sel *vector.SelectionMask) {
	c.bp.Eval(batch, sel)
}

func (c CompiledPred) MatchesPage(seg *Segment, pageIdx int) bool {
	return predicateAlwaysMatchesPage(c.bp, seg, pageIdx)
}

func (c CompiledPred) ApplyEncoded(src pageSource, pageIdx int, sel *vector.SelectionMask, scratch []byte) (bool, []byte, error) {
	if c.ee == nil {
		return false, scratch, nil
	}
	return c.ee.EvalEncoded(src, pageIdx, sel, scratch)
}

// cmpFilterOp maps ordered comparison PredOps to the vector kernel op, OpInvalid falls out as zero.
func cmpFilterOp(op PredOp) vector.FilterOp {
	switch op {
	case OpEq:
		return vector.FilterEqual
	case OpLt:
		return vector.FilterLess
	case OpLe:
		return vector.FilterLessEqual
	case OpGt:
		return vector.FilterGreater
	case OpGe:
		return vector.FilterGreaterEqual
	}
	return 0
}

// toBound maps a Pred to the equivalent bound predicate so eval and prune reuse the proven legacy code.
func (p Pred) toBound() (BoundPredicate, error) {
	switch p.Op {
	case OpEq, OpLt, OpLe, OpGt, OpGe:
		switch p.Kind {
		case vector.VecInt64, vector.VecTimestamp, vector.VecTime, vector.VecDecimal64:
			return boundCmpInt64{column: p.Col, colID: p.ColID, value: p.I64, op: cmpFilterOp(p.Op)}, nil
		case vector.VecInt16:
			if p.I64 < math.MinInt16 || p.I64 > math.MaxInt16 {
				return nil, fmt.Errorf("toBound: literal %d outside int16 range", p.I64)
			}
			return boundCmpNarrowInt{column: p.Col, colID: p.ColID, value: p.I64, op: cmpFilterOp(p.Op)}, nil
		case vector.VecInt32, vector.VecDate:
			if p.I64 < math.MinInt32 || p.I64 > math.MaxInt32 {
				return nil, fmt.Errorf("toBound: literal %d outside int32 range", p.I64)
			}
			return boundCmpNarrowInt{column: p.Col, colID: p.ColID, value: p.I64, op: cmpFilterOp(p.Op)}, nil
		case vector.VecFloat64:
			return boundCmpFloat64{column: p.Col, colID: p.ColID, value: p.F64, op: cmpFilterOp(p.Op)}, nil
		case vector.VecText, vector.VecBytes, vector.VecJSON:
			switch p.Op {
			case OpEq:
				return boundEqBytes{column: p.Col, colID: p.ColID, value: p.Bytes}, nil
			case OpLt:
				return boundLtBytes{column: p.Col, colID: p.ColID, value: p.Bytes}, nil
			case OpLe:
				return boundLtBytes{column: p.Col, colID: p.ColID, value: p.Bytes, inclusive: true}, nil
			case OpGt:
				return boundGtBytes{column: p.Col, colID: p.ColID, value: p.Bytes}, nil
			case OpGe:
				return boundGtBytes{column: p.Col, colID: p.ColID, value: p.Bytes, inclusive: true}, nil
			}
		}
	case OpIsNull:
		return boundIsNull{column: p.Col, colID: p.ColID}, nil
	case OpAnd:
		kids := make([]BoundPredicate, 0, len(p.Children))
		for _, c := range p.Children {
			b, err := c.toBound()
			if err != nil {
				return nil, err
			}
			kids = append(kids, b)
		}
		return &boundAnd{children: kids}, nil
	case OpOr:
		kids := make([]BoundPredicate, 0, len(p.Children))
		for _, c := range p.Children {
			b, err := c.toBound()
			if err != nil {
				return nil, err
			}
			kids = append(kids, b)
		}
		return &boundOr{children: kids}, nil
	case OpNot:
		if len(p.Children) != 1 {
			return nil, fmt.Errorf("Pred NOT requires one child")
		}
		inner, err := p.Children[0].toBound()
		if err != nil {
			return nil, err
		}
		return boundNot{child: inner, cols: p.Children[0].Columns()}, nil
	}
	return nil, fmt.Errorf("toBound: unsupported (op=%v, kind=%v)", p.Op, p.Kind)
}
