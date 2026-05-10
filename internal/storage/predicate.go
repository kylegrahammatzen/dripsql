package storage

import "github.com/kylegrahammatzen/dripsql/internal/types"

type PredicateOp uint8

const (
	PredicateNone PredicateOp = iota
	PredicateOpEq
	PredicateOpNotEq
	PredicateOpLess
	PredicateOpLessEqual
	PredicateOpGreater
	PredicateOpGreaterEqual
	PredicateOpBetween
	PredicateOpIn
	PredicateOpNotIn
	PredicateAnd
	PredicateOr
	PredicateNot
)

type Predicate struct {
	Column string
	Op     PredicateOp

	Bool  bool
	Bools []bool

	Int64  int64
	Lo     int64
	Hi     int64
	Int64s []int64

	Text  string
	Texts []string

	Children []Predicate
}

func PredicateColumns(pred Predicate) []string {
	var out []string
	seen := make(map[string]struct{})
	collectPredicateColumns(pred, seen, &out)
	return out
}

func collectPredicateColumns(pred Predicate, seen map[string]struct{}, out *[]string) {
	switch pred.Op {
	case PredicateNone:
		return
	case PredicateAnd, PredicateOr, PredicateNot:
		for _, child := range pred.Children {
			collectPredicateColumns(child, seen, out)
		}
	default:
		if pred.Column == "" {
			return
		}
		if _, ok := seen[pred.Column]; ok {
			return
		}
		seen[pred.Column] = struct{}{}
		*out = append(*out, pred.Column)
	}
}

func columnByName(batch types.Batch, name string) (types.Column, bool) {
	for _, col := range batch.Columns {
		if col.Name == name {
			return col, true
		}
	}
	return types.Column{}, false
}
