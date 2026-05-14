package storage

import (
	"slices"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

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

// PredicateValue holds the literal payload for a comparison or set predicate.
type PredicateValue struct {
	Bool  bool
	Bools []bool

	Int64  int64
	Lo     int64
	Hi     int64
	Int64s []int64

	Text  string
	Texts []string

	UUID  types.UUID16
	UUIDs []types.UUID16
}

type Predicate struct {
	Column string
	Op     PredicateOp
	PredicateValue
	Children []Predicate
}

// setMatcher is the value-set membership test shared by IN, NOT IN, and
// segment/page pruning across every comparable column type. It stores up to
// eight values inline as a slice and switches to a map at larger sizes.
type setMatcher[T comparable] struct {
	small []T
	large map[T]struct{}
}

func newSetMatcher[T comparable](values []T) setMatcher[T] {
	if len(values) <= 8 {
		return setMatcher[T]{small: slices.Clone(values)}
	}
	large := make(map[T]struct{}, len(values))
	for _, value := range values {
		large[value] = struct{}{}
	}
	return setMatcher[T]{large: large}
}

func (m setMatcher[T]) Has(value T) bool {
	if m.large != nil {
		_, ok := m.large[value]
		return ok
	}
	return slices.Contains(m.small, value)
}

// Each invokes fn for every value in the set; ordering is unspecified.
func (m setMatcher[T]) Each(fn func(T) bool) {
	if m.large != nil {
		for value := range m.large {
			if !fn(value) {
				return
			}
		}
		return
	}
	for _, value := range m.small {
		if !fn(value) {
			return
		}
	}
}

type (
	boolMatcher  = setMatcher[bool]
	int64Matcher = setMatcher[int64]
	textMatcher  = setMatcher[string]
	uuidMatcher  = setMatcher[types.UUID16]
)

func anyIntBetween(m int64Matcher, lo int64, hi int64) bool {
	match := false
	m.Each(func(value int64) bool {
		if lo <= value && value <= hi {
			match = true
			return false
		}
		return true
	})
	return match
}

func anyTextInBloom(m textMatcher, bloom []uint64, probes uint64) bool {
	if len(bloom) == 0 {
		return true
	}
	match := false
	m.Each(func(value string) bool {
		if hashBloomHas(bloom, textHash32String(value), probes) {
			match = true
			return false
		}
		return true
	})
	return match
}

func anyUUIDInBloom(m uuidMatcher, bloom []uint64, probes uint64) bool {
	if len(bloom) == 0 {
		return true
	}
	match := false
	m.Each(func(value types.UUID16) bool {
		if hashBloomHas(bloom, uuidHash32(value), probes) {
			match = true
			return false
		}
		return true
	})
	return match
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
