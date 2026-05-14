package exec

import (
	"context"
	"errors"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

var ErrSumOverflow = errors.New("sum int64 overflow")

type AggregateSink interface {
	Consume(types.Batch, types.SelectionMask) error
	Result() (any, error)
}

type encodedAggregateSink interface {
	EncodedColumns() []string
}

type Aggregate struct {
	Sinks []AggregateSink

	state operatorState
}

func (a *Aggregate) Open(context.Context) error {
	if len(a.Sinks) == 0 {
		return fmt.Errorf("aggregate requires at least one sink")
	}
	for i, sink := range a.Sinks {
		if sink == nil {
			return fmt.Errorf("aggregate sink %d is nil", i)
		}
	}
	return a.state.open()
}

func (a *Aggregate) Push(batch types.Batch, sel types.SelectionMask) error {
	if err := a.state.requireOpen(); err != nil {
		return err
	}
	for _, sink := range a.Sinks {
		if err := sink.Consume(batch, sel); err != nil {
			return err
		}
	}
	return nil
}

func (a *Aggregate) Close() error {
	return a.state.close()
}

func (a *Aggregate) EncodedColumns() []string {
	seen := make(map[string]struct{})
	var columns []string
	for _, sink := range a.Sinks {
		encoded, ok := sink.(encodedAggregateSink)
		if !ok {
			continue
		}
		for _, column := range encoded.EncodedColumns() {
			if column == "" {
				continue
			}
			if _, ok := seen[column]; ok {
				continue
			}
			seen[column] = struct{}{}
			columns = append(columns, column)
		}
	}
	return columns
}

type CountSink struct {
	N int64
}

func (s *CountSink) Consume(batch types.Batch, sel types.SelectionMask) error {
	if err := validateBatchSelection(batch, sel); err != nil {
		return err
	}
	s.N += int64(sel.PopCount())
	return nil
}

func (s *CountSink) Result() (any, error) {
	return s.N, nil
}

type CountNonNullSink struct {
	Column string
	N      int64
}

func (s *CountNonNullSink) Consume(batch types.Batch, sel types.SelectionMask) error {
	col, err := lookupSinkColumn(batch, sel, s.Column, "count", types.VecInvalid)
	if err != nil {
		return err
	}
	s.N += countValidSelected(col.V.Valid, sel)
	return nil
}

func (s *CountNonNullSink) Result() (any, error) {
	return s.N, nil
}

func (s *CountNonNullSink) EncodedColumns() []string {
	return []string{s.Column}
}

type SumInt64Result struct {
	Sum   int64
	Count int64
}

type SumInt64Sink struct {
	Column string
	Sum    int64
	Count  int64
}

func (s *SumInt64Sink) Consume(batch types.Batch, sel types.SelectionMask) error {
	col, err := lookupSinkColumn(batch, sel, s.Column, "sum int64", types.VecInt64)
	if err != nil {
		return err
	}
	sum, count, overflow := sumInt64VectorSelected(col.V, sel, s.Sum)
	if overflow {
		return ErrSumOverflow
	}
	s.Sum = sum
	s.Count += count
	return nil
}

func (s *SumInt64Sink) Result() (any, error) {
	return SumInt64Result{Sum: s.Sum, Count: s.Count}, nil
}

func (s *SumInt64Sink) EncodedColumns() []string {
	return []string{s.Column}
}

type SumInt32Sink struct {
	Column string
	Sum    int64
	Count  int64
}

func (s *SumInt32Sink) Consume(batch types.Batch, sel types.SelectionMask) error {
	col, err := lookupSinkColumn(batch, sel, s.Column, "sum int32", types.VecInt32)
	if err != nil {
		return err
	}
	sum, count := sumInt32VectorSelected(col.V, sel)
	s.Sum += sum
	s.Count += count
	return nil
}

func (s *SumInt32Sink) Result() (any, error) {
	return SumInt64Result{Sum: s.Sum, Count: s.Count}, nil
}

func (s *SumInt32Sink) EncodedColumns() []string {
	return []string{s.Column}
}

type MinMaxInt64Result struct {
	Value int64
	Set   bool
}

type MinInt64Sink struct {
	Column string
	Value  int64
	Set    bool
}

func (s *MinInt64Sink) Consume(batch types.Batch, sel types.SelectionMask) error {
	col, err := lookupSinkColumn(batch, sel, s.Column, "min int64", types.VecInt64)
	if err != nil {
		return err
	}
	value, set := minMaxInt64VectorSelected(col.V, sel, true)
	if set && (!s.Set || value < s.Value) {
		s.Value = value
		s.Set = true
	}
	return nil
}

func (s *MinInt64Sink) Result() (any, error) {
	return MinMaxInt64Result{Value: s.Value, Set: s.Set}, nil
}

func (s *MinInt64Sink) EncodedColumns() []string {
	return []string{s.Column}
}

type MaxInt64Sink struct {
	Column string
	Value  int64
	Set    bool
}

func (s *MaxInt64Sink) Consume(batch types.Batch, sel types.SelectionMask) error {
	col, err := lookupSinkColumn(batch, sel, s.Column, "max int64", types.VecInt64)
	if err != nil {
		return err
	}
	value, set := minMaxInt64VectorSelected(col.V, sel, false)
	if set && (!s.Set || value > s.Value) {
		s.Value = value
		s.Set = true
	}
	return nil
}

func (s *MaxInt64Sink) Result() (any, error) {
	return MinMaxInt64Result{Value: s.Value, Set: s.Set}, nil
}

func (s *MaxInt64Sink) EncodedColumns() []string {
	return []string{s.Column}
}

type AvgInt64Result struct {
	Sum   int64
	Count int64
	Avg   float64
	Set   bool
}

type AvgInt64Sink struct {
	Column string
	Sum    int64
	Count  int64
}

func (s *AvgInt64Sink) Consume(batch types.Batch, sel types.SelectionMask) error {
	col, err := lookupSinkColumn(batch, sel, s.Column, "avg int64", types.VecInt64)
	if err != nil {
		return err
	}
	sum, count, overflow := sumInt64VectorSelected(col.V, sel, s.Sum)
	if overflow {
		return ErrSumOverflow
	}
	s.Sum = sum
	s.Count += count
	return nil
}

func (s *AvgInt64Sink) Result() (any, error) {
	if s.Count == 0 {
		return AvgInt64Result{}, nil
	}
	return AvgInt64Result{Sum: s.Sum, Count: s.Count, Avg: float64(s.Sum) / float64(s.Count), Set: true}, nil
}

func (s *AvgInt64Sink) EncodedColumns() []string {
	return []string{s.Column}
}

type GroupStringCountSink struct {
	Column string
	Counts map[string]int64
}

func (s *GroupStringCountSink) Consume(batch types.Batch, sel types.SelectionMask) error {
	if s.Column == "" {
		return fmt.Errorf("group string column is required")
	}
	if s.Counts == nil {
		s.Counts = make(map[string]int64)
	}
	if err := validateBatchSelection(batch, sel); err != nil {
		return err
	}
	col, ok := columnByName(batch, s.Column)
	if !ok {
		return fmt.Errorf("missing group string column %q", s.Column)
	}
	if col.V.Kind != types.VecText {
		return fmt.Errorf("group string column %q has kind %s", s.Column, col.V.Kind)
	}
	return groupStringCountSelected(col.V, sel, s.Counts)
}

func (s *GroupStringCountSink) Result() (any, error) {
	return s.Counts, nil
}

type GroupKey struct {
	Kind  types.VecKind
	Bool  bool
	I64   int64
	U32   uint32
	UUID  types.UUID16
	Bytes string
}

type GroupAnyCountSink struct {
	Column string
	Counts map[GroupKey]int64
}

func (s *GroupAnyCountSink) Consume(batch types.Batch, sel types.SelectionMask) error {
	if s.Column == "" {
		return fmt.Errorf("group column is required")
	}
	if s.Counts == nil {
		s.Counts = make(map[GroupKey]int64)
	}
	if err := validateBatchSelection(batch, sel); err != nil {
		return err
	}
	col, ok := columnByName(batch, s.Column)
	if !ok {
		return fmt.Errorf("missing group column %q", s.Column)
	}
	if !groupAnyKind(col.V.Kind) {
		return fmt.Errorf("group column %q has unsupported kind %s", s.Column, col.V.Kind)
	}
	return groupAnyCountSelected(col.V, sel, s.Counts)
}

func (s *GroupAnyCountSink) Result() (any, error) {
	return s.Counts, nil
}

func groupAnyKind(kind types.VecKind) bool {
	switch kind {
	case types.VecBool,
		types.VecInt16,
		types.VecInt32,
		types.VecDate,
		types.VecInt64,
		types.VecDecimal64,
		types.VecTimestamp,
		types.VecTime,
		types.VecUUID,
		types.VecEnum32,
		types.VecBytes:
		return true
	default:
		return false
	}
}

func lookupSinkColumn(batch types.Batch, sel types.SelectionMask, name, op string, wantKind types.VecKind) (types.Column, error) {
	if name == "" {
		return types.Column{}, fmt.Errorf("%s column is required", op)
	}
	if err := validateBatchSelection(batch, sel); err != nil {
		return types.Column{}, err
	}
	col, ok := columnByName(batch, name)
	if !ok {
		return types.Column{}, fmt.Errorf("missing %s column %q", op, name)
	}
	if wantKind != types.VecInvalid && col.V.Kind != wantKind {
		return types.Column{}, fmt.Errorf("%s column %q has kind %s", op, name, col.V.Kind)
	}
	return col, nil
}

func columnByName(batch types.Batch, name string) (types.Column, bool) {
	for _, col := range batch.Columns {
		if col.Name == name {
			return col, true
		}
	}
	return types.Column{}, false
}

func AddInt64(left int64, right int64) (int64, bool) {
	next := left + right
	if (right > 0 && next < left) || (right < 0 && next > left) {
		return 0, false
	}
	return next, true
}
