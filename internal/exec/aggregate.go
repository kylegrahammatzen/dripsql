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
	if s.Column == "" {
		return fmt.Errorf("count column is required")
	}
	if err := validateBatchSelection(batch, sel); err != nil {
		return err
	}
	col, ok := columnByName(batch, s.Column)
	if !ok {
		return fmt.Errorf("missing count column %q", s.Column)
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
	if s.Column == "" {
		return fmt.Errorf("sum int64 column is required")
	}
	if err := validateBatchSelection(batch, sel); err != nil {
		return err
	}
	col, ok := columnByName(batch, s.Column)
	if !ok {
		return fmt.Errorf("missing sum int64 column %q", s.Column)
	}
	if col.V.Kind != types.VecInt64 {
		return fmt.Errorf("sum int64 column %q has kind %s", s.Column, col.V.Kind)
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
	if s.Column == "" {
		return fmt.Errorf("sum int32 column is required")
	}
	if err := validateBatchSelection(batch, sel); err != nil {
		return err
	}
	col, ok := columnByName(batch, s.Column)
	if !ok {
		return fmt.Errorf("missing sum int32 column %q", s.Column)
	}
	if col.V.Kind != types.VecInt32 {
		return fmt.Errorf("sum int32 column %q has kind %s", s.Column, col.V.Kind)
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
	if s.Column == "" {
		return fmt.Errorf("min int64 column is required")
	}
	col, err := int64Column(batch, sel, s.Column, "min")
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
	if s.Column == "" {
		return fmt.Errorf("max int64 column is required")
	}
	col, err := int64Column(batch, sel, s.Column, "max")
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
	if s.Column == "" {
		return fmt.Errorf("avg int64 column is required")
	}
	col, err := int64Column(batch, sel, s.Column, "avg")
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

func groupKey(v types.Vec, row int) (GroupKey, error) {
	switch v.Kind {
	case types.VecBool:
		return GroupKey{Kind: v.Kind, Bool: v.BoolBits[row>>6]&(uint64(1)<<uint(row&63)) != 0}, nil
	case types.VecInt16:
		return GroupKey{Kind: v.Kind, I64: int64(v.I16[row])}, nil
	case types.VecInt32, types.VecDate:
		return GroupKey{Kind: v.Kind, I64: int64(v.I32[row])}, nil
	case types.VecInt64, types.VecDecimal64, types.VecTimestamp, types.VecTime:
		return GroupKey{Kind: v.Kind, I64: v.I64[row]}, nil
	case types.VecUUID:
		return GroupKey{Kind: v.Kind, UUID: v.UUID[row]}, nil
	case types.VecEnum32:
		return GroupKey{Kind: v.Kind, U32: v.U32[row]}, nil
	case types.VecBytes:
		if v.Encoding != types.EncodingFlat {
			return GroupKey{}, fmt.Errorf("group bytes unsupported encoding %s", v.Encoding)
		}
		return GroupKey{Kind: v.Kind, Bytes: v.Var.StringCopy(row)}, nil
	default:
		return GroupKey{}, fmt.Errorf("unsupported group kind %s", v.Kind)
	}
}

func int64Column(batch types.Batch, sel types.SelectionMask, name string, op string) (types.Column, error) {
	if err := validateBatchSelection(batch, sel); err != nil {
		return types.Column{}, err
	}
	col, ok := columnByName(batch, name)
	if !ok {
		return types.Column{}, fmt.Errorf("missing %s int64 column %q", op, name)
	}
	if col.V.Kind != types.VecInt64 {
		return types.Column{}, fmt.Errorf("%s int64 column %q has kind %s", op, name, col.V.Kind)
	}
	return col, nil
}

func TextValueCopy(v types.Vec, row int) (string, bool) {
	switch v.Encoding {
	case types.EncodingFlat:
		return v.Var.StringCopy(row), true
	case types.EncodingDictionary:
		if row >= len(v.DictIDs) {
			return "", false
		}
		id := int(v.DictIDs[row])
		if id >= v.DictValues.Rows() {
			return "", false
		}
		return v.DictValues.StringCopy(id), true
	default:
		return "", false
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

func AddInt64(left int64, right int64) (int64, bool) {
	next := left + right
	if (right > 0 && next < left) || (right < 0 && next > left) {
		return 0, false
	}
	return next, true
}
