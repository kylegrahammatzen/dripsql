package exec

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/storage/codec"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestAggregateLifecycleAndCountSelection(t *testing.T) {
	batch := execInt64Batch(t, []int64{1, 2, 3}, nil)
	sel := types.NewSelectionMask(batch.Len)
	sel.Set(0)
	sel.Set(2)
	sink := &CountSink{}
	agg := &Aggregate{Sinks: []AggregateSink{sink}}
	if err := agg.Push(batch, sel); !errors.Is(err, ErrOperatorNotOpen) {
		t.Fatalf("Push before Open err = %v, want ErrOperatorNotOpen", err)
	}
	if err := agg.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := agg.Open(context.Background()); !errors.Is(err, ErrOperatorAlreadyOpen) {
		t.Fatalf("second Open err = %v, want ErrOperatorAlreadyOpen", err)
	}
	if err := agg.Push(batch, sel); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if got, err := sink.Result(); err != nil || got.(int64) != 2 {
		t.Fatalf("Result = %v, %v; want 2, nil", got, err)
	}
	if err := agg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := agg.Push(batch, sel); !errors.Is(err, ErrOperatorClosed) {
		t.Fatalf("Push after Close err = %v, want ErrOperatorClosed", err)
	}
}

func TestCountSinkHandlesEmptyAndDeselectedBatches(t *testing.T) {
	sink := &CountSink{}
	if err := sink.Consume(types.Batch{}, types.SelectionMask{}); err != nil {
		t.Fatalf("Consume empty: %v", err)
	}
	batch := execInt64Batch(t, []int64{1, 2, 3}, nil)
	if err := sink.Consume(batch, types.NewSelectionMask(batch.Len)); err != nil {
		t.Fatalf("Consume deselected: %v", err)
	}
	if got, err := sink.Result(); err != nil || got.(int64) != 0 {
		t.Fatalf("Result = %v, %v; want 0, nil", got, err)
	}
}

func TestSumInt64SinkSelectionAndNulls(t *testing.T) {
	valid := types.NewValidity(4)
	types.SetInvalid(valid, 1)
	batch := execInt64Batch(t, []int64{10, 99, -4, 7}, valid)
	sel := types.NewSelectionMask(batch.Len)
	sel.Set(0)
	sel.Set(1)
	sel.Set(2)
	sink := &SumInt64Sink{Column: "amount"}
	if err := sink.Consume(batch, sel); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	gotAny, err := sink.Result()
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	got := gotAny.(SumInt64Result)
	if got.Sum != 6 || got.Count != 2 {
		t.Fatalf("Result = %#v, want sum=6 count=2", got)
	}
}

func TestSumInt64SinkOverflow(t *testing.T) {
	batch := execInt64Batch(t, []int64{math.MaxInt64, 1}, nil)
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	sink := &SumInt64Sink{Column: "amount"}
	if err := sink.Consume(batch, sel); !errors.Is(err, ErrSumOverflow) {
		t.Fatalf("Consume err = %v, want ErrSumOverflow", err)
	}
}

func TestSumInt64SinkFORBitPackSelectionAndNulls(t *testing.T) {
	valid := types.NewValidity(4)
	types.SetInvalid(valid, 1)
	batch := execFORInt64Batch(t, []int64{10, 99, -4, 7}, valid)
	sel := types.NewSelectionMask(batch.Len)
	sel.Set(0)
	sel.Set(1)
	sel.Set(2)
	sink := &SumInt64Sink{Column: "amount"}
	if err := sink.Consume(batch, sel); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	gotAny, err := sink.Result()
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	got := gotAny.(SumInt64Result)
	if got.Sum != 6 || got.Count != 2 {
		t.Fatalf("Result = %#v, want sum=6 count=2", got)
	}
}

func TestSumInt64SinkConstantSelection(t *testing.T) {
	batch := execConstantInt64Batch(t, 7, 4, nil)
	sel := types.NewSelectionMask(batch.Len)
	sel.Set(0)
	sel.Set(2)
	sink := &SumInt64Sink{Column: "amount"}
	if err := sink.Consume(batch, sel); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	gotAny, err := sink.Result()
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	got := gotAny.(SumInt64Result)
	if got.Sum != 14 || got.Count != 2 {
		t.Fatalf("Result = %#v, want sum=14 count=2", got)
	}
}

func TestCountNonNullSinkSelection(t *testing.T) {
	valid := types.NewValidity(4)
	types.SetInvalid(valid, 1)
	batch := execInt64Batch(t, []int64{1, 2, 3, 4}, valid)
	sel := types.NewSelectionMask(batch.Len)
	sel.Set(0)
	sel.Set(1)
	sel.Set(3)
	sink := &CountNonNullSink{Column: "amount"}
	if err := sink.Consume(batch, sel); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if got, err := sink.Result(); err != nil || got.(int64) != 2 {
		t.Fatalf("Result = %v, %v; want 2, nil", got, err)
	}
}

func TestSumInt32SinkSelectionAndNulls(t *testing.T) {
	valid := types.NewValidity(4)
	types.SetInvalid(valid, 2)
	batch := execInt32Batch(t, []int32{10, -2, 99, 7}, valid)
	sel := types.NewSelectionMask(batch.Len)
	sel.Set(0)
	sel.Set(2)
	sel.Set(3)
	sink := &SumInt32Sink{Column: "score"}
	if err := sink.Consume(batch, sel); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	gotAny, err := sink.Result()
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	got := gotAny.(SumInt64Result)
	if got.Sum != 17 || got.Count != 2 {
		t.Fatalf("Result = %#v, want sum=17 count=2", got)
	}
}

func TestMinMaxInt64SinksSkipNulls(t *testing.T) {
	valid := types.NewValidity(5)
	types.SetInvalid(valid, 1)
	batch := execInt64Batch(t, []int64{10, -100, 3, 90, -4}, valid)
	sel := types.NewSelectionMask(batch.Len)
	sel.Set(0)
	sel.Set(1)
	sel.Set(4)
	minSink := &MinInt64Sink{Column: "amount"}
	maxSink := &MaxInt64Sink{Column: "amount"}
	for _, sink := range []AggregateSink{minSink, maxSink} {
		if err := sink.Consume(batch, sel); err != nil {
			t.Fatalf("Consume: %v", err)
		}
	}
	minAny, err := minSink.Result()
	if err != nil {
		t.Fatalf("min Result: %v", err)
	}
	maxAny, err := maxSink.Result()
	if err != nil {
		t.Fatalf("max Result: %v", err)
	}
	min := minAny.(MinMaxInt64Result)
	max := maxAny.(MinMaxInt64Result)
	if !min.Set || min.Value != -4 || !max.Set || max.Value != 10 {
		t.Fatalf("min/max = %#v/%#v, want -4/10", min, max)
	}
}

func TestMinMaxInt64SinksFORBitPack(t *testing.T) {
	batch := execFORInt64Batch(t, []int64{10, -100, 3, 90, -4}, nil)
	sel := types.NewSelectionMask(batch.Len)
	sel.Set(0)
	sel.Set(2)
	sel.Set(4)
	minSink := &MinInt64Sink{Column: "amount"}
	maxSink := &MaxInt64Sink{Column: "amount"}
	for _, sink := range []AggregateSink{minSink, maxSink} {
		if err := sink.Consume(batch, sel); err != nil {
			t.Fatalf("Consume: %v", err)
		}
	}
	minAny, err := minSink.Result()
	if err != nil {
		t.Fatalf("min Result: %v", err)
	}
	maxAny, err := maxSink.Result()
	if err != nil {
		t.Fatalf("max Result: %v", err)
	}
	min := minAny.(MinMaxInt64Result)
	max := maxAny.(MinMaxInt64Result)
	if !min.Set || min.Value != -4 || !max.Set || max.Value != 10 {
		t.Fatalf("min/max = %#v/%#v, want -4/10", min, max)
	}
}

func TestAggregateRequestsEncodedNumericColumns(t *testing.T) {
	agg := &Aggregate{Sinks: []AggregateSink{
		&SumInt64Sink{Column: "amount"},
		&CountNonNullSink{Column: "amount"},
		&SumInt32Sink{Column: "score"},
		&CountSink{},
	}}
	got := agg.EncodedColumns()
	if len(got) != 2 || got[0] != "amount" || got[1] != "score" {
		t.Fatalf("EncodedColumns = %v, want [amount score]", got)
	}
}

func TestMinInt64SinkAllNullResultUnset(t *testing.T) {
	valid := types.NewValidity(2)
	types.SetInvalid(valid, 0)
	types.SetInvalid(valid, 1)
	batch := execInt64Batch(t, []int64{1, 2}, valid)
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	sink := &MinInt64Sink{Column: "amount"}
	if err := sink.Consume(batch, sel); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	gotAny, err := sink.Result()
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if got := gotAny.(MinMaxInt64Result); got.Set {
		t.Fatalf("Result = %#v, want unset", got)
	}
}

func TestAvgInt64Sink(t *testing.T) {
	valid := types.NewValidity(4)
	types.SetInvalid(valid, 3)
	batch := execInt64Batch(t, []int64{10, 20, 30, 999}, valid)
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	sink := &AvgInt64Sink{Column: "amount"}
	if err := sink.Consume(batch, sel); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	gotAny, err := sink.Result()
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	got := gotAny.(AvgInt64Result)
	if !got.Set || got.Sum != 60 || got.Count != 3 || got.Avg != 20 {
		t.Fatalf("Result = %#v, want sum=60 count=3 avg=20", got)
	}
}

func TestGroupStringCountSinkFlatAndDict(t *testing.T) {
	flat := execTextBatch(t, []string{"checkout", "login", "checkout"}, nil)
	dict := execDictTextBatch(t, []uint8{0, 1, 0, 1}, []string{"checkout", "signup"}, nil)
	sink := &GroupStringCountSink{Column: "event_type"}
	flatSel := types.NewSelectionMask(flat.Len)
	flatSel.FillAll()
	if err := sink.Consume(flat, flatSel); err != nil {
		t.Fatalf("Consume flat: %v", err)
	}
	dictSel := types.NewSelectionMask(dict.Len)
	dictSel.Set(0)
	dictSel.Set(1)
	dictSel.Set(3)
	if err := sink.Consume(dict, dictSel); err != nil {
		t.Fatalf("Consume dict: %v", err)
	}
	gotAny, err := sink.Result()
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	got := gotAny.(map[string]int64)
	if got["checkout"] != 3 || got["login"] != 1 || got["signup"] != 2 || len(got) != 3 {
		t.Fatalf("counts = %#v", got)
	}
}

func TestGroupStringCountSinkSkipsNulls(t *testing.T) {
	valid := types.NewValidity(3)
	types.SetInvalid(valid, 1)
	batch := execTextBatch(t, []string{"checkout", "ignored", "checkout"}, valid)
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	sink := &GroupStringCountSink{Column: "event_type"}
	if err := sink.Consume(batch, sel); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	gotAny, err := sink.Result()
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	got := gotAny.(map[string]int64)
	if got["checkout"] != 2 || len(got) != 1 {
		t.Fatalf("counts = %#v", got)
	}
}

func TestGroupStringCountSinkRejectsInvalidDictID(t *testing.T) {
	batch := execDictTextBatch(t, []uint8{0, 2}, []string{"checkout"}, nil)
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	sink := &GroupStringCountSink{Column: "event_type"}
	if err := sink.Consume(batch, sel); err == nil {
		t.Fatal("expected invalid dictionary id error")
	}
}

func TestGroupStringCountSinkCopiesFlatKeys(t *testing.T) {
	batch := execTextBatch(t, []string{"checkout"}, nil)
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	sink := &GroupStringCountSink{Column: "event_type"}
	if err := sink.Consume(batch, sel); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	copy(batch.Columns[0].V.Var.Data, []byte("MUTATION"))
	if sink.Counts["checkout"] != 1 {
		t.Fatalf("counts = %#v, want stable checkout key", sink.Counts)
	}
}

func TestGroupStringCountSinkCopiesDictKeys(t *testing.T) {
	batch := execDictTextBatch(t, []uint8{0}, []string{"checkout"}, nil)
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	sink := &GroupStringCountSink{Column: "event_type"}
	if err := sink.Consume(batch, sel); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	copy(batch.Columns[0].V.DictValues.Data, []byte("MUTATION"))
	if sink.Counts["checkout"] != 1 {
		t.Fatalf("counts = %#v, want stable checkout key", sink.Counts)
	}
}

func TestAggregateSelectionAllEmptyAndMalformedSelection(t *testing.T) {
	if !selectionAll(types.SelectionMask{}) {
		t.Fatal("empty selection should be all-selected for zero rows")
	}
	if selectionAll(types.SelectionMask{Rows: 1}) {
		t.Fatal("selection with rows but no words should not be all-selected")
	}
	batch := execInt64Batch(t, []int64{1}, nil)
	sink := &SumInt64Sink{Column: "amount"}
	if err := sink.Consume(batch, types.SelectionMask{Rows: batch.Len}); err == nil {
		t.Fatal("expected malformed selection error")
	}
}

func TestGroupAnyCountSinkInt64AcrossBatches(t *testing.T) {
	first := execInt64Batch(t, []int64{42, 7, 42}, nil)
	second := execInt64Batch(t, []int64{7, 9}, nil)
	sink := &GroupAnyCountSink{Column: "amount"}
	firstSel := types.NewSelectionMask(first.Len)
	firstSel.FillAll()
	if err := sink.Consume(first, firstSel); err != nil {
		t.Fatalf("Consume first: %v", err)
	}
	secondSel := types.NewSelectionMask(second.Len)
	secondSel.Set(0)
	if err := sink.Consume(second, secondSel); err != nil {
		t.Fatalf("Consume second: %v", err)
	}
	got := groupAnyCounts(t, sink)
	if got[GroupKey{Kind: types.VecInt64, I64: 42}] != 2 || got[GroupKey{Kind: types.VecInt64, I64: 7}] != 2 || len(got) != 2 {
		t.Fatalf("counts = %#v", got)
	}
}

func TestGroupAnyCountSinkBoolSkipsNulls(t *testing.T) {
	valid := types.NewValidity(4)
	types.SetInvalid(valid, 1)
	batch := execBoolBatch(t, []bool{true, false, false, true}, valid)
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	sink := &GroupAnyCountSink{Column: "flag"}
	if err := sink.Consume(batch, sel); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	got := groupAnyCounts(t, sink)
	if got[GroupKey{Kind: types.VecBool, Bool: true}] != 2 || got[GroupKey{Kind: types.VecBool, Bool: false}] != 1 || len(got) != 2 {
		t.Fatalf("counts = %#v", got)
	}
}

func TestGroupAnyCountSinkEnumUUIDAndBytes(t *testing.T) {
	batch := execMixedGroupBatch(t)
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	for _, tt := range []struct {
		name   string
		column string
		want   map[GroupKey]int64
	}{
		{name: "enum", column: "status", want: map[GroupKey]int64{
			{Kind: types.VecEnum32, U32: 1}: 2,
			{Kind: types.VecEnum32, U32: 2}: 1,
		}},
		{name: "uuid", column: "request_id", want: map[GroupKey]int64{
			{Kind: types.VecUUID, UUID: testUUID(1)}: 2,
			{Kind: types.VecUUID, UUID: testUUID(2)}: 1,
		}},
		{name: "bytes", column: "payload", want: map[GroupKey]int64{
			{Kind: types.VecBytes, Bytes: "a"}: 2,
			{Kind: types.VecBytes, Bytes: "b"}: 1,
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sink := &GroupAnyCountSink{Column: tt.column}
			if err := sink.Consume(batch, sel); err != nil {
				t.Fatalf("Consume: %v", err)
			}
			got := groupAnyCounts(t, sink)
			if len(got) != len(tt.want) {
				t.Fatalf("counts = %#v, want %#v", got, tt.want)
			}
			for key, want := range tt.want {
				if got[key] != want {
					t.Fatalf("counts[%#v] = %d, want %d (all %#v)", key, got[key], want, got)
				}
			}
		})
	}
}

func TestGroupAnyCountSinkRejectsText(t *testing.T) {
	batch := execTextBatch(t, []string{"a"}, nil)
	sel := types.NewSelectionMask(batch.Len)
	sel.FillAll()
	sink := &GroupAnyCountSink{Column: "event_type"}
	if err := sink.Consume(batch, sel); err == nil {
		t.Fatal("expected unsupported text group kind error")
	}
}

func execInt64Batch(t testing.TB, values []int64, valid types.Validity) types.Batch {
	t.Helper()
	batch, err := types.NewBatch([]types.Column{{Name: "amount", Type: types.Int64, V: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: len(values), Valid: valid, I64: append([]int64(nil), values...)}}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func execFORInt64Batch(t testing.TB, values []int64, valid types.Validity) types.Batch {
	t.Helper()
	page, err := (codec.FORBitPack{}).Encode(types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: len(values), Valid: valid, I64: append([]int64(nil), values...)})
	if err != nil {
		t.Fatalf("FOR Encode: %v", err)
	}
	vec, err := (codec.FORBitPack{}).DecodeEncoded(page)
	if err != nil {
		t.Fatalf("FOR DecodeEncoded: %v", err)
	}
	batch, err := types.NewBatch([]types.Column{{Name: "amount", Type: types.Int64, V: vec}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func execConstantInt64Batch(t testing.TB, value int64, rows int, valid types.Validity) types.Batch {
	t.Helper()
	batch, err := types.NewBatch([]types.Column{{Name: "amount", Type: types.Int64, V: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingConstant, Len: rows, Valid: valid, ConstantI64: value, ConstantValid: true}}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func execInt32Batch(t testing.TB, values []int32, valid types.Validity) types.Batch {
	t.Helper()
	batch, err := types.NewBatch([]types.Column{{Name: "score", Type: types.Int32, V: types.Vec{Kind: types.VecInt32, Encoding: types.EncodingFlat, Len: len(values), Valid: valid, I32: append([]int32(nil), values...)}}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func execTextBatch(t testing.TB, values []string, valid types.Validity) types.Batch {
	t.Helper()
	varText := types.NewVarBytes(len(values), 0)
	for row, value := range values {
		varText.AppendString(row, value)
	}
	batch, err := types.NewBatch([]types.Column{{Name: "event_type", Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: len(values), Valid: valid, Var: varText}}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func execDictTextBatch(t testing.TB, ids []uint8, values []string, valid types.Validity) types.Batch {
	t.Helper()
	dict := types.NewVarBytes(len(values), 0)
	for row, value := range values {
		dict.AppendString(row, value)
	}
	batch, err := types.NewBatch([]types.Column{{Name: "event_type", Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingDictionary, Len: len(ids), Valid: valid, DictIDs: append([]uint8(nil), ids...), DictValues: dict}}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func execBoolBatch(t *testing.T, values []bool, valid types.Validity) types.Batch {
	t.Helper()
	bits := make([]uint64, types.ValidityWords(len(values)))
	for row, value := range values {
		if value {
			bits[row>>6] |= uint64(1) << uint(row&63)
		}
	}
	batch, err := types.NewBatch([]types.Column{{Name: "flag", Type: types.Bool, V: types.Vec{Kind: types.VecBool, Encoding: types.EncodingFlat, Len: len(values), Valid: valid, BoolBits: bits}}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func execMixedGroupBatch(t *testing.T) types.Batch {
	t.Helper()
	payload := types.NewVarBytes(3, 0)
	for row, value := range []string{"a", "b", "a"} {
		payload.AppendString(row, value)
	}
	batch, err := types.NewBatch([]types.Column{
		{Name: "status", Type: types.Named("status"), EnumLabels: []string{"new", "done"}, V: types.Vec{Kind: types.VecEnum32, Encoding: types.EncodingFlat, Len: 3, U32: []uint32{1, 2, 1}}},
		{Name: "request_id", Type: types.UUID, V: types.Vec{Kind: types.VecUUID, Encoding: types.EncodingFlat, Len: 3, UUID: []types.UUID16{testUUID(1), testUUID(2), testUUID(1)}}},
		{Name: "payload", Type: types.Bytes, V: types.Vec{Kind: types.VecBytes, Encoding: types.EncodingFlat, Len: 3, Var: payload}},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func testUUID(value byte) types.UUID16 {
	var out types.UUID16
	out[15] = value
	return out
}

func groupAnyCounts(t *testing.T, sink *GroupAnyCountSink) map[GroupKey]int64 {
	t.Helper()
	gotAny, err := sink.Result()
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	return gotAny.(map[GroupKey]int64)
}
