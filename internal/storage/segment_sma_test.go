package storage

import (
	"reflect"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestTextStatsGroupSumsRoundTripV2(t *testing.T) {
	original := SegmentMeta{
		ID:       7,
		Rows:     12,
		PageRows: 12,
		Columns: []ColumnMeta{
			{
				Name: "country",
				Type: types.Text,
				Rows: 12,
				Text: &TextStats{
					DataBytes: 24,
					Values:    []string{"CA", "GB", "US"},
					Counts:    []uint32{3, 4, 5},
					GroupSums: map[string][]int64{
						"amount": {30, 40, 50},
						"count":  {3, 4, 5},
					},
				},
				Pages: []PageMeta{{Rows: 12, Kind: types.VecText, Encoding: types.EncodingDictionary}},
			},
		},
	}
	encoded, err := marshalSegmentMeta(original)
	if err != nil {
		t.Fatalf("marshalSegmentMeta: %v", err)
	}
	decoded, err := unmarshalSegmentMeta(encoded)
	if err != nil {
		t.Fatalf("unmarshalSegmentMeta: %v", err)
	}
	got := decoded.Columns[0].Text
	want := original.Columns[0].Text
	if !reflect.DeepEqual(got.GroupSums, want.GroupSums) {
		t.Fatalf("GroupSums = %#v, want %#v", got.GroupSums, want.GroupSums)
	}
	if !reflect.DeepEqual(got.Values, want.Values) || !reflect.DeepEqual(got.Counts, want.Counts) {
		t.Fatalf("existing fields drifted: got %#v want %#v", got, want)
	}
}

func TestTextStatsTruncatedSkipsGroupSums(t *testing.T) {
	original := SegmentMeta{
		ID:       8,
		Rows:     12,
		PageRows: 12,
		Columns: []ColumnMeta{
			{
				Name: "url",
				Type: types.Text,
				Rows: 12,
				Text: &TextStats{
					DataBytes: 80,
					Truncated: true,
					GroupSums: map[string][]int64{"amount": {1, 2, 3}},
				},
				Pages: []PageMeta{{Rows: 12, Kind: types.VecText, Encoding: types.EncodingFlat}},
			},
		},
	}
	encoded, err := marshalSegmentMeta(original)
	if err != nil {
		t.Fatalf("marshalSegmentMeta: %v", err)
	}
	decoded, err := unmarshalSegmentMeta(encoded)
	if err != nil {
		t.Fatalf("unmarshalSegmentMeta: %v", err)
	}
	if decoded.Columns[0].Text.GroupSums != nil {
		t.Fatalf("truncated stats kept GroupSums = %v", decoded.Columns[0].Text.GroupSums)
	}
}

func TestTextStatsGroupCountsRoundTripV2(t *testing.T) {
	original := SegmentMeta{
		ID:       9,
		Rows:     12,
		PageRows: 12,
		Columns: []ColumnMeta{
			{
				Name: "country",
				Type: types.Text,
				Rows: 12,
				Text: &TextStats{
					DataBytes: 24,
					Values:    []string{"CA", "US"},
					Counts:    []uint32{4, 8},
					GroupCounts: map[string]map[string][]int64{
						"event_type": {
							"checkout": {1, 4},
							"login":    {3, 4},
						},
					},
				},
				Pages: []PageMeta{{Rows: 12, Kind: types.VecText, Encoding: types.EncodingDictionary}},
			},
		},
	}
	encoded, err := marshalSegmentMeta(original)
	if err != nil {
		t.Fatalf("marshalSegmentMeta: %v", err)
	}
	decoded, err := unmarshalSegmentMeta(encoded)
	if err != nil {
		t.Fatalf("unmarshalSegmentMeta: %v", err)
	}
	got := decoded.Columns[0].Text.GroupCounts
	want := original.Columns[0].Text.GroupCounts
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GroupCounts = %#v, want %#v", got, want)
	}
	if v, ok := decoded.Columns[0].Text.CountByValueAndPeer("event_type", "checkout", 1); !ok || v != 4 {
		t.Fatalf("CountByValueAndPeer = %d, %v; want 4, true", v, ok)
	}
}

func TestTextStatsCountByValueAndPeerEdges(t *testing.T) {
	stats := &TextStats{
		Values: []string{"X", "Y"},
		GroupCounts: map[string]map[string][]int64{
			"event_type": {"checkout": {3, 5}},
		},
	}
	if v, ok := stats.CountByValueAndPeer("event_type", "checkout", 0); !ok || v != 3 {
		t.Fatalf("X = %d, %v; want 3, true", v, ok)
	}
	if _, ok := stats.CountByValueAndPeer("event_type", "missing", 0); ok {
		t.Fatal("missing sibling value returned ok=true")
	}
	if _, ok := stats.CountByValueAndPeer("missing_col", "checkout", 0); ok {
		t.Fatal("missing sibling col returned ok=true")
	}
	if _, ok := stats.CountByValueAndPeer("event_type", "checkout", 9); ok {
		t.Fatal("out-of-range idx returned ok=true")
	}
	stats.Truncated = true
	if _, ok := stats.CountByValueAndPeer("event_type", "checkout", 0); ok {
		t.Fatal("truncated returned ok=true")
	}
}

func TestTextStatsSumByValueAccessor(t *testing.T) {
	stats := &TextStats{
		Values:    []string{"US", "CA"},
		Counts:    []uint32{10, 5},
		GroupSums: map[string][]int64{"amount": {100, 50}},
	}
	if v, ok := stats.SumByValue("amount", 0); !ok || v != 100 {
		t.Fatalf("SumByValue(amount, 0) = %d, %v; want 100, true", v, ok)
	}
	if v, ok := stats.SumByValue("amount", 1); !ok || v != 50 {
		t.Fatalf("SumByValue(amount, 1) = %d, %v; want 50, true", v, ok)
	}
	if _, ok := stats.SumByValue("missing", 0); ok {
		t.Fatal("SumByValue on missing column returned ok=true")
	}
	if _, ok := stats.SumByValue("amount", 9); ok {
		t.Fatal("SumByValue out of range returned ok=true")
	}
	stats.Truncated = true
	if _, ok := stats.SumByValue("amount", 0); ok {
		t.Fatal("SumByValue on truncated returned ok=true")
	}
	var nilStats *TextStats
	if _, ok := nilStats.SumByValue("amount", 0); ok {
		t.Fatal("SumByValue on nil returned ok=true")
	}
}

func TestSegmentFooterRejectsUnknownMagic(t *testing.T) {
	bad := append([]byte("DRIPFTRX"), make([]byte, 8)...)
	if _, err := unmarshalSegmentMeta(bad); err == nil {
		t.Fatal("unmarshalSegmentMeta accepted unknown magic")
	}
}

func TestWriteSegmentPopulatesSMAForLowCardText(t *testing.T) {
	dir := t.TempDir()
	path := filepathJoin(dir, "sma.seg")
	batch := smaTextIntBatch(t, []string{"US", "CA", "US", "GB", "CA", "US"}, []int64{1, 2, 3, 4, 5, 6})
	meta, err := WriteSegment(path, 1, []types.Batch{batch})
	if err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	got, err := ReadSegmentFooter(path)
	if err != nil {
		t.Fatalf("ReadSegmentFooter: %v", err)
	}
	if len(got.Columns) != len(meta.Columns) {
		t.Fatalf("column count drift")
	}
	textCol := smaFindColumn(t, got, "country")
	if textCol.Text == nil || textCol.Text.GroupSums == nil {
		t.Fatalf("country.GroupSums missing: %#v", textCol.Text)
	}
	sums, ok := textCol.Text.GroupSums["amount"]
	if !ok {
		t.Fatalf("GroupSums[amount] missing: %#v", textCol.Text.GroupSums)
	}
	want := map[string]int64{"US": 1 + 3 + 6, "CA": 2 + 5, "GB": 4}
	for i, value := range textCol.Text.Values {
		if got := sums[i]; got != want[value] {
			t.Fatalf("GroupSums[amount][%q] = %d, want %d", value, got, want[value])
		}
	}
}

func TestWriteSegmentPopulatesCrossCounts(t *testing.T) {
	dir := t.TempDir()
	path := filepathJoin(dir, "cross.seg")
	countries := []string{"US", "CA", "US", "GB", "CA", "US", "GB", "US"}
	events := []string{"checkout", "login", "checkout", "checkout", "login", "login", "checkout", "checkout"}
	countryVar := types.NewVarBytes(len(countries), 32)
	eventVar := types.NewVarBytes(len(events), 64)
	for i, v := range countries {
		countryVar.AppendString(i, v)
	}
	for i, v := range events {
		eventVar.AppendString(i, v)
	}
	batch, err := types.NewBatch([]types.Column{
		{Name: "country", Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: len(countries), Var: countryVar}},
		{Name: "event_type", Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: len(events), Var: eventVar}},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	if _, err := WriteSegment(path, 1, []types.Batch{batch}); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	got, err := ReadSegmentFooter(path)
	if err != nil {
		t.Fatalf("ReadSegmentFooter: %v", err)
	}
	countryCol := smaFindColumn(t, got, "country")
	if countryCol.Text.GroupCounts == nil {
		t.Fatalf("country.GroupCounts missing: %#v", countryCol.Text)
	}
	bySibling, ok := countryCol.Text.GroupCounts["event_type"]
	if !ok {
		t.Fatalf("country.GroupCounts[event_type] missing")
	}
	checkout := bySibling["checkout"]
	if len(checkout) != len(countryCol.Text.Values) {
		t.Fatalf("checkout slice len = %d, want %d", len(checkout), len(countryCol.Text.Values))
	}
	// Hand-tally: checkout matches at rows 0, 2, 3, 6, 7 → US=3, CA=0, GB=2.
	want := map[string]int64{"US": 3, "CA": 0, "GB": 2}
	for i, value := range countryCol.Text.Values {
		if checkout[i] != want[value] {
			t.Fatalf("checkout[%q] = %d, want %d", value, checkout[i], want[value])
		}
	}
	// Symmetric: event_type column should also have cross-counts back to country.
	eventCol := smaFindColumn(t, got, "event_type")
	if eventCol.Text.GroupCounts["country"] == nil {
		t.Fatalf("event_type.GroupCounts[country] missing")
	}
	usCounts := eventCol.Text.GroupCounts["country"]["US"]
	// US matches at rows 0, 2, 5, 7 → checkout=3, login=1.
	wantEvent := map[string]int64{"checkout": 3, "login": 1}
	for i, value := range eventCol.Text.Values {
		if usCounts[i] != wantEvent[value] {
			t.Fatalf("event_type[%q] for country=US = %d, want %d", value, usCounts[i], wantEvent[value])
		}
	}
}

func TestWriteSegmentSMAHandlesInt32(t *testing.T) {
	dir := t.TempDir()
	path := filepathJoin(dir, "sma32.seg")
	values := []string{"X", "Y", "X", "Y"}
	amounts := []int32{10, 20, 30, 40}
	textVar := types.NewVarBytes(len(values), 16)
	for i, v := range values {
		textVar.AppendString(i, v)
	}
	batch, err := types.NewBatch([]types.Column{
		{Name: "country", Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: len(values), Var: textVar}},
		{Name: "amount", Type: types.Int32, V: types.Vec{Kind: types.VecInt32, Encoding: types.EncodingFlat, Len: len(amounts), I32: amounts}},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	if _, err := WriteSegment(path, 1, []types.Batch{batch}); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	got, err := ReadSegmentFooter(path)
	if err != nil {
		t.Fatalf("ReadSegmentFooter: %v", err)
	}
	textCol := smaFindColumn(t, got, "country")
	sums := textCol.Text.GroupSums["amount"]
	want := map[string]int64{"X": 10 + 30, "Y": 20 + 40}
	for i, value := range textCol.Text.Values {
		if sums[i] != want[value] {
			t.Fatalf("GroupSums[amount][%q] = %d, want %d", value, sums[i], want[value])
		}
	}
}

func TestWriteSegmentSkipsSMAForTruncatedText(t *testing.T) {
	dir := t.TempDir()
	path := filepathJoin(dir, "trunc.seg")
	values := make([]string, TextStatsMaxValues+10)
	amounts := make([]int64, len(values))
	for i := range values {
		values[i] = string(rune('a'+i%26)) + string(rune('0'+i/26))
		amounts[i] = int64(i)
	}
	textVar := types.NewVarBytes(len(values), len(values)*4)
	for i, v := range values {
		textVar.AppendString(i, v)
	}
	batch, err := types.NewBatch([]types.Column{
		{Name: "url", Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: len(values), Var: textVar}},
		{Name: "amount", Type: types.Int64, V: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: len(amounts), I64: amounts}},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	if _, err := WriteSegment(path, 1, []types.Batch{batch}); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	got, err := ReadSegmentFooter(path)
	if err != nil {
		t.Fatalf("ReadSegmentFooter: %v", err)
	}
	textCol := smaFindColumn(t, got, "url")
	if !textCol.Text.Truncated {
		t.Fatalf("expected truncated, got %#v", textCol.Text)
	}
	if textCol.Text.GroupSums != nil {
		t.Fatalf("truncated stats should have no GroupSums, got %v", textCol.Text.GroupSums)
	}
}

func smaTextIntBatch(t *testing.T, values []string, amounts []int64) types.Batch {
	t.Helper()
	textVar := types.NewVarBytes(len(values), len(values)*8)
	for i, v := range values {
		textVar.AppendString(i, v)
	}
	batch, err := types.NewBatch([]types.Column{
		{Name: "country", Type: types.Text, V: types.Vec{Kind: types.VecText, Encoding: types.EncodingFlat, Len: len(values), Var: textVar}},
		{Name: "amount", Type: types.Int64, V: types.Vec{Kind: types.VecInt64, Encoding: types.EncodingFlat, Len: len(amounts), I64: amounts}},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return batch
}

func smaFindColumn(t *testing.T, meta SegmentMeta, name string) ColumnMeta {
	t.Helper()
	for _, col := range meta.Columns {
		if col.Name == name {
			return col
		}
	}
	t.Fatalf("column %q not in segment meta", name)
	return ColumnMeta{}
}

func filepathJoin(dir, name string) string {
	return dir + string([]byte{'/'}) + name
}
