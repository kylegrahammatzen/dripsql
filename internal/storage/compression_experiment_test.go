package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/bits"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestSyntheticStorageIdeaComparison(t *testing.T) {
	rows := storageExperimentRows(t)
	batch := syntheticStorageExperimentBatch(t, 0, rows)

	var segment bytes.Buffer
	stats, err := WriteSegment(&segment, batch)
	if err != nil {
		t.Fatalf("WriteSegment() error = %v", err)
	}
	verifyExperimentRoundTrips(t, batch, segment.Bytes())

	actualColumns := int64(0)
	ideaPayload := int64(0)
	for _, col := range batch.Columns {
		segmentCol, ok := stats.Column(col.Name)
		if !ok {
			t.Fatalf("missing stats for column %q", col.Name)
		}
		columnBytes := columnPhysicalBytes(segmentCol)
		actualColumns += columnBytes
		idea := storageIdeaForColumn(col, segmentCol)
		ideaPayload += idea.Bytes
		t.Logf("column=%-10s kind=%-6s actual=%9s payload=%9s page/filter=%9s idea=%-24s idea=%9s shrink=%5.2fx note=%s",
			col.Name,
			col.Vector.Kind(),
			formatExperimentBytes(columnBytes),
			formatExperimentBytes(segmentCol.Payload.Bytes),
			formatExperimentBytes(segmentCol.Pages.Bytes+segmentCol.Filters.Bytes),
			idea.Name,
			formatExperimentBytes(idea.Bytes),
			experimentRatio(columnBytes, idea.Bytes),
			idea.Note,
		)
	}

	envelope := int64(segment.Len()) - actualColumns
	if envelope < 0 {
		t.Fatalf("envelope bytes = %d", envelope)
	}
	ideaWithCurrentEnvelope := ideaPayload + envelope

	t.Logf("summary rows=%s actual=%s column_bytes=%s envelope=%s idea_payload=%s idea_plus_current_envelope=%s shrink=%5.2fx",
		commasExperiment(rows),
		formatExperimentBytes(int64(segment.Len())),
		formatExperimentBytes(actualColumns),
		formatExperimentBytes(envelope),
		formatExperimentBytes(ideaPayload),
		formatExperimentBytes(ideaWithCurrentEnvelope),
		experimentRatio(int64(segment.Len()), ideaWithCurrentEnvelope),
	)

	if ideaWithCurrentEnvelope >= int64(segment.Len()) {
		t.Fatalf("idea storage estimate %s is not smaller than current storage %s", formatExperimentBytes(ideaWithCurrentEnvelope), formatExperimentBytes(int64(segment.Len())))
	}
}

func verifyExperimentRoundTrips(t *testing.T, want vector.Batch, data []byte) {
	t.Helper()
	reader, err := OpenSegmentBytes(data)
	if err != nil {
		t.Fatalf("OpenSegmentBytes() error = %v", err)
	}
	got, _, err := reader.ReadBatch(nil)
	if err != nil {
		t.Fatalf("ReadBatch() error = %v", err)
	}
	assertExperimentBatchEqual(t, "storage", want, got)
	t.Logf("roundtrip verified: storage exactly reconstructs %s rows across %d columns", commasExperiment(want.Count), len(want.Columns))
}

func assertExperimentBatchEqual(t *testing.T, label string, want vector.Batch, got vector.Batch) {
	t.Helper()
	if got.Count != want.Count {
		t.Fatalf("%s rows = %d, want %d", label, got.Count, want.Count)
	}
	if len(got.Columns) != len(want.Columns) {
		t.Fatalf("%s columns = %d, want %d", label, len(got.Columns), len(want.Columns))
	}
	for _, wantCol := range want.Columns {
		gotCol, ok := got.Column(wantCol.Name)
		if !ok {
			t.Fatalf("%s missing column %q", label, wantCol.Name)
		}
		if gotCol.Vector.Kind() != wantCol.Vector.Kind() {
			t.Fatalf("%s column %q kind = %s, want %s", label, wantCol.Name, gotCol.Vector.Kind(), wantCol.Vector.Kind())
		}
		switch wantValues := wantCol.Vector.(type) {
		case vector.Int64:
			gotValues := gotCol.Vector.(vector.Int64)
			if len(gotValues.Values) != len(wantValues.Values) {
				t.Fatalf("%s column %q rows = %d, want %d", label, wantCol.Name, len(gotValues.Values), len(wantValues.Values))
			}
			for row, wantValue := range wantValues.Values {
				if gotValues.Values[row] != wantValue {
					t.Fatalf("%s column %q row %d = %d, want %d", label, wantCol.Name, row, gotValues.Values[row], wantValue)
				}
			}
		case vector.String:
			gotValues := gotCol.Vector.(vector.String)
			if gotValues.Len() != wantValues.Len() {
				t.Fatalf("%s column %q rows = %d, want %d", label, wantCol.Name, gotValues.Len(), wantValues.Len())
			}
			for row := 0; row < wantValues.Len(); row++ {
				if gotValue, wantValue := gotValues.Value(row), wantValues.Value(row); gotValue != wantValue {
					t.Fatalf("%s column %q row %d = %q, want %q", label, wantCol.Name, row, gotValue, wantValue)
				}
			}
		default:
			t.Fatalf("%s column %q unsupported kind %s", label, wantCol.Name, wantCol.Vector.Kind())
		}
	}
}

type storageIdea struct {
	Name  string
	Bytes int64
	Note  string
}

func storageIdeaForColumn(col vector.Column, segmentCol Column) storageIdea {
	actual := columnPhysicalBytes(segmentCol)
	best := storageIdea{Name: "current", Bytes: actual, Note: "current column bytes"}
	switch values := col.Vector.(type) {
	case vector.Int64:
		best = minStorageIdea(best, int64SequenceIdea(values))
		best = minStorageIdea(best, int64FORBitpackIdea(values))
		best = minStorageIdea(best, int64DeltaVarintIdea(values))
	case vector.String:
		best = minStorageIdea(best, structuredStringIdea(col.Name, values))
	}
	return best
}

func minStorageIdea(a storageIdea, b storageIdea) storageIdea {
	if b.Bytes <= 0 {
		return a
	}
	if a.Bytes <= 0 || b.Bytes < a.Bytes {
		return b
	}
	return a
}

func int64SequenceIdea(values vector.Int64) storageIdea {
	if len(values.Values) == 0 {
		return storageIdea{Name: "int64-sequence", Bytes: 1, Note: "empty sequence"}
	}
	step := int64(0)
	if len(values.Values) > 1 {
		step = values.Values[1] - values.Values[0]
	}
	for row := 2; row < len(values.Values); row++ {
		if values.Values[row]-values.Values[row-1] != step {
			return storageIdea{}
		}
	}
	return storageIdea{Name: "int64-sequence", Bytes: int64(int64SequencePayloadLen), Note: "base + step; count in footer"}
}

func int64FORBitpackIdea(values vector.Int64) storageIdea {
	if len(values.Values) == 0 {
		return storageIdea{Name: "int64-for-bitpack", Bytes: 1, Note: "empty block"}
	}
	minValue, maxValue, ok := values.MinMax()
	if !ok {
		return storageIdea{}
	}
	if !okInt64UnsignedSpan(minValue, maxValue) {
		return storageIdea{}
	}
	span := uint64(maxValue - minValue)
	bitWidth := bits.Len64(span)
	bytes := int64(1 + binary.MaxVarintLen64*2 + 1)
	packed, err := packedBitsBytes(len(values.Values), bitWidth)
	if err != nil {
		return storageIdea{}
	}
	bytes += packed
	return storageIdea{Name: fmt.Sprintf("int64-for-bitpack/%db", bitWidth), Bytes: bytes, Note: "frame-of-reference + packed offsets"}
}

func int64DeltaVarintIdea(values vector.Int64) storageIdea {
	if len(values.Values) == 0 {
		return storageIdea{Name: "int64-delta-varint", Bytes: 1, Note: "empty block"}
	}
	buf := make([]byte, 0, len(values.Values)*2)
	buf = append(buf, 1)
	buf = binary.AppendVarint(buf, values.Values[0])
	prev := values.Values[0]
	for _, value := range values.Values[1:] {
		buf = binary.AppendVarint(buf, value-prev)
		prev = value
	}
	return storageIdea{Name: "int64-delta-varint", Bytes: int64(len(buf)), Note: "base + signed varint deltas"}
}

func structuredStringIdea(name string, values vector.String) storageIdea {
	switch name {
	case "url":
		return prefixBase36SequenceIdea(values, "/item/", "", "url-prefix-sequence")
	case "email":
		return prefixBase36SequenceIdea(values, "u", "@drip.test", "email-prefix-sequence")
	case "payload":
		return payloadPatternIdea(values)
	default:
		return storageIdea{}
	}
}

func prefixBase36SequenceIdea(values vector.String, prefix string, suffix string, name string) storageIdea {
	ids := make([]int64, values.Len())
	for row := 0; row < values.Len(); row++ {
		value := values.Value(row)
		if !strings.HasPrefix(value, prefix) || !strings.HasSuffix(value, suffix) {
			return storageIdea{}
		}
		middle := value[len(prefix) : len(value)-len(suffix)]
		id, err := strconv.ParseInt(middle, 36, 64)
		if err != nil {
			return storageIdea{}
		}
		ids[row] = id
	}
	idea := int64SequenceIdea(vector.FromInt64(ids))
	if idea.Bytes == 0 {
		idea = int64FORBitpackIdea(vector.FromInt64(ids))
	}
	if idea.Bytes == 0 {
		return storageIdea{}
	}
	return storageIdea{Name: name, Bytes: int64(1+len(prefix)+len(suffix)) + idea.Bytes, Note: "shared prefix/suffix + numeric tail"}
}

func payloadPatternIdea(values vector.String) storageIdea {
	ids := make([]int64, values.Len())
	buckets := make([]int64, values.Len())
	for row := 0; row < values.Len(); row++ {
		value := values.Value(row)
		if !strings.HasPrefix(value, "id=") {
			return storageIdea{}
		}
		parts := strings.Split(value[3:], ";bucket=")
		if len(parts) != 2 {
			return storageIdea{}
		}
		id, err := strconv.ParseInt(parts[0], 36, 64)
		if err != nil {
			return storageIdea{}
		}
		bucket, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return storageIdea{}
		}
		ids[row] = id
		buckets[row] = bucket
	}
	idIdea := int64SequenceIdea(vector.FromInt64(ids))
	if idIdea.Bytes == 0 {
		idIdea = int64FORBitpackIdea(vector.FromInt64(ids))
	}
	bucketIdea := int64FORBitpackIdea(vector.FromInt64(buckets))
	if idIdea.Bytes == 0 || bucketIdea.Bytes == 0 {
		return storageIdea{}
	}
	return storageIdea{Name: "payload-struct", Bytes: 1 + idIdea.Bytes + bucketIdea.Bytes, Note: "id sequence + packed bucket"}
}

func okInt64UnsignedSpan(minValue int64, maxValue int64) bool {
	return minValue >= 0 && maxValue >= minValue
}

func packedBitsBytes(rows int, bitWidth int) (int64, error) {
	bitsLen, err := checkedMulInt("experiment packed bits", rows, bitWidth)
	if err != nil {
		return 0, err
	}
	bytesLen, err := checkedAddInt("experiment packed bytes", bitsLen, 7)
	if err != nil {
		return 0, err
	}
	return int64(bytesLen / 8), nil
}

func columnPhysicalBytes(col Column) int64 {
	return col.Payload.Bytes + col.Pages.Bytes + col.Filters.Bytes
}

func storageExperimentRows(t *testing.T) int {
	t.Helper()
	rows := 100_000
	if raw := os.Getenv("DRIPSQL_STORAGE_EXPERIMENT_ROWS"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 {
			t.Fatalf("DRIPSQL_STORAGE_EXPERIMENT_ROWS=%q is not a positive integer", raw)
		}
		rows = parsed
	}
	if testing.Short() && rows > 100_000 {
		t.Skip("skipping large storage experiment in short mode")
	}
	return rows
}

func syntheticStorageExperimentBatch(t *testing.T, start int64, count int) vector.Batch {
	t.Helper()
	cols := syntheticExperimentColumns(start, count)
	urls, err := makeExperimentStrings(start, count, appendExperimentURL)
	if err != nil {
		t.Fatalf("make urls: %v", err)
	}
	emails, err := makeExperimentStrings(start, count, appendExperimentEmail)
	if err != nil {
		t.Fatalf("make emails: %v", err)
	}
	payloads, err := makeExperimentStrings(start, count, appendExperimentPayload)
	if err != nil {
		t.Fatalf("make payloads: %v", err)
	}
	batch, err := vector.NewBatch(
		vector.Column{Name: "tenant_id", Vector: vector.FromInt64(cols.tenants)},
		vector.Column{Name: "user_id", Vector: vector.FromInt64(cols.users)},
		vector.Column{Name: "created_at", Vector: vector.FromInt64(cols.createdAt)},
		vector.Column{Name: "event_type", Vector: vector.FromString(cols.events)},
		vector.Column{Name: "country", Vector: vector.FromString(cols.countries)},
		vector.Column{Name: "status", Vector: vector.FromString(cols.statuses)},
		vector.Column{Name: "path", Vector: vector.FromString(cols.paths)},
		vector.Column{Name: "user_agent", Vector: vector.FromString(cols.userAgents)},
		vector.Column{Name: "url", Vector: urls},
		vector.Column{Name: "email", Vector: emails},
		vector.Column{Name: "payload", Vector: payloads},
	)
	if err != nil {
		t.Fatalf("NewBatch() error = %v", err)
	}
	return batch
}

type syntheticExperimentColumnData struct {
	tenants    []int64
	users      []int64
	createdAt  []int64
	events     []string
	countries  []string
	statuses   []string
	paths      []string
	userAgents []string
}

func syntheticExperimentColumns(start int64, count int) syntheticExperimentColumnData {
	eventChoices := []string{"signup", "checkout", "page_view", "cancel"}
	countryChoices := []string{"US", "CA", "GB", "DE", "FR", "JP", "BR", "AU"}
	statusChoices := []string{"ok", "retry", "error"}
	pathChoices := []string{"/", "/pricing", "/docs", "/search", "/cart", "/checkout", "/account", "/support", "/blog", "/settings", "/api", "/download", "/products", "/teams", "/billing", "/logout"}
	userAgentChoices := []string{"Chrome/Windows", "Safari/iOS", "Firefox/Linux", "Edge/Windows", "Chrome/Android", "Safari/macOS"}
	cols := syntheticExperimentColumnData{
		tenants:    make([]int64, count),
		users:      make([]int64, count),
		createdAt:  make([]int64, count),
		events:     make([]string, count),
		countries:  make([]string, count),
		statuses:   make([]string, count),
		paths:      make([]string, count),
		userAgents: make([]string, count),
	}
	for i := 0; i < count; i++ {
		row := start + int64(i)
		cols.tenants[i] = row % 1024
		cols.users[i] = (row * 13) % 10_000_000
		cols.createdAt[i] = 1_700_000_000 + row
		cols.events[i] = eventChoices[row%int64(len(eventChoices))]
		cols.countries[i] = countryChoices[(row/7)%int64(len(countryChoices))]
		cols.statuses[i] = statusChoices[(row/13)%int64(len(statusChoices))]
		cols.paths[i] = pathChoices[(row/17)%int64(len(pathChoices))]
		cols.userAgents[i] = userAgentChoices[(row/19)%int64(len(userAgentChoices))]
	}
	return cols
}

func makeExperimentStrings(start int64, count int, build func([]byte, int64) []byte) (vector.String, error) {
	data := make([]byte, 0, count*24)
	ranges := make([]uint64, count)
	for i := 0; i < count; i++ {
		row := start + int64(i)
		begin := len(data)
		data = build(data, row)
		length := len(data) - begin
		if uint64(begin) > uint64(^uint32(0)) || uint64(length) > uint64(^uint32(0)) {
			return vector.String{}, fmt.Errorf("generated string column exceeds 32-bit string range")
		}
		ranges[i] = vector.StringRange(uint32(begin), uint32(length))
	}
	return vector.FromStringDataUnsafe(data, ranges), nil
}

func appendExperimentURL(buf []byte, row int64) []byte {
	buf = append(buf, "/item/"...)
	return strconv.AppendInt(buf, row%10_000_000, 36)
}

func appendExperimentEmail(buf []byte, row int64) []byte {
	buf = append(buf, 'u')
	buf = strconv.AppendInt(buf, (row*17)%50_000_000, 36)
	return append(buf, "@drip.test"...)
}

func appendExperimentPayload(buf []byte, row int64) []byte {
	buf = append(buf, "id="...)
	buf = strconv.AppendInt(buf, row%10_000_000, 36)
	buf = append(buf, ";bucket="...)
	return strconv.AppendInt(buf, row%97, 10)
}

func formatExperimentBytes(n int64) string {
	const mib = 1024 * 1024
	const kib = 1024
	if n >= mib {
		return fmt.Sprintf("%.2f MiB", float64(n)/mib)
	}
	if n >= kib {
		return fmt.Sprintf("%.2f KiB", float64(n)/kib)
	}
	return fmt.Sprintf("%d B", n)
}

func experimentRatio(before int64, after int64) float64 {
	if after <= 0 {
		return 0
	}
	return float64(before) / float64(after)
}

func commasExperiment(n int) string {
	s := strconv.Itoa(n)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}
