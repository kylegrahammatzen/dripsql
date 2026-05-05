package storage

import (
	"encoding/binary"
	"slices"
	"strings"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestEncodeStringUsesDictionaryWhenSmaller(t *testing.T) {
	values := []string{"signup", "checkout", "signup", "signup", "checkout", "signup"}

	encoded, err := encodeString(values)
	if err != nil {
		t.Fatal(err)
	}
	if encoded[0] != stringCodecDictionary {
		t.Fatalf("string codec = %d, want dictionary", encoded[0])
	}
	if len(encoded) >= plainStringPayloadLen(values) {
		t.Fatalf("encoded length = %d, want less than plain length %d", len(encoded), plainStringPayloadLen(values))
	}
	if !stringDictionaryIDEncodingIsPacked(int(encoded[5])) {
		t.Fatalf("string dictionary id encoding = %d, want packed", encoded[5])
	}

	decoded, err := decodeString(encoded, len(values))
	if err != nil {
		t.Fatal(err)
	}
	requireVectorEqual(t, vector.NewString(values), decoded)
}

func TestEncodeStringUsesPlainWhenDictionaryIsLarger(t *testing.T) {
	values := []string{"alpha", "bravo", "charlie", "delta"}

	encoded, err := encodeString(values)
	if err != nil {
		t.Fatal(err)
	}
	if encoded[0] != stringCodecPlain {
		t.Fatalf("string codec = %d, want plain", encoded[0])
	}

	decoded, err := decodeString(encoded, len(values))
	if err != nil {
		t.Fatal(err)
	}
	requireVectorEqual(t, vector.NewString(values), decoded)
}

func TestEncodeStringDictionaryHandlesEmptyStrings(t *testing.T) {
	values := []string{"", "", "", ""}

	encoded, err := encodeString(values)
	if err != nil {
		t.Fatal(err)
	}
	if encoded[0] != stringCodecDictionary {
		t.Fatalf("string codec = %d, want dictionary", encoded[0])
	}
	if stringDictionaryPackedBitWidth(int(encoded[5])) != 0 {
		t.Fatalf("string dictionary id encoding = %d, want packed bit width 0", encoded[5])
	}

	decoded, err := decodeString(encoded, len(values))
	if err != nil {
		t.Fatal(err)
	}
	requireVectorEqual(t, vector.NewString(values), decoded)
}

func TestSegmentRewritesCompactString(t *testing.T) {
	original := mustBatch(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "signup", "signup"})},
	)

	_, compact, _ := roundTrip(t, original)
	_, got, _ := roundTrip(t, compact)

	requireBatchEqual(t, original, got)
}

func TestDecodeStringRejectsBadPayload(t *testing.T) {
	validOneValueDictionary := []byte{stringCodecDictionary}
	validOneValueDictionary = binary.LittleEndian.AppendUint32(validOneValueDictionary, 1)
	validOneValueDictionary = append(validOneValueDictionary, 1)
	validOneValueDictionary = binary.LittleEndian.AppendUint32(validOneValueDictionary, 1)
	validOneValueDictionary = append(validOneValueDictionary, 'a')

	shortDictionaryData := []byte{stringCodecDictionary}
	shortDictionaryData = binary.LittleEndian.AppendUint32(shortDictionaryData, 1)
	shortDictionaryData = append(shortDictionaryData, 1)
	shortDictionaryData = binary.LittleEndian.AppendUint32(shortDictionaryData, 2)
	shortDictionaryData = append(shortDictionaryData, 'a')
	shortPackedIDs := []byte{stringCodecDictionary}
	shortPackedIDs = binary.LittleEndian.AppendUint32(shortPackedIDs, 2)
	shortPackedIDs = append(shortPackedIDs, stringDictionaryPackedIDFlag|1)
	shortPackedIDs = binary.LittleEndian.AppendUint32(shortPackedIDs, 1)
	shortPackedIDs = append(shortPackedIDs, 'a')
	shortPackedIDs = binary.LittleEndian.AppendUint32(shortPackedIDs, 1)
	shortPackedIDs = append(shortPackedIDs, 'b')
	packedIDOutOfRange := []byte{stringCodecDictionary}
	packedIDOutOfRange = binary.LittleEndian.AppendUint32(packedIDOutOfRange, 1)
	packedIDOutOfRange = append(packedIDOutOfRange, stringDictionaryPackedIDFlag|1)
	packedIDOutOfRange = binary.LittleEndian.AppendUint32(packedIDOutOfRange, 1)
	packedIDOutOfRange = append(packedIDOutOfRange, 'a', 1)

	tests := []struct {
		name    string
		encoded []byte
		count   int
		wantErr string
	}{
		{name: "missing codec", encoded: nil, wantErr: "missing string codec"},
		{name: "unknown codec", encoded: []byte{255}, wantErr: "unsupported string codec"},
		{name: "short plain", encoded: []byte{stringCodecPlain, 0, 0, 0}, count: 1, wantErr: "short string length"},
		{name: "plain trailing", encoded: []byte{stringCodecPlain, 0}, wantErr: "trailing string bytes"},
		{name: "short dictionary header", encoded: []byte{stringCodecDictionary, 0, 0, 0}, wantErr: "short string dictionary header"},
		{name: "bad dictionary id width", encoded: []byte{stringCodecDictionary, 0, 0, 0, 0, 3}, wantErr: "unsupported string dictionary id width"},
		{name: "bad packed bit width", encoded: []byte{stringCodecDictionary, 0, 0, 0, 0, stringDictionaryPackedIDFlag | 33}, wantErr: "unsupported string dictionary packed bit width"},
		{name: "short dictionary data", encoded: shortDictionaryData, count: 1, wantErr: "short string dictionary data"},
		{name: "short dictionary ids", encoded: append(slices.Clone(validOneValueDictionary), 0), count: 2, wantErr: "short string dictionary ids"},
		{name: "dictionary id out of range", encoded: append(slices.Clone(validOneValueDictionary), 1), count: 1, wantErr: "out of range"},
		{name: "short packed ids", encoded: shortPackedIDs, count: 2, wantErr: "short string dictionary ids"},
		{name: "packed id out of range", encoded: packedIDOutOfRange, count: 1, wantErr: "out of range"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeString(tt.encoded, tt.count)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %q, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestEncodeInt64UsesDictionaryWhenSmaller(t *testing.T) {
	values := []int64{7, 42, 7, 7, 42, 7}

	encoded := encodeInt64(values)
	if encoded[0] != int64CodecDictionary {
		t.Fatalf("int64 codec = %d, want dictionary", encoded[0])
	}
	if len(encoded) >= plainInt64PayloadLen(values) {
		t.Fatalf("encoded length = %d, want less than plain length %d", len(encoded), plainInt64PayloadLen(values))
	}
	if !int64DictionaryIDEncodingIsPacked(int(encoded[5])) {
		t.Fatalf("int64 dictionary id encoding = %d, want packed", encoded[5])
	}

	decoded, err := decodeInt64(encoded, len(values))
	if err != nil {
		t.Fatal(err)
	}
	requireVectorEqual(t, vector.NewInt64(values), vector.FromInt64(decoded))
}

func TestEncodeInt64DictionaryUsesFixedIDsWhenPackedTies(t *testing.T) {
	values := make([]int64, 512)
	for row := range values {
		values[row] = int64(row % 256)
	}

	encoded := encodeInt64(values)
	if encoded[0] != int64CodecDictionary {
		t.Fatalf("int64 codec = %d, want dictionary", encoded[0])
	}
	if encoded[5] != 1 {
		t.Fatalf("int64 dictionary id encoding = %d, want fixed width 1", encoded[5])
	}

	decoded, err := decodeInt64(encoded, len(values))
	if err != nil {
		t.Fatal(err)
	}
	requireVectorEqual(t, vector.NewInt64(values), vector.FromInt64(decoded))
}

func TestEncodeInt64UsesPlainWhenDictionaryIsLarger(t *testing.T) {
	values := []int64{1, 2, 3, 4}

	encoded := encodeInt64(values)
	if encoded[0] != int64CodecPlain {
		t.Fatalf("int64 codec = %d, want plain", encoded[0])
	}

	decoded, err := decodeInt64(encoded, len(values))
	if err != nil {
		t.Fatal(err)
	}
	requireVectorEqual(t, vector.NewInt64(values), vector.FromInt64(decoded))
}

func TestDecodeInt64RejectsBadPayload(t *testing.T) {
	validOneValueDictionary := []byte{int64CodecDictionary}
	validOneValueDictionary = binary.LittleEndian.AppendUint32(validOneValueDictionary, 1)
	validOneValueDictionary = append(validOneValueDictionary, 1)
	validOneValueDictionary = binary.LittleEndian.AppendUint64(validOneValueDictionary, 7)

	shortDictionaryValues := []byte{int64CodecDictionary}
	shortDictionaryValues = binary.LittleEndian.AppendUint32(shortDictionaryValues, 1)
	shortDictionaryValues = append(shortDictionaryValues, 1, 1, 2, 3)
	shortPackedIDs := []byte{int64CodecDictionary}
	shortPackedIDs = binary.LittleEndian.AppendUint32(shortPackedIDs, 2)
	shortPackedIDs = append(shortPackedIDs, int64DictionaryPackedIDFlag|1)
	shortPackedIDs = binary.LittleEndian.AppendUint64(shortPackedIDs, 7)
	shortPackedIDs = binary.LittleEndian.AppendUint64(shortPackedIDs, 42)
	packedIDOutOfRange := []byte{int64CodecDictionary}
	packedIDOutOfRange = binary.LittleEndian.AppendUint32(packedIDOutOfRange, 1)
	packedIDOutOfRange = append(packedIDOutOfRange, int64DictionaryPackedIDFlag|1)
	packedIDOutOfRange = binary.LittleEndian.AppendUint64(packedIDOutOfRange, 7)
	packedIDOutOfRange = append(packedIDOutOfRange, 1)

	tests := []struct {
		name    string
		encoded []byte
		count   int
		wantErr string
	}{
		{name: "missing codec", encoded: nil, wantErr: "missing int64 codec"},
		{name: "unknown codec", encoded: []byte{255}, wantErr: "unsupported int64 codec"},
		{name: "short plain", encoded: []byte{int64CodecPlain, 0, 0, 0}, count: 1, wantErr: "invalid int64 encoded length"},
		{name: "short dictionary header", encoded: []byte{int64CodecDictionary, 0, 0, 0}, wantErr: "short int64 dictionary header"},
		{name: "bad dictionary id width", encoded: []byte{int64CodecDictionary, 0, 0, 0, 0, 3}, wantErr: "unsupported int64 dictionary id width"},
		{name: "bad packed bit width", encoded: []byte{int64CodecDictionary, 0, 0, 0, 0, int64DictionaryPackedIDFlag | 33}, wantErr: "unsupported int64 dictionary packed bit width"},
		{name: "short dictionary values", encoded: shortDictionaryValues, count: 1, wantErr: "short int64 dictionary values"},
		{name: "short dictionary ids", encoded: append(slices.Clone(validOneValueDictionary), 0), count: 2, wantErr: "short int64 dictionary ids"},
		{name: "dictionary id out of range", encoded: append(slices.Clone(validOneValueDictionary), 1), count: 1, wantErr: "out of range"},
		{name: "short packed ids", encoded: shortPackedIDs, count: 2, wantErr: "short int64 dictionary ids"},
		{name: "packed id out of range", encoded: packedIDOutOfRange, count: 1, wantErr: "out of range"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeInt64(tt.encoded, tt.count)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %q, want %q", err, tt.wantErr)
			}
		})
	}
}

func BenchmarkEncodeInt64(b *testing.B) {
	b.ReportAllocs()

	values := make([]int64, 100_000)
	for i := range values {
		values[i] = int64(i % 1024)
	}

	for b.Loop() {
		_ = encodeInt64(values)
	}
}

func BenchmarkDecodeInt64(b *testing.B) {
	b.ReportAllocs()

	values := make([]int64, 100_000)
	for i := range values {
		values[i] = int64(i % 1024)
	}
	encoded := encodeInt64(values)

	for b.Loop() {
		if _, err := decodeInt64(encoded, len(values)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeString(b *testing.B) {
	b.ReportAllocs()

	choices := []string{"signup", "checkout", "page_view", "cancel"}
	values := make([]string, 100_000)
	for i := range values {
		values[i] = choices[i%len(choices)]
	}
	encoded, err := encodeString(values)
	if err != nil {
		b.Fatal(err)
	}

	for b.Loop() {
		if _, err := decodeString(encoded, len(values)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeStringPlain(b *testing.B) {
	b.ReportAllocs()

	values := make([]string, 100_000)
	for i := range values {
		values[i] = string([]byte{byte(i), byte(i >> 8), byte(i >> 16)})
	}
	encoded, err := encodeString(values)
	if err != nil {
		b.Fatal(err)
	}
	if encoded[0] != stringCodecPlain {
		b.Fatalf("string codec = %d, want plain", encoded[0])
	}

	for b.Loop() {
		if _, err := decodeString(encoded, len(values)); err != nil {
			b.Fatal(err)
		}
	}
}

func plainStringPayloadLen(values []string) int {
	size := 1
	for _, value := range values {
		size += 4 + len(value)
	}
	return size
}

func plainInt64PayloadLen(values []int64) int {
	return 1 + len(values)*8
}
