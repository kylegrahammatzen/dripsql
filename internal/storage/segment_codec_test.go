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
		{name: "short dictionary data", encoded: shortDictionaryData, count: 1, wantErr: "short string dictionary data"},
		{name: "short dictionary ids", encoded: append(slices.Clone(validOneValueDictionary), 0), count: 2, wantErr: "short string dictionary ids"},
		{name: "dictionary id out of range", encoded: append(slices.Clone(validOneValueDictionary), 1), count: 1, wantErr: "out of range"},
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
