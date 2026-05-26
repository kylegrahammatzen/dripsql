package storage

import (
	"reflect"
	"testing"
)

func TestIdentitySidecar_RoundTrip(t *testing.T) {
	id := SegmentIdentity{
		TableID:          42,
		SchemaGeneration: 99,
		ColumnIDs:        []uint64{1, 2, 7},
	}
	b := encodeIdentitySidecar(id)
	if b == nil {
		t.Fatal("encode returned nil for populated identity")
	}
	got, err := decodeIdentitySidecar(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !reflect.DeepEqual(got, id) {
		t.Fatalf("round-trip: got %+v, want %+v", got, id)
	}
}

func TestIdentitySidecar_ZeroIsNoBody(t *testing.T) {
	if encodeIdentitySidecar(SegmentIdentity{}) != nil {
		t.Fatal("zero identity should encode to nil so writers can skip the section entirely")
	}
}

func TestIdentitySidecar_RejectsBadMagic(t *testing.T) {
	b := encodeIdentitySidecar(SegmentIdentity{TableID: 1, ColumnIDs: []uint64{1}})
	b[0] = 'X'
	if _, err := decodeIdentitySidecar(b); err == nil {
		t.Fatal("expected error on bad magic")
	}
}

func TestIdentitySidecar_RejectsTruncated(t *testing.T) {
	b := encodeIdentitySidecar(SegmentIdentity{TableID: 1, ColumnIDs: []uint64{1, 2, 3}})
	if _, err := decodeIdentitySidecar(b[:len(b)-1]); err == nil {
		t.Fatal("expected error on truncated payload")
	}
}
