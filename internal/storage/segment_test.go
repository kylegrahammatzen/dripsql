package storage

import (
	"bytes"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestSegmentRoundTrip(t *testing.T) {
	batch, err := vector.NewBatch(
		vector.Column{Name: "tenant_id", Values: vector.NewInt64([]int64{7, 42, 7})},
		vector.Column{Name: "event_type", Values: vector.NewString([]string{"signup", "checkout", "signup"})},
	)
	if err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	writeMeta, err := WriteSegment(&buf, batch)
	if err != nil {
		t.Fatal(err)
	}
	if writeMeta.Rows != 3 {
		t.Fatalf("written rows = %d, want 3", writeMeta.Rows)
	}
	if writeMeta.Columns[0].MinInt64 != 7 || writeMeta.Columns[0].MaxInt64 != 42 {
		t.Fatalf("written min/max = %d/%d, want 7/42", writeMeta.Columns[0].MinInt64, writeMeta.Columns[0].MaxInt64)
	}

	got, readMeta, err := ReadSegment(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if got.Count != 3 {
		t.Fatalf("decoded rows = %d, want 3", got.Count)
	}
	if readMeta.Rows != 3 || len(readMeta.Columns) != 2 {
		t.Fatalf("metadata = %+v, want 3 rows and 2 columns", readMeta)
	}

	tenantCol, ok := got.Column("tenant_id")
	if !ok {
		t.Fatal("missing tenant_id column")
	}
	tenantValues, ok := tenantCol.Values.(vector.Int64)
	if !ok {
		t.Fatalf("tenant_id type = %T, want vector.Int64", tenantCol.Values)
	}
	if tenantValues.Values[1] != 42 {
		t.Fatalf("tenant_id[1] = %d, want 42", tenantValues.Values[1])
	}

	eventCol, ok := got.Column("event_type")
	if !ok {
		t.Fatal("missing event_type column")
	}
	eventValues, ok := eventCol.Values.(vector.String)
	if !ok {
		t.Fatalf("event_type type = %T, want vector.String", eventCol.Values)
	}
	if eventValues.Values[1] != "checkout" {
		t.Fatalf("event_type[1] = %q, want checkout", eventValues.Values[1])
	}
}

func TestReadSegmentRejectsBadMagic(t *testing.T) {
	_, _, err := ReadSegment(bytes.NewReader([]byte("not-a-segment")))
	if err == nil {
		t.Fatal("expected invalid magic error")
	}
}
