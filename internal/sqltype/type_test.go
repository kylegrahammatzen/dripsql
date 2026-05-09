package sqltype

import "testing"

func TestTypeValid(t *testing.T) {
	for _, typ := range []Type{Bool, Int16, Int32, Int64, Float32, Float64, Decimal, Text, Bytes, UUID, Timestamp, Time, Date, JSON, Named("status")} {
		if !typ.Valid() {
			t.Fatalf("%s should be valid", typ)
		}
	}
	for _, typ := range []Type{{}, Named("")} {
		if typ.Valid() {
			t.Fatalf("%#v should be invalid", typ)
		}
	}
}

func TestTypeString(t *testing.T) {
	if Int64.String() != "int64" {
		t.Fatalf("Int64.String() = %q", Int64.String())
	}
	if Named("event_status").String() != "event_status" {
		t.Fatalf("Named.String() = %q", Named("event_status").String())
	}
}
