package storage

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestReadSegmentRejectsBadInput(t *testing.T) {
	t.Run("bad magic", func(t *testing.T) {
		_, _, err := ReadSegment(bytes.NewReader([]byte("not-a-segment")))
		if err == nil {
			t.Fatal("expected invalid magic error")
		}
	})

	t.Run("unsupported version", func(t *testing.T) {
		data := []byte(segmentMagic)
		data = binary.LittleEndian.AppendUint16(data, segmentVersion+1)
		data = binary.LittleEndian.AppendUint64(data, 0)
		data = binary.LittleEndian.AppendUint32(data, 0)

		_, _, err := ReadSegment(bytes.NewReader(data))
		if err == nil {
			t.Fatal("expected unsupported version error")
		}
	})

	t.Run("unknown kind", func(t *testing.T) {
		var buf bytes.Buffer
		sw := newSegmentWriter(&buf)

		if err := sw.WriteHeader(1, 1); err != nil {
			t.Fatal(err)
		}

		stats := ColumnStats{Name: "mystery", Kind: vector.Kind(255), Count: 1, EncodedLen: 1}
		if err := sw.WriteColumn(stats, []byte{0}); err != nil {
			t.Fatal(err)
		}

		_, _, err := ReadSegment(bytes.NewReader(buf.Bytes()))
		if err == nil {
			t.Fatal("expected unknown kind error")
		}
	})

	t.Run("huge encoded column", func(t *testing.T) {
		var buf bytes.Buffer
		sw := newSegmentWriter(&buf)

		if err := sw.WriteHeader(1, 1); err != nil {
			t.Fatal(err)
		}
		stats := ColumnStats{Name: "tenant_id", Kind: vector.KindInt64, Count: 1}
		if err := sw.writeColumnHeader(stats, maxEncodedColumnLen+1); err != nil {
			t.Fatal(err)
		}

		_, _, err := ReadSegment(bytes.NewReader(buf.Bytes()))
		if err == nil {
			t.Fatal("expected huge encoded column error")
		}
	})
}

func TestWriteHeaderRejectsInvalidCounts(t *testing.T) {
	var buf bytes.Buffer
	sw := newSegmentWriter(&buf)

	if err := sw.WriteHeader(-1, 1); err == nil {
		t.Fatal("expected negative row count error")
	}
	if err := sw.WriteHeader(1, -1); err == nil {
		t.Fatal("expected negative column count error")
	}
}

func TestReadSegmentRejectsTruncatedInput(t *testing.T) {
	batch := mustBatch(t,
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{1, 2, 3})},
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout", "cancel"})},
	)

	var buf bytes.Buffer
	if _, err := WriteSegment(&buf, batch); err != nil {
		t.Fatal(err)
	}

	data := buf.Bytes()
	for i := range data {
		_, _, err := ReadSegment(bytes.NewReader(data[:i]))
		if err == nil {
			t.Fatalf("expected error for truncated segment length %d", i)
		}

		_, err = ReadSegmentStats(bytes.NewReader(data[:i]))
		if err == nil {
			t.Fatalf("expected stats error for truncated segment length %d", i)
		}
	}
}
