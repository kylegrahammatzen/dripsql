package storage

import (
	"bytes"
	"encoding/binary"
	"strings"
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
		data = binary.LittleEndian.AppendUint64(data, uint64(segmentHeaderLen+footerTrailerLen))

		_, _, err := ReadSegment(bytes.NewReader(data))
		if err == nil {
			t.Fatal("expected unsupported version error")
		}
	})

	t.Run("unknown kind", func(t *testing.T) {
		var buf bytes.Buffer
		sw := newSegmentWriter(&buf)

		if err := sw.WriteHeader(1, 1, uint64(segmentHeaderLen+64)); err != nil {
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

		segmentLen := uint64(segmentHeaderLen+2+len("tenant_id")+columnFixedHeaderLen) + maxEncodedColumnLen + 1 + 4 + uint64(footerTrailerLen)
		if err := sw.WriteHeader(1, 1, segmentLen); err != nil {
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

	if err := sw.WriteHeader(-1, 1, uint64(segmentHeaderLen+footerTrailerLen)); err == nil {
		t.Fatal("expected negative row count error")
	}
	if err := sw.WriteHeader(1, -1, uint64(segmentHeaderLen+footerTrailerLen)); err == nil {
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

func TestReadSegmentColumnsRejectsFooterOffsetNameMismatch(t *testing.T) {
	data := writeSegmentBytes(t,
		vector.Column{Name: "event_type", Vector: vector.NewString([]string{"signup", "checkout"})},
		vector.Column{Name: "tenant_id", Vector: vector.NewInt64([]int64{7, 42})},
	)

	eventOffset := footerColumnOffset(t, data, "event_type")
	data = replaceFooterColumnOffset(t, data, "tenant_id", eventOffset)

	_, _, err := ReadSegmentColumns(bytes.NewReader(data), "tenant_id")
	if err == nil {
		t.Fatal("expected footer offset mismatch error")
	}
	if !strings.Contains(err.Error(), `footer offset for "tenant_id" points to column "event_type"`) {
		t.Fatalf("error = %q, want footer offset mismatch", err)
	}
}

func footerColumnOffset(t testing.TB, data []byte, column string) uint64 {
	t.Helper()

	_, _, offsetPos := footerColumnEntry(t, data, column)
	return binary.LittleEndian.Uint64(data[offsetPos:])
}

func replaceFooterColumnOffset(t testing.TB, data []byte, column string, offset uint64) []byte {
	t.Helper()

	copyData := append([]byte(nil), data...)
	_, _, offsetPos := footerColumnEntry(t, copyData, column)
	binary.LittleEndian.PutUint64(copyData[offsetPos:], offset)
	return copyData
}

func footerColumnEntry(t testing.TB, data []byte, column string) (footerStart int, entryName string, offsetPos int) {
	t.Helper()

	if len(data) < footerTrailerLen {
		t.Fatalf("segment len = %d, want footer trailer", len(data))
	}
	trailerStart := len(data) - footerTrailerLen
	footerLen := int(binary.LittleEndian.Uint64(data[trailerStart:]))
	footerStart = trailerStart - footerLen
	if footerStart < segmentHeaderLen {
		t.Fatalf("footer start = %d, want at least %d", footerStart, segmentHeaderLen)
	}

	offset := footerStart
	count := int(binary.LittleEndian.Uint32(data[offset:]))
	offset += 4
	for range count {
		nameLen := int(binary.LittleEndian.Uint16(data[offset:]))
		offset += 2
		entryName = string(data[offset : offset+nameLen])
		offset += nameLen
		offsetPos = offset
		offset += 8
		if entryName == column {
			return footerStart, entryName, offsetPos
		}
	}
	t.Fatalf("missing footer entry for %q", column)
	return 0, "", 0
}
