// Segment reader tests: round-trip via WriteSegment, error paths for bad magic /
// truncated footers / out-of-range page reads.
package storage

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

func TestOpenSegment_RoundTrip_IntColumn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	pages := []types.Batch{
		makeIntBatch(t, "id", 0, 100),
		makeIntBatch(t, "id", 100, 100),
	}
	if err := WriteSegment(path, pages); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()
	if len(seg.Cols) != 1 {
		t.Fatalf("col count = %d, want 1", len(seg.Cols))
	}
	if seg.Cols[0].Name != "id" {
		t.Fatalf("col name = %q, want id", seg.Cols[0].Name)
	}
	if seg.Cols[0].Kind != types.VecInt64 {
		t.Fatalf("kind = %v, want Int64", seg.Cols[0].Kind)
	}
	if seg.Cols[0].Rows != 200 {
		t.Fatalf("rows = %d, want 200", seg.Cols[0].Rows)
	}
	if len(seg.Cols[0].Pages) != 2 {
		t.Fatalf("pages = %d, want 2", len(seg.Cols[0].Pages))
	}
	for i := range 2 {
		v, err := seg.ReadPage(0, i)
		if err != nil {
			t.Fatalf("ReadPage(0, %d): %v", i, err)
		}
		if int(v.Len) != 100 {
			t.Fatalf("page %d len = %d, want 100", i, v.Len)
		}
		base := int64(i * 100)
		for j, got := range v.I64() {
			want := base + int64(j)
			if got != want {
				t.Fatalf("page %d row %d: got %d want %d", i, j, got, want)
			}
		}
	}
}

func TestOpenSegment_RoundTrip_TwoColumns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	pages := []types.Batch{
		makeTwoColumnBatch(t, 0, 64),
		makeTwoColumnBatch(t, 64, 64),
	}
	if err := WriteSegment(path, pages); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()

	if got := []string{seg.Cols[0].Name, seg.Cols[1].Name}; got[0] != "id" || got[1] != "tag" {
		t.Fatalf("col names = %v, want [id tag]", got)
	}
	idVec, err := seg.ReadPage(0, 1)
	if err != nil {
		t.Fatalf("ReadPage(id, 1): %v", err)
	}
	if idVec.I64()[0] != 64 {
		t.Fatalf("id page 1 row 0 = %d, want 64", idVec.I64()[0])
	}
	tagVec, err := seg.ReadPage(1, 0)
	if err != nil {
		t.Fatalf("ReadPage(tag, 0): %v", err)
	}
	if got := string(tagVec.Var().Bytes(0)); got != "hello" {
		t.Fatalf("tag page 0 row 0 = %q, want hello", got)
	}
}

func TestOpenSegment_MissingFileErrors(t *testing.T) {
	if _, err := OpenSegment(filepath.Join(t.TempDir(), "nope.dsv4")); err == nil {
		t.Fatal("OpenSegment must error on missing file")
	}
}

func TestOpenSegment_TooSmallErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tiny.dsv4")
	if err := os.WriteFile(path, []byte("DRIP"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := OpenSegment(path); err == nil {
		t.Fatal("OpenSegment must reject too-small files")
	}
}

func TestOpenSegment_BadTailMagicErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if err := WriteSegment(path, []types.Batch{makeIntBatch(t, "id", 0, 5)}); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	data, _ := os.ReadFile(path)
	data[len(data)-1] = 'X'
	_ = os.WriteFile(path, data, 0o644)
	if _, err := OpenSegment(path); err == nil {
		t.Fatal("OpenSegment must reject bad tail magic")
	}
}

func TestOpenSegment_BadHeadMagicErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if err := WriteSegment(path, []types.Batch{makeIntBatch(t, "id", 0, 5)}); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	data, _ := os.ReadFile(path)
	data[0] = 'X'
	_ = os.WriteFile(path, data, 0o644)
	if _, err := OpenSegment(path); err == nil {
		t.Fatal("OpenSegment must reject bad head magic")
	}
}

func TestSegment_ReadPage_OutOfRange(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if err := WriteSegment(path, []types.Batch{makeIntBatch(t, "id", 0, 10)}); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()
	if _, err := seg.ReadPage(5, 0); err == nil {
		t.Fatal("ReadPage must reject col index out of range")
	}
	if _, err := seg.ReadPage(0, 7); err == nil {
		t.Fatal("ReadPage must reject page index out of range")
	}
}

func TestSegment_StatsMarshaledIntoFooter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	pages := []types.Batch{makeIntBatch(t, "id", 5, 100)}
	if err := WriteSegment(path, pages); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()
	got := UnmarshalNumericStats[int64](seg.Cols[0].Stats[:], true)
	if got.Min != 5 || got.Max != 104 {
		t.Fatalf("stats min/max = %d/%d, want 5/104", got.Min, got.Max)
	}
	if !bytes.Equal(seg.Cols[0].Stats[:8], encodeI64LE(5)) {
		t.Fatal("stats bytes 0-7 should be int64 min")
	}
}

func encodeI64LE(v int64) []byte {
	out := make([]byte, 8)
	for i := range 8 {
		out[i] = byte(uint64(v) >> uint(i*8))
	}
	return out
}

func TestParseFooter_RejectsAbsurdColumnCount(t *testing.T) {
	body := make([]byte, 100)
	binary.LittleEndian.PutUint32(body[0:4], 1_000_000)
	if _, err := parseFooter(body); err == nil {
		t.Fatal("parseFooter must reject column count that cannot fit body length")
	}
}

func TestParseFooter_RejectsAbsurdLabelCount(t *testing.T) {
	w := newWireBuffer(64)
	w.U32(1) // 1 column
	w.LenPrefixedString("x")
	w.U8(byte(types.VecText))
	w.U8(0)
	w.U32(1_000_000) // huge label count
	if _, err := parseFooter(w.Bytes()); err == nil {
		t.Fatal("parseFooter must reject label count that cannot fit remaining body")
	}
}

func TestParseFooter_RejectsTrailingBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if err := WriteSegment(path, []types.Batch{makeIntBatch(t, "id", 0, 5)}); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	data, _ := os.ReadFile(path)
	footerLen, _ := ReadFooterSuffix(data[len(data)-FooterSuffixSize:])
	footerStart := len(data) - FooterSuffixSize - int(footerLen)
	body := append([]byte{}, data[footerStart:len(data)-FooterSuffixSize]...)
	body = append(body, 0, 0, 0)
	if _, err := parseFooter(body); err == nil {
		t.Fatal("parseFooter must reject trailing bytes after canonical footer")
	}
}

func TestOpenSegment_RejectsCorruptPageOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if err := WriteSegment(path, []types.Batch{makeIntBatch(t, "id", 0, 5)}); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	data, _ := os.ReadFile(path)
	footerLen, _ := ReadFooterSuffix(data[len(data)-FooterSuffixSize:])
	footerStart := len(data) - FooterSuffixSize - int(footerLen)
	pageDirOff := footerStart + int(footerLen) - PageEntrySize
	binary.LittleEndian.PutUint64(data[pageDirOff+0:pageDirOff+8], 1<<40)
	_ = os.WriteFile(path, data, 0o644)
	if _, err := OpenSegment(path); err == nil {
		t.Fatal("OpenSegment must reject page payload offset past bodyEnd")
	}
}

func TestOpenSegment_RejectsPageFlagAllNull(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if err := WriteSegment(path, []types.Batch{makeIntBatch(t, "id", 0, 5)}); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	data, _ := os.ReadFile(path)
	footerLen, _ := ReadFooterSuffix(data[len(data)-FooterSuffixSize:])
	footerStart := len(data) - FooterSuffixSize - int(footerLen)
	// Footer ends with [page-entries...][page-stats...]; single column with one page means
	// the page entry is at footerEnd - 1*StatsWireSize - PageEntrySize.
	pageDirOff := footerStart + int(footerLen) - StatsWireSize - PageEntrySize
	data[pageDirOff+30] = PageFlagAllValid | PageFlagAllNull
	_ = os.WriteFile(path, data, 0o644)
	if _, err := OpenSegment(path); err == nil {
		t.Fatal("OpenSegment must reject pages with PageFlagAllNull until null support lands")
	}
}
