// Segment writer smoke tests: column-major payload ordering, footer suffix magic,
// and per-column page entries in the footer page directory.
package storage

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func makeIntBatch(t *testing.T, name string, start, n int64) vector.Batch {
	t.Helper()
	v := vector.NewVec(vector.VecInt64, int(n))
	for i := range v.I64() {
		v.I64()[i] = start + int64(i)
	}
	b, err := vector.NewBatch([]vector.Column{{Name: name, Type: schema.Int64, V: v}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return b
}

func makeTwoColumnBatch(t *testing.T, start, n int64) vector.Batch {
	t.Helper()
	v1 := vector.NewVec(vector.VecInt64, int(n))
	for i := range v1.I64() {
		v1.I64()[i] = start + int64(i)
	}
	v2 := vector.NewVarVec(vector.VecText, int(n), 0)
	vb := v2.Var()
	for i := range int(n) {
		vb.AppendString(i, "hello")
	}
	b, err := vector.NewBatch([]vector.Column{
		{Name: "id", Type: schema.Int64, V: v1},
		{Name: "tag", Type: schema.Text, V: v2},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return b
}

func TestWriteSegment_FileExistsAndStartsWithMagic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if _, err := WriteSegment(path, []vector.Batch{makeIntBatch(t, "id", 0, 100)}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(data[:MagicLen]) != Magic {
		t.Fatalf("head magic %q != %q", data[:MagicLen], Magic)
	}
	if string(data[len(data)-MagicLen:]) != Magic {
		t.Fatalf("tail magic mismatch")
	}
}

func TestWriteSegment_FooterSuffixDecodes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if _, err := WriteSegment(path, []vector.Batch{makeIntBatch(t, "id", 0, 50)}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	data, _ := os.ReadFile(path)
	footerLen, _, ok := ReadFooterSuffix(data[len(data)-FooterSuffixSize:])
	if !ok {
		t.Fatal("footer magic mismatch")
	}
	if footerLen == 0 || footerLen >= uint64(len(data)) {
		t.Fatalf("absurd footer length %d for %d-byte file", footerLen, len(data))
	}
}

func TestWriteSegment_ColumnMajorPagesContiguous(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	pages := []vector.Batch{
		makeTwoColumnBatch(t, 0, 100),
		makeTwoColumnBatch(t, 100, 100),
		makeTwoColumnBatch(t, 200, 100),
	}
	if _, err := WriteSegment(path, pages, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	data, _ := os.ReadFile(path)
	footerLen, sidecarLen, _ := ReadFooterSuffix(data[len(data)-FooterSuffixSize:])
	footerStart := uint64(len(data)) - FooterSuffixSize - sidecarLen - footerLen
	footer := data[footerStart : footerStart+footerLen]

	colCount := binary.LittleEndian.Uint32(footer[0:4])
	if colCount != 2 {
		t.Fatalf("column count = %d, want 2", colCount)
	}

	pos := 4
	pageEntryOffsets := make([]int, 0, 2)
	pageCounts := make([]uint32, 0, 2)
	for range int(colCount) {
		nameLen := int(binary.LittleEndian.Uint16(footer[pos : pos+2]))
		pos += 2 + nameLen
		pos += 2 // kind + encoding_flags
		labelCount := binary.LittleEndian.Uint32(footer[pos : pos+4])
		pos += 4
		for range int(labelCount) {
			llen := int(binary.LittleEndian.Uint16(footer[pos : pos+2]))
			pos += 2 + llen
		}
		pos += 9 // rows + null_count + marker
		pos += StatsWireSize
		pos += 16 // heavy_blob offset + length
		pageCounts = append(pageCounts, binary.LittleEndian.Uint32(footer[pos:pos+4]))
		pos += 4
		pageEntryOffsets = append(pageEntryOffsets, pos)
	}
	if pageCounts[0] != 3 || pageCounts[1] != 3 {
		t.Fatalf("page counts %v, want [3 3]", pageCounts)
	}

	pageDirStart := pos
	col0Pages := make([]Page, 3)
	col1Pages := make([]Page, 3)
	for i := range 3 {
		col0Pages[i] = DecodePageEntry(footer[pageDirStart+i*PageEntrySize : pageDirStart+(i+1)*PageEntrySize])
	}
	for i := range 3 {
		col1Pages[i] = DecodePageEntry(footer[pageDirStart+(3+i)*PageEntrySize : pageDirStart+(3+i+1)*PageEntrySize])
	}

	for i := range 2 {
		if col0Pages[i+1].PayloadOffset != col0Pages[i].PayloadOffset+col0Pages[i].PayloadLength {
			t.Fatalf("col0 page %d not contiguous with %d", i+1, i)
		}
	}
	if col1Pages[0].PayloadOffset != col0Pages[2].PayloadOffset+col0Pages[2].PayloadLength {
		t.Fatalf("col1 page 0 should follow col0 page 2 (column-major)")
	}
	for i := range 2 {
		if col1Pages[i+1].PayloadOffset != col1Pages[i].PayloadOffset+col1Pages[i].PayloadLength {
			t.Fatalf("col1 page %d not contiguous with %d", i+1, i)
		}
	}

	for i, p := range col0Pages {
		if p.RowStart != uint32(i*100) || p.Rows != 100 {
			t.Fatalf("col0 page %d: RowStart=%d Rows=%d", i, p.RowStart, p.Rows)
		}
	}
}

func TestWriteSegment_NullableRoundTrip(t *testing.T) {
	v := vector.NewVec(vector.VecInt64, 6)
	for i := range v.I64() {
		v.I64()[i] = int64(100 + i)
	}
	valid := make(vector.Validity, vector.ValidityWords(6))
	valid.SetValid(0)
	valid.SetValid(2)
	valid.SetValid(4)
	v.Valid = valid
	b, err := vector.NewBatch([]vector.Column{{Name: "x", Type: schema.Int64, V: v}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	path := filepath.Join(t.TempDir(), "nullable.dsv4")
	if _, err := WriteSegment(path, []vector.Batch{b}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()
	if seg.Cols[0].NullCount != 3 {
		t.Fatalf("column null count = %d, want 3", seg.Cols[0].NullCount)
	}
	got, _, err := seg.ReadPageInto(0, 0, nil)
	if err != nil {
		t.Fatalf("ReadPage: %v", err)
	}
	if got.Valid == nil {
		t.Fatal("decoded Vec.Valid is nil but null count > 0")
	}
	for i := range 6 {
		wantValid := i%2 == 0
		if got.Valid.IsValid(i) != wantValid {
			t.Fatalf("row %d valid = %v, want %v", i, got.Valid.IsValid(i), wantValid)
		}
		if wantValid && got.I64()[i] != int64(100+i) {
			t.Fatalf("row %d value = %d, want %d", i, got.I64()[i], 100+i)
		}
	}
}

func TestWriteSegment_AllNullRoundTrip(t *testing.T) {
	v := vector.NewVec(vector.VecInt64, 4)
	v.Valid = make(vector.Validity, vector.ValidityWords(4))
	b, err := vector.NewBatch([]vector.Column{{Name: "x", Type: schema.Int64, V: v}})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	path := filepath.Join(t.TempDir(), "allnull.dsv4")
	if _, err := WriteSegment(path, []vector.Batch{b}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()
	if seg.Cols[0].Marker != columnMarkerAllNull {
		t.Fatalf("column marker = %d, want AllNull", seg.Cols[0].Marker)
	}
	got, _, err := seg.ReadPageInto(0, 0, nil)
	if err != nil {
		t.Fatalf("ReadPage: %v", err)
	}
	for i := range 4 {
		if got.Valid != nil && got.Valid.IsValid(i) {
			t.Fatalf("row %d should be null", i)
		}
	}
}

func TestWriteSegment_RejectsEmpty(t *testing.T) {
	if _, err := WriteSegment(filepath.Join(t.TempDir(), "x"), nil, nil); err == nil {
		t.Fatal("WriteSegment must reject empty page list")
	}
}

func TestWriteSegment_RejectsMismatchedBatchLen(t *testing.T) {
	v := vector.NewVec(vector.VecInt64, 10)
	bad := vector.Batch{Len: 20, Columns: []vector.Column{{Name: "id", Type: schema.Int64, V: v}}}
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if _, err := WriteSegment(path, []vector.Batch{bad}, nil); err == nil {
		t.Fatal("WriteSegment must reject Batch.Len != Vec.Len")
	}
}

func TestWriteSegment_RejectsTypeDrift(t *testing.T) {
	first := makeIntBatch(t, "id", 0, 10)
	v := vector.NewVec(vector.VecInt32, 10)
	second := vector.Batch{Len: 10, Columns: []vector.Column{{Name: "id", Type: schema.Int32, V: v}}}
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if _, err := WriteSegment(path, []vector.Batch{first, second}, nil); err == nil {
		t.Fatal("WriteSegment must reject type drift across batches")
	}
}

func TestWriteSegment_RejectsKindMismatchVsType(t *testing.T) {
	v := vector.NewVec(vector.VecInt32, 5)
	bad := vector.Batch{Len: 5, Columns: []vector.Column{{Name: "id", Type: schema.Int64, V: v}}}
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if _, err := WriteSegment(path, []vector.Batch{bad}, nil); err == nil {
		t.Fatal("WriteSegment must reject Vec.Kind that does not match Column.Type")
	}
}

func TestWriteSegment_AtomicNoFinalOnFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.dsv4")
	// Drop a stale tmp file to confirm it gets cleaned up regardless.
	if err := os.WriteFile(path+".tmp", []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	// Force a failure via Vec.Kind that does not match the declared Type.
	v := vector.NewVec(vector.VecInt32, 4)
	bad := vector.Batch{Len: 4, Columns: []vector.Column{{Name: "id", Type: schema.Int64, V: v}}}
	if _, err := WriteSegment(path, []vector.Batch{bad}, nil); err == nil {
		t.Fatal("WriteSegment must reject kind mismatch")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("final segment file must not exist after failed write, got err=%v", err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("stale tmp must be cleaned up, got err=%v", err)
	}
}

func TestWriteSegment_AtomicRenamesTmpToFinal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.dsv4")
	if _, err := WriteSegment(path, []vector.Batch{makeIntBatch(t, "id", 0, 16)}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("final segment file missing after success: %v", err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("tmp file must not linger after success, got err=%v", err)
	}
}

func TestWriteSegment_RejectsEnumLabelDrift(t *testing.T) {
	v1 := vector.NewVec(vector.VecEnum32, 4)
	v2 := vector.NewVec(vector.VecEnum32, 4)
	first := vector.Batch{Len: 4, Columns: []vector.Column{
		{Name: "k", Type: schema.Named("status"), EnumLabels: []string{"a", "b"}, V: v1},
	}}
	second := vector.Batch{Len: 4, Columns: []vector.Column{
		{Name: "k", Type: schema.Named("status"), EnumLabels: []string{"a", "b", "c"}, V: v2},
	}}
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if _, err := WriteSegment(path, []vector.Batch{first, second}, nil); err == nil {
		t.Fatal("WriteSegment must reject enum-label drift across batches")
	}
}
