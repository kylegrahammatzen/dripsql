// Segment reader tests covering round-trip via WriteSegment, error paths for bad magic, truncated footers, out-of-range page reads, the identity sidecar codec, and the ReadTs commit visibility filter.
package storage

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

func TestOpenSegment_RoundTrip_IntColumn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	pages := []vector.Batch{
		makeIntBatch(t, "id", 0, 100),
		makeIntBatch(t, "id", 100, 100),
	}
	if _, err := WriteSegment(path, pages, nil); err != nil {
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
	if seg.Cols[0].Kind != vector.VecInt64 {
		t.Fatalf("kind = %v, want Int64", seg.Cols[0].Kind)
	}
	if seg.Cols[0].Rows != 200 {
		t.Fatalf("rows = %d, want 200", seg.Cols[0].Rows)
	}
	if len(seg.Cols[0].Pages) != 2 {
		t.Fatalf("pages = %d, want 2", len(seg.Cols[0].Pages))
	}
	for i := range 2 {
		v, _, err := seg.ReadPageInto(0, i, nil)
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
	pages := []vector.Batch{
		makeTwoColumnBatch(t, 0, 64),
		makeTwoColumnBatch(t, 64, 64),
	}
	if _, err := WriteSegment(path, pages, nil); err != nil {
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
	idVec, _, err := seg.ReadPageInto(0, 1, nil)
	if err != nil {
		t.Fatalf("ReadPage(id, 1): %v", err)
	}
	if idVec.I64()[0] != 64 {
		t.Fatalf("id page 1 row 0 = %d, want 64", idVec.I64()[0])
	}
	tagVec, _, err := seg.ReadPageInto(1, 0, nil)
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
	if _, err := WriteSegment(path, []vector.Batch{makeIntBatch(t, "id", 0, 5)}, nil); err != nil {
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
	if _, err := WriteSegment(path, []vector.Batch{makeIntBatch(t, "id", 0, 5)}, nil); err != nil {
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
	if _, err := WriteSegment(path, []vector.Batch{makeIntBatch(t, "id", 0, 10)}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()
	if _, _, err := seg.ReadPageInto(5, 0, nil); err == nil {
		t.Fatal("ReadPage must reject col index out of range")
	}
	if _, _, err := seg.ReadPageInto(0, 7, nil); err == nil {
		t.Fatal("ReadPage must reject page index out of range")
	}
}

func TestSegment_StatsMarshaledIntoFooter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	pages := []vector.Batch{makeIntBatch(t, "id", 5, 100)}
	if _, err := WriteSegment(path, pages, nil); err != nil {
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
	w.U8(byte(vector.VecText))
	w.U8(0)
	w.U32(1_000_000) // huge label count
	if _, err := parseFooter(w.Bytes()); err == nil {
		t.Fatal("parseFooter must reject label count that cannot fit remaining body")
	}
}

func TestParseFooter_RejectsTrailingBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if _, err := WriteSegment(path, []vector.Batch{makeIntBatch(t, "id", 0, 5)}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	data, _ := os.ReadFile(path)
	_, footerLen, sidecarLen, _ := ReadFooterSuffix(data[len(data)-FooterSuffixSize:])
	footerStart := len(data) - FooterSuffixSize - int(sidecarLen) - int(footerLen)
	body := append([]byte{}, data[footerStart:footerStart+int(footerLen)]...)
	body = append(body, 0, 0, 0)
	if _, err := parseFooter(body); err == nil {
		t.Fatal("parseFooter must reject trailing bytes after canonical footer")
	}
}

func TestOpenSegment_RejectsCorruptPageOffset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if _, err := WriteSegment(path, []vector.Batch{makeIntBatch(t, "id", 0, 5)}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	data, _ := os.ReadFile(path)
	_, footerLen, sidecarLen, _ := ReadFooterSuffix(data[len(data)-FooterSuffixSize:])
	footerStart := len(data) - FooterSuffixSize - int(sidecarLen) - int(footerLen)
	pageDirOff := footerStart + int(footerLen) - PageEntrySize
	binary.LittleEndian.PutUint64(data[pageDirOff+0:pageDirOff+8], 1<<40)
	_ = os.WriteFile(path, data, 0o644)
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment must not fail before lazy validation: %v", err)
	}
	defer seg.Close()
	if err := seg.ValidateColumns(); err == nil {
		t.Fatal("ValidateColumns must reject page payload offset past bodyEnd")
	}
}

func TestOpenSegment_RejectsPageFlagAllNull(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if _, err := WriteSegment(path, []vector.Batch{makeIntBatch(t, "id", 0, 5)}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	data, _ := os.ReadFile(path)
	_, footerLen, sidecarLen, _ := ReadFooterSuffix(data[len(data)-FooterSuffixSize:])
	footerStart := len(data) - FooterSuffixSize - int(sidecarLen) - int(footerLen)
	// Footer ends with [page-entries...][page-stats...]; single column with one page means
	// the page entry is at footerEnd - 1*StatsWireSize - PageEntrySize.
	pageDirOff := footerStart + int(footerLen) - StatsWireSize - PageEntrySize
	data[pageDirOff+30] = PageFlagAllValid | PageFlagAllNull
	_ = os.WriteFile(path, data, 0o644)
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment must not fail before lazy validation: %v", err)
	}
	defer seg.Close()
	if err := seg.ValidateColumns(); err == nil {
		t.Fatal("ValidateColumns must reject pages with both AllValid and AllNull set")
	}
}

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

func TestSegmentIdentity_RoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	id := SegmentIdentity{
		TableID:          7,
		SchemaGeneration: 13,
		ColumnIDs:        []uint64{101},
	}
	if _, err := WriteSegmentWithIdentity(path, []vector.Batch{makeIntBatch(t, "id", 0, 4)}, nil, id); err != nil {
		t.Fatalf("WriteSegmentWithIdentity: %v", err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()
	if seg.TableID != id.TableID || seg.SchemaGeneration != id.SchemaGeneration {
		t.Fatalf("segment identity: got TableID=%d gen=%d, want %d/%d", seg.TableID, seg.SchemaGeneration, id.TableID, id.SchemaGeneration)
	}
	if len(seg.Cols) != 1 || seg.Cols[0].ColumnID != id.ColumnIDs[0] {
		t.Fatalf("column identity: got %+v, want ColumnID=%d", seg.Cols, id.ColumnIDs[0])
	}
}

func TestSegmentIdentity_AbsentOnLegacyWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if _, err := WriteSegment(path, []vector.Batch{makeIntBatch(t, "id", 0, 4)}, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()
	if seg.TableID != 0 || seg.SchemaGeneration != 0 {
		t.Fatalf("legacy segment must carry zero identity, got TableID=%d gen=%d", seg.TableID, seg.SchemaGeneration)
	}
	if seg.Cols[0].ColumnID != 0 {
		t.Fatalf("legacy column must carry zero ColumnID, got %d", seg.Cols[0].ColumnID)
	}
}

func TestSegmentIdentity_RejectsColumnCountMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	// Two ids but the batch only writes one column. The reader must refuse to silently pad-or-truncate.
	id := SegmentIdentity{
		TableID:          1,
		SchemaGeneration: 1,
		ColumnIDs:        []uint64{1, 2},
	}
	if _, err := WriteSegmentWithIdentity(path, []vector.Batch{makeIntBatch(t, "id", 0, 4)}, nil, id); err != nil {
		t.Fatalf("WriteSegmentWithIdentity: %v", err)
	}
	if _, err := OpenSegment(path); err == nil {
		t.Fatal("expected error on identity column count mismatch")
	}
}

// makeTrustProbeBatch builds one page with a fully valid int column, a mixed-null
// int column, and a mixed-null text column so sink trust can diverge per column.
func makeTrustProbeBatch(t *testing.T, start int64) vector.Batch {
	t.Helper()
	const n = 8
	id := vector.NewVec(vector.VecInt64, n)
	for i := range id.I64() {
		id.I64()[i] = start + int64(i)
	}
	x := vector.NewVec(vector.VecInt64, n)
	xValid := vector.NewValidity(n)
	for i := range x.I64() {
		x.I64()[i] = 1000 + start + int64(i)
		if i%2 == 1 {
			xValid.SetInvalid(i)
		}
	}
	x.Valid = xValid
	g := vector.NewVarVec(vector.VecText, n, 0)
	gValid := vector.NewValidity(n)
	gValid.SetInvalid(0)
	for i := range n {
		if i%2 == 0 {
			g.Var().AppendString(i, "a")
		} else {
			g.Var().AppendString(i, "b")
		}
	}
	g.Valid = gValid
	b, err := vector.NewBatch([]vector.Column{
		{Name: "id", Type: schema.Int64, V: id},
		{Name: "x", Type: schema.Int64, V: x},
		{Name: "g", Type: schema.Text, V: g},
	})
	if err != nil {
		t.Fatalf("NewBatch: %v", err)
	}
	return b
}

// Contract: a freshly written segment reopens as format version 1 with every
// column trusted, exact stats on mixed columns, and all sidecar sections served.
func TestOpenSegment_V4RoundTripAllTrusted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	pages := []vector.Batch{makeTrustProbeBatch(t, 0), makeTrustProbeBatch(t, 8)}
	if _, err := WriteSegment(path, pages, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()
	if seg.FormatVersion != SegmentFormatVersion {
		t.Fatalf("FormatVersion = %d, want %d", seg.FormatVersion, SegmentFormatVersion)
	}
	for i := range seg.Cols {
		if !seg.Cols[i].SinkTrusted {
			t.Fatalf("col %q untrusted on a v4 file", seg.Cols[i].Name)
		}
	}
	xIdx, _ := findSegmentColumnIdx(seg, "x")
	stats, present := numericStatsFromCol(&seg.Cols[xIdx])
	if !present || !stats.HasNonNull || stats.Min != 1000 || stats.Max != 1014 {
		t.Fatalf("x stats = %+v present=%v, want exact min 1000 max 1014", stats, present)
	}
	sums, err := seg.NumericSums()
	if err != nil {
		t.Fatalf("NumericSums: %v", err)
	}
	if sums["x"].Sum != 8056 || sums["id"].Sum != 120 {
		t.Fatalf("sums = %v, want x 8056 id 120", sums)
	}
	hists, err := seg.DictHistograms()
	if err != nil {
		t.Fatalf("DictHistograms: %v", err)
	}
	if hists["g"]["a"] != 6 || hists["g"]["b"] != 8 {
		t.Fatalf("g histogram = %v, want a 6 b 8", hists["g"])
	}
	gs, err := seg.GroupSums()
	if err != nil {
		t.Fatalf("GroupSums: %v", err)
	}
	if _, ok := gs["g"]["id"]; !ok {
		t.Fatalf("group sums = %v, want pair g/id", gs)
	}
}

// Contract: a legacy v3 suffix reads as format version 0, mixed-page columns lose
// sink trust with stats absent and sidecar sections dropped, and fully valid
// columns in the same file keep exact stats and sidecars. Pins new behavior, no
// fail-before exists since v3 discrimination did not exist.
func TestOpenSegment_V3LegacySuffixSoftDisablesMixedColumns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	pages := []vector.Batch{makeTrustProbeBatch(t, 0), makeTrustProbeBatch(t, 8)}
	if _, err := WriteSegment(path, pages, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	_, footerLen, sidecarLen, err := ReadFooterSuffix(data[len(data)-FooterSuffixSize:])
	if err != nil {
		t.Fatalf("ReadFooterSuffix: %v", err)
	}
	// Private copy of the legacy 24-byte suffix encoding, [u64 sidecarLen][u64 footerLen][magic].
	legacy := make([]byte, FooterSuffixV3Size)
	binary.LittleEndian.PutUint64(legacy[0:8], sidecarLen)
	binary.LittleEndian.PutUint64(legacy[8:16], footerLen)
	copy(legacy[16:24], MagicV3)
	data = append(data[:len(data)-FooterSuffixSize], legacy...)
	copy(data[0:MagicLen], MagicV3)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()
	if seg.FormatVersion != 0 {
		t.Fatalf("FormatVersion = %d, want 0", seg.FormatVersion)
	}
	xIdx, _ := findSegmentColumnIdx(seg, "x")
	gIdx, _ := findSegmentColumnIdx(seg, "g")
	idIdx, _ := findSegmentColumnIdx(seg, "id")
	if seg.Cols[xIdx].SinkTrusted || seg.Cols[gIdx].SinkTrusted {
		t.Fatal("mixed-page columns must lose sink trust on v3 files")
	}
	if !seg.Cols[idIdx].SinkTrusted {
		t.Fatal("fully valid column must stay trusted on v3 files")
	}
	if stats, _ := numericStatsFromCol(&seg.Cols[xIdx]); stats.HasNonNull {
		t.Fatalf("untrusted x stats must read absent, got %+v", stats)
	}
	idStats, present := numericStatsFromCol(&seg.Cols[idIdx])
	if !present || !idStats.HasNonNull || idStats.Min != 0 || idStats.Max != 15 {
		t.Fatalf("id stats = %+v present=%v, want exact min 0 max 15", idStats, present)
	}
	sums, err := seg.NumericSums()
	if err != nil {
		t.Fatalf("NumericSums: %v", err)
	}
	if _, ok := sums["x"]; ok {
		t.Fatal("untrusted x must be dropped from numeric sums")
	}
	if sums["id"].Sum != 120 {
		t.Fatalf("trusted id sum = %d, want 120", sums["id"].Sum)
	}
	filters, err := seg.IntFilterSet()
	if err != nil {
		t.Fatalf("IntFilterSet: %v", err)
	}
	if _, ok := filters["x"]; ok {
		t.Fatal("untrusted x must be dropped from int filters")
	}
	if _, ok := filters["id"]; !ok {
		t.Fatal("trusted id must keep its int filter")
	}
	hists, err := seg.DictHistograms()
	if err != nil {
		t.Fatalf("DictHistograms: %v", err)
	}
	if _, ok := hists["g"]; ok {
		t.Fatal("untrusted g must be dropped from dict histograms")
	}
	blooms, err := seg.VarBlooms()
	if err != nil {
		t.Fatalf("VarBlooms: %v", err)
	}
	if _, ok := blooms["g"]; ok {
		t.Fatal("untrusted g must be dropped from var blooms")
	}
	gs, err := seg.GroupSums()
	if err != nil {
		t.Fatalf("GroupSums: %v", err)
	}
	if _, ok := gs["g"]; ok {
		t.Fatal("group sums pair must drop when the group column is untrusted")
	}
}

func TestScanOpts_ReadTs_SkipsNewerSegments(t *testing.T) {
	tmp := t.TempDir()
	mk := func(name string, val int64) *Segment {
		path := filepath.Join(tmp, name)
		v := vector.NewVec(vector.VecInt64, 1)
		v.I64()[0] = val
		batch, _ := vector.NewBatch([]vector.Column{{Name: "id", Type: schema.Int64, V: v}})
		if _, err := WriteSegment(path, []vector.Batch{batch}, nil); err != nil {
			t.Fatalf("WriteSegment: %v", err)
		}
		s, err := OpenSegment(path)
		if err != nil {
			t.Fatalf("OpenSegment: %v", err)
		}
		return s
	}
	older := mk("older.dsv4", 10)
	older.CommitTs = 5
	newer := mk("newer.dsv4", 20)
	newer.CommitTs = 15
	defer older.Close()
	defer newer.Close()

	type row struct {
		commitTs uint64
		val      int64
	}
	collect := func(readTs uint64) []row {
		var got []row
		err := Scan(ScanOpts{
			Segments: []*Segment{older, newer},
			ReadTs:   readTs,
		}, func(batch vector.Batch, sel *vector.SelectionMask) error {
			col, _ := batch.ColumnByName("id")
			sel.IterSet(func(r int) {
				got = append(got, row{val: col.V.I64()[r]})
			})
			return nil
		})
		if err != nil {
			t.Fatalf("Scan(readTs=%d): %v", readTs, err)
		}
		return got
	}

	if got := collect(100); len(got) != 2 {
		t.Errorf("ReadTs=100 returned %d rows, want 2", len(got))
	}
	if got := collect(10); len(got) != 1 || got[0].val != 10 {
		t.Errorf("ReadTs=10 returned %v, want [{val:10}]", got)
	}
	if got := collect(1); len(got) != 0 {
		t.Errorf("ReadTs=1 returned %v, want []", got)
	}
	// ReadTs == 0 is the sentinel for no cutoff and preserves pre-MVCC behavior.
	if got := collect(0); len(got) != 2 {
		t.Errorf("ReadTs=0 returned %d rows, want 2 (no cutoff)", len(got))
	}
}
