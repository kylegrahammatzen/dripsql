// Pins the sink authority contract for columns whose pages interleave NULLs with live values.
package storage

import (
	"path/filepath"
	"testing"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/vector"
)

// Contract: mixed pages still feed the column sink, so the numeric sum sidecar,
// int filter, dict histogram, and footer stats are exact over valid rows only.
func TestMixedPages_SinkStaysAuthoritative(t *testing.T) {
	makePage := func(xs []int64, xNulls []int, gs []string, gNulls []int) vector.Batch {
		n := len(xs)
		xv := vector.NewVec(vector.VecInt64, n)
		copy(xv.I64(), xs)
		if len(xNulls) > 0 {
			xv.Valid = vector.NewValidity(n)
			for _, r := range xNulls {
				xv.Valid.SetInvalid(r)
			}
		}
		gv := vector.NewVarVec(vector.VecText, n, 0)
		for i, s := range gs {
			gv.Var().AppendString(i, s)
		}
		if len(gNulls) > 0 {
			gv.Valid = vector.NewValidity(n)
			for _, r := range gNulls {
				gv.Valid.SetInvalid(r)
			}
		}
		b, err := vector.NewBatch([]vector.Column{
			{Name: "x", Type: schema.Int64, V: xv},
			{Name: "g", Type: schema.Text, V: gv},
		})
		if err != nil {
			t.Fatalf("NewBatch: %v", err)
		}
		return b
	}

	// Null slots carry poison values that must never surface in stats or sidecars.
	pages := []vector.Batch{
		makePage(
			[]int64{10, 20, 30, 40, 50, 60, 70, 80}, []int{1, 5},
			[]string{"red", "poison", "blue", "red", "poison", "blue", "red", "green"}, []int{1, 4},
		),
		makePage(
			[]int64{100, 200, 300, 400, 500, 600, 700, 800}, []int{0},
			[]string{"red", "green", "green", "blue", "red", "red", "red", "green"}, nil,
		),
	}
	path := filepath.Join(t.TempDir(), "seg.dsv4")
	if _, err := WriteSegment(path, pages, nil); err != nil {
		t.Fatalf("WriteSegment: %v", err)
	}
	seg, err := OpenSegment(path)
	if err != nil {
		t.Fatalf("OpenSegment: %v", err)
	}
	defer seg.Close()

	sums, err := seg.NumericSums()
	if err != nil {
		t.Fatalf("NumericSums: %v", err)
	}
	sum, ok := sums["x"]
	if !ok {
		t.Fatal("numeric sum sidecar missing for mixed-page column x")
	}
	if sum.Sum != 3780 {
		t.Fatalf("sum = %d, want 3780 over valid rows", sum.Sum)
	}

	filters, err := seg.IntFilterSet()
	if err != nil {
		t.Fatalf("IntFilterSet: %v", err)
	}
	f, ok := filters["x"]
	if !ok {
		t.Fatal("int filter sidecar missing for mixed-page column x")
	}
	for _, v := range []int64{10, 30, 40, 50, 70, 80, 200, 300, 400, 500, 600, 700, 800} {
		if !f.Contains(v) {
			t.Fatalf("int filter false negative for valid value %d", v)
		}
	}

	hists, err := seg.DictHistograms()
	if err != nil {
		t.Fatalf("DictHistograms: %v", err)
	}
	h, ok := hists["g"]
	if !ok {
		t.Fatal("dict histogram sidecar missing for mixed-page column g")
	}
	wantHist := DictHistogram{"red": 7, "blue": 3, "green": 4}
	if len(h) != len(wantHist) {
		t.Fatalf("histogram %v, want %v", h, wantHist)
	}
	for k, c := range wantHist {
		if h[k] != c {
			t.Fatalf("histogram[%q] = %d, want %d", k, h[k], c)
		}
	}

	if seg.Cols[0].Name != "x" || seg.Cols[1].Name != "g" {
		t.Fatalf("unexpected column order %q %q", seg.Cols[0].Name, seg.Cols[1].Name)
	}
	xs := UnmarshalNumericStats[int64](seg.Cols[0].Stats[:], true)
	if xs.Min != 10 || xs.Max != 800 {
		t.Fatalf("x stats min=%d max=%d, want 10 and 800", xs.Min, xs.Max)
	}
	gs := UnmarshalVarBytesStats(seg.Cols[1].Stats[:], true)
	if gs.MinLen != 3 || gs.MaxLen != 5 || gs.TotalBytes != 53 {
		t.Fatalf("g stats minLen=%d maxLen=%d total=%d, want 3, 5, 53", gs.MinLen, gs.MaxLen, gs.TotalBytes)
	}
}
