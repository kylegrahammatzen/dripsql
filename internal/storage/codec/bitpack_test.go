// Bitpack round-trip tests: pack-unpack identity across widths and row counts,
// edge widths (1, 64), and non-block-aligned row counts.
package codec

import (
	"math/rand/v2"
	"testing"
)

func TestBitpack_RoundTrip_AllWidths(t *testing.T) {
	rows := 2048
	rng := rand.New(rand.NewPCG(1, 2))
	for width := 1; width <= 64; width++ {
		src := make([]uint64, rows)
		mask := uint64(1)<<uint(width) - 1
		if width == 64 {
			mask = ^uint64(0)
		}
		for i := range src {
			src[i] = rng.Uint64() & mask
		}
		dst := make([]byte, PackedSize(rows, width))
		Pack(width, src, dst)
		out := make([]uint64, rows)
		Unpack(width, dst, rows, out)
		for i := range src {
			if out[i] != src[i] {
				t.Fatalf("width=%d row=%d: got %d want %d", width, i, out[i], src[i])
			}
		}
	}
}

func TestBitpack_RoundTrip_NonBlockAlignedRows(t *testing.T) {
	width := 17
	for _, rows := range []int{1, 63, 64, 65, 1023, 1024, 1025, 1536, 2047, 2048, 3000} {
		src := make([]uint64, rows)
		mask := uint64(1)<<uint(width) - 1
		for i := range src {
			src[i] = uint64(i) & mask
		}
		dst := make([]byte, PackedSize(rows, width))
		Pack(width, src, dst)
		out := make([]uint64, rows)
		Unpack(width, dst, rows, out)
		for i := range src {
			if out[i] != src[i] {
				t.Fatalf("rows=%d row=%d: got %d want %d", rows, i, out[i], src[i])
			}
		}
	}
}

func TestBitpack_ZeroRows_NoOp(t *testing.T) {
	dst := make([]byte, 0)
	Pack(8, nil, dst)
	out := make([]uint64, 0)
	Unpack(8, dst, 0, out)
}

func TestBitpack_Width1_BoolLike(t *testing.T) {
	rows := 1024
	src := make([]uint64, rows)
	for i := range src {
		src[i] = uint64(i & 1)
	}
	dst := make([]byte, PackedSize(rows, 1))
	if len(dst) != 128 {
		t.Fatalf("PackedSize(1024, 1) = %d, want 128 (one block * 1 width * 128 bytes)", len(dst))
	}
	Pack(1, src, dst)
	out := make([]uint64, rows)
	Unpack(1, dst, rows, out)
	for i := range src {
		if out[i] != src[i] {
			t.Fatalf("row %d: got %d want %d", i, out[i], src[i])
		}
	}
}

func TestBitpack_Width64_FullRange(t *testing.T) {
	rows := 100
	src := []uint64{0, 1, ^uint64(0), ^uint64(0) >> 1, 1 << 63}
	for len(src) < rows {
		src = append(src, uint64(len(src))*0xDEADBEEFCAFEBABE)
	}
	src = src[:rows]
	dst := make([]byte, PackedSize(rows, 64))
	Pack(64, src, dst)
	out := make([]uint64, rows)
	Unpack(64, dst, rows, out)
	for i := range src {
		if out[i] != src[i] {
			t.Fatalf("row %d: got %x want %x", i, out[i], src[i])
		}
	}
}

func TestBitpack_PackedSize_Formula(t *testing.T) {
	cases := []struct {
		rows, width, want int
	}{
		{0, 8, 0},
		{1, 8, 1024},
		{1024, 8, 1024},
		{1025, 8, 2048},
		{2048, 8, 2048},
		{1024, 17, 17 * 128},
		{1024, 64, 64 * 128},
	}
	for _, tc := range cases {
		if got := PackedSize(tc.rows, tc.width); got != tc.want {
			t.Fatalf("PackedSize(%d, %d) = %d, want %d", tc.rows, tc.width, got, tc.want)
		}
	}
}

func TestBitpack_RejectsBadWidth(t *testing.T) {
	for _, w := range []int{-1, 0, 65, 1000} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("Pack(width=%d) must panic", w)
				}
			}()
			Pack(w, []uint64{1, 2, 3}, make([]byte, 1024))
		}()
		func() {
			defer func() {
				if recover() == nil {
					t.Fatalf("Unpack(width=%d) must panic", w)
				}
			}()
			Unpack(w, make([]byte, 1024), 3, make([]uint64, 3))
		}()
	}
}

func TestBitpack_HighBitsTruncated(t *testing.T) {
	src := []uint64{0xFFFF, 0xFFFF, 0xFFFF}
	dst := make([]byte, PackedSize(3, 8))
	Pack(8, src, dst)
	out := make([]uint64, 3)
	Unpack(8, dst, 3, out)
	for i, v := range out {
		if v != 0xFF {
			t.Fatalf("row %d: got %x want 0xFF (high bits should drop)", i, v)
		}
	}
}
