package storage

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBadSegmentMagicReturnsError(t *testing.T) {
	cases := []struct {
		name  string
		write func(t *testing.T, path string)
		want  string
	}{
		{
			name: "header",
			write: func(t *testing.T, path string) {
				writeSegmentBytesAt(t, path, []byte("BADMAGIC"), 0)
			},
			want: "invalid header",
		},
		{
			name: "footer",
			write: func(t *testing.T, path string) {
				info, err := os.Stat(path)
				if err != nil {
					t.Fatalf("Stat: %v", err)
				}
				writeSegmentBytesAt(t, path, []byte("BADMAGIC"), info.Size()-int64(len(segmentMagic)))
			},
			want: "invalid footer magic",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store, table := newTestStore(t)
			meta := appendRows(t, store, table, []int64{1}, []string{"a"})
			path := filepath.Join(tableDir(store.root, table), meta.Path)
			tc.write(t, path)

			// Segments intentionally re-reads segment footers instead of trusting manifest metadata.
			_, err := store.Segments(context.Background(), table)
			if err == nil {
				t.Fatalf("expected bad segment magic error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Segments error = %v, want %q", err, tc.want)
			}
		})
	}
}

func writeSegmentBytesAt(t *testing.T, path string, data []byte, offset int64) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	defer file.Close()
	if _, err := file.WriteAt(data, offset); err != nil {
		t.Fatalf("WriteAt: %v", err)
	}
}
