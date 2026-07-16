// Smoke test for OpenRandomAccess that writes a temp file, reopens with the targeted-read
// helper, and verifies a ReadAt round-trip. The platform-specific flag is exercised in CI.
package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOpenRandomAccess_ReadAtRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sample.bin")
	payload := []byte("DRIPV4S1________ABCDEFGH")
	if err := os.WriteFile(path, payload, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	f, err := OpenRandomAccess(path)
	if err != nil {
		t.Fatalf("OpenRandomAccess: %v", err)
	}
	defer f.Close()
	buf := make([]byte, 8)
	n, err := f.ReadAt(buf, 16)
	if err != nil {
		t.Fatalf("ReadAt: %v", err)
	}
	if n != 8 || string(buf) != "ABCDEFGH" {
		t.Fatalf("ReadAt got %q (%d), want %q", buf[:n], n, "ABCDEFGH")
	}
}

func TestOpenRandomAccess_MissingFileErrors(t *testing.T) {
	if _, err := OpenRandomAccess(filepath.Join(t.TempDir(), "nope.bin")); err == nil {
		t.Fatal("OpenRandomAccess of missing file must error")
	}
}
