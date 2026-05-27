package catalog

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestSave_LeavesBackOnSecondSave(t *testing.T) {
	dir := t.TempDir()
	if err := Save(dir, validBase()); err != nil {
		t.Fatal(err)
	}
	f, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	f.Generation = 2
	f.Tables[0].UpdatedAtGeneration = 2
	if err := Save(dir, f); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, bakFileName)); err != nil {
		t.Fatalf(".bak not preserved: %v", err)
	}
	bak, err := os.ReadFile(filepath.Join(dir, bakFileName))
	if err != nil {
		t.Fatal(err)
	}
	var prior File
	if err := json.Unmarshal(bak, &prior); err != nil {
		t.Fatal(err)
	}
	if prior.Generation != 1 {
		t.Fatalf("bak Generation = %d, want 1", prior.Generation)
	}
}

func TestLoad_SweepsStaleTmp(t *testing.T) {
	dir := t.TempDir()
	if err := Save(dir, validBase()); err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(dir, tmpFileName)
	if err := os.WriteFile(tmp, []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatalf("stale .tmp not swept: %v", err)
	}
}

func TestLoad_RecoversFromBakWhenMainMissing(t *testing.T) {
	dir := t.TempDir()
	if err := Save(dir, validBase()); err != nil {
		t.Fatal(err)
	}
	// Save once more so .bak exists.
	f, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	f.Generation = 2
	f.Tables[0].UpdatedAtGeneration = 2
	if err := Save(dir, f); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, fileName)); err != nil {
		t.Fatal(err)
	}
	got, err := Load(dir)
	if err != nil {
		t.Fatalf("recovery load: %v", err)
	}
	if got.Generation != 1 {
		t.Fatalf(".bak should have held the previous generation 1, got %d", got.Generation)
	}
}

func TestLoad_EmptyDirReturnsFreshFile(t *testing.T) {
	dir := t.TempDir()
	got, err := Load(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got.FormatVersion != CurrentFormatVersion {
		t.Fatalf("FormatVersion = %d", got.FormatVersion)
	}
	if got.NextTableID != 1 || got.NextTypeID != 1 {
		t.Fatalf("next ids = %d/%d", got.NextTableID, got.NextTypeID)
	}
	if got.Generation != 0 {
		t.Fatalf("Generation = %d", got.Generation)
	}
}
