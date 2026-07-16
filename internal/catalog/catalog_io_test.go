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

// Catalogs written before the index surface was removed carry an indexes key that must be ignored, not rejected.
func TestLoad_IgnoresLegacyIndexesKey(t *testing.T) {
	dir := t.TempDir()
	if err := Save(dir, validBase()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, fileName)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	var tables []map[string]json.RawMessage
	if err := json.Unmarshal(doc["tables"], &tables); err != nil {
		t.Fatal(err)
	}
	tables[0]["indexes"] = json.RawMessage(`[{"name":"idx_id","kind":"btree","columns":[1],"unique":true}]`)
	doc["tables"], err = json.Marshal(tables)
	if err != nil {
		t.Fatal(err)
	}
	out, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, out, 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := Load(dir)
	if err != nil {
		t.Fatalf("catalog with indexes key must still load: %v", err)
	}
	if len(f.Tables) != 1 || f.Tables[0].Name != "t" {
		t.Fatalf("unexpected tables after load: %+v", f.Tables)
	}
	if _, err := LoadReadOnly(dir); err != nil {
		t.Fatalf("read-only load with indexes key: %v", err)
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
