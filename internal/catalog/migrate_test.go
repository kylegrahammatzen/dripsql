package catalog

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"
)

var updateGolden = flag.Bool("update", false, "rewrite v2 golden fixtures")

func TestMigrate_GoldenFixtures(t *testing.T) {
	cases := []string{"empty", "users", "events"}
	for _, name := range cases {
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join("testdata", "v1", name+".json"))
			if err != nil {
				t.Fatal(err)
			}
			f, err := migrateV1(raw)
			if err != nil {
				t.Fatalf("migrateV1: %v", err)
			}
			got, err := json.MarshalIndent(f, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, '\n')
			goldenPath := filepath.Join("testdata", "v2", name+".json")
			if *updateGolden {
				if err := os.WriteFile(goldenPath, got, 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}
			want, err := os.ReadFile(goldenPath)
			if err != nil {
				t.Fatalf("read golden: %v (run with -update to create)", err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("v2 output mismatch for %s:\n--- got ---\n%s\n--- want ---\n%s", name, got, want)
			}
		})
	}
}

func TestMigrate_RejectsBadLegacyEnum(t *testing.T) {
	raw := []byte(`{"version":1,"tables":[{"id":1,"spec":{"Name":"t","Columns":[{"Name":"x","Type":{"Kind":99,"Name":""},"Nullable":false,"Codec":0}],"Options":{}}}]}`)
	if _, err := migrateV1(raw); err == nil {
		t.Fatal("expected error for unknown kind 99")
	}
}

func TestMigrate_PreservesNamedType(t *testing.T) {
	raw := []byte(`{"version":3,"types":[{"id":1,"spec":{"Name":"event_kind","IfNotExists":false,"EnumLabels":["a","b"]}}],"tables":[{"id":1,"spec":{"Name":"t","Columns":[{"Name":"k","Type":{"Kind":15,"Name":"event_kind"},"Nullable":false,"Codec":0}],"Options":{}}}]}`)
	f, err := migrateV1(raw)
	if err != nil {
		t.Fatalf("migrateV1: %v", err)
	}
	if f.Tables[0].Columns[0].Type != "named:event_kind" {
		t.Fatalf("type = %q", f.Tables[0].Columns[0].Type)
	}
	if len(f.Types) != 1 || f.Types[0].Name != "event_kind" {
		t.Fatalf("types = %+v", f.Types)
	}
}
