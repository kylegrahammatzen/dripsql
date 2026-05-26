package catalog

import (
	"encoding/json"
	"regexp"
	"strings"
	"testing"
)

var bareNull = regexp.MustCompile(`:\s*null\b`)

func containsBareNull(b []byte) bool {
	return bareNull.Match(b)
}

func TestFile_EmptySlicesRenderAsArray(t *testing.T) {
	f := newEmpty()
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if containsBareNull(data) {
		t.Fatalf("empty catalog must not contain null:\n%s", data)
	}
	if !strings.Contains(string(data), `"types": []`) {
		t.Fatal("types slot did not render as []")
	}
	if !strings.Contains(string(data), `"tables": []`) {
		t.Fatal("tables slot did not render as []")
	}
}

func TestFile_TableEmptySlicesRenderAsArray(t *testing.T) {
	timeID := ColumnID(1)
	f := &File{
		FormatVersion: CurrentFormatVersion,
		Generation:    1,
		NextTableID:   2,
		NextTypeID:    1,
		Tables: []Table{{
			TableID:             1,
			Name:                "t",
			SchemaVersion:       1,
			NextColumnID:        2,
			CreatedAtGeneration: 1,
			UpdatedAtGeneration: 1,
			Columns: []Column{{
				ColumnID: 1, Name: "id", Type: "int64", Ordinal: 0, AddedAtGeneration: 1,
			}},
			StoragePolicy: StoragePolicy{
				Storage:      "default",
				Profile:      "default",
				Compression:  "default",
				SegmentRows:  SegmentRows{Mode: "auto"},
				TimeColumnID: &timeID,
			},
		}},
	}
	data, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if containsBareNull(data) {
		t.Fatalf("populated catalog must not contain null:\n%s", data)
	}
	wantSlots := []string{
		`"primary_key": []`,
		`"constraints": []`,
		`"indexes": []`,
		`"sort_by": []`,
		`"column_codecs": []`,
	}
	for _, s := range wantSlots {
		if !strings.Contains(string(data), s) {
			t.Fatalf("missing %q in:\n%s", s, data)
		}
	}
}

func TestFile_RoundTrip(t *testing.T) {
	orig := &File{
		FormatVersion: CurrentFormatVersion,
		Generation:    7,
		NextTableID:   3,
		NextTypeID:    2,
		Types: []Type{{
			TypeID:              1,
			Name:                "kind",
			Kind:                "enum",
			Labels:              []string{"a", "b"},
			CreatedAtGeneration: 2,
			UpdatedAtGeneration: 2,
		}},
		Tables: []Table{{
			TableID:             2,
			Name:                "users",
			SchemaVersion:       1,
			NextColumnID:        3,
			CreatedAtGeneration: 7,
			UpdatedAtGeneration: 7,
			Columns: []Column{
				{ColumnID: 1, Name: "id", Type: "int64", Ordinal: 0, AddedAtGeneration: 7},
				{ColumnID: 2, Name: "k", Type: "named:kind", Ordinal: 1, Nullable: true, AddedAtGeneration: 7},
			},
			StoragePolicy: StoragePolicy{
				Storage:      "columnar",
				Profile:      "event_analytics",
				Compression:  "best",
				SegmentRows:  SegmentRows{Mode: "fixed", Rows: 5000},
				SortBy:       []ColumnID{1},
				ColumnCodecs: []ColumnCodec{{ColumnID: 1, Codec: "for"}},
			},
		}},
	}
	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatal(err)
	}
	var got File
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if err := Validate(&got); err != nil {
		t.Fatalf("validation: %v", err)
	}
	if got.Tables[0].Columns[1].Type != "named:kind" {
		t.Fatalf("Type round-trip wrong: %q", got.Tables[0].Columns[1].Type)
	}
}
