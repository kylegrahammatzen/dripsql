// v1 catalog -> v2 migration with frozen iota-to-string tables.
// Never edit these tables after merge or existing databases load with wrong meanings.
package catalog

import (
	"encoding/json"
	"fmt"
)

type legacyFile struct {
	Version uint64          `json:"version"`
	Types   []legacyTypeRec `json:"types"`
	Tables  []legacyTableRec `json:"tables"`
}

type legacyTypeRec struct {
	ID   uint64          `json:"id"`
	Spec legacyTypeSpec  `json:"spec"`
}

type legacyTypeSpec struct {
	Name        string   `json:"Name"`
	IfNotExists bool     `json:"IfNotExists"`
	EnumLabels  []string `json:"EnumLabels"`
}

type legacyTableRec struct {
	ID   uint64          `json:"id"`
	Spec legacyTableSpec `json:"spec"`
}

type legacyTableSpec struct {
	Name        string             `json:"Name"`
	IfNotExists bool               `json:"IfNotExists"`
	Columns     []legacyColumnSpec `json:"Columns"`
	Options     legacyOptions      `json:"Options"`
}

type legacyColumnSpec struct {
	Name     string      `json:"Name"`
	Type     legacyType  `json:"Type"`
	Nullable bool        `json:"Nullable"`
	Codec    uint8       `json:"Codec"`
}

type legacyType struct {
	Kind uint8  `json:"Kind"`
	Name string `json:"Name"`
}

type legacyOptions struct {
	Storage     uint8             `json:"Storage"`
	Profile     uint8             `json:"Profile"`
	SegmentRows legacySegmentRows `json:"SegmentRows"`
	SortBy      []string          `json:"SortBy"`
	Compression uint8             `json:"Compression"`
	TimeColumn  string            `json:"TimeColumn"`
}

type legacySegmentRows struct {
	Auto bool `json:"Auto"`
	Rows int  `json:"Rows"`
}

// Frozen snapshot of schema.Kind iota values as of v1.
var v1Kinds = map[uint8]string{
	1:  "bool",
	2:  "int16",
	3:  "int32",
	4:  "int64",
	5:  "float32",
	6:  "float64",
	7:  "decimal",
	8:  "text",
	9:  "bytes",
	10: "uuid",
	11: "timestamp",
	12: "time",
	13: "date",
	14: "json",
	15: "named",
}

// Frozen snapshot of schema.Encoding iota values as of v1.
// EncInvalid maps to empty so the migrator drops it from storage_policy.column_codecs.
var v1Encodings = map[uint8]string{
	0:  "",
	1:  "plain",
	2:  "dict",
	3:  "const",
	4:  "seq",
	5:  "for",
	6:  "delta",
	7:  "flate",
	8:  "zstd",
	9:  "alp",
	10: "alp-rd",
	11: "fsst",
	12: "pcodec",
}

var v1Storage = map[uint8]string{
	0: "default",
	1: "columnar",
	2: "row",
	3: "hybrid",
}

var v1Profile = map[uint8]string{
	0: "default",
	1: "event_analytics",
	2: "time_series",
	3: "dimension_table",
	4: "log_analytics",
}

var v1Compression = map[uint8]string{
	0: "default",
	1: "auto",
	2: "none",
	3: "fast",
	4: "best",
}

// v1 has no format_version key and an empty payload is treated as a fresh v2.
func isV1(raw []byte) bool {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(raw, &probe); err != nil {
		return false
	}
	if len(probe) == 0 {
		return false
	}
	_, hasFormatVersion := probe["format_version"]
	return !hasFormatVersion
}

func migrateV1(raw []byte) (*File, error) {
	var legacy legacyFile
	if err := json.Unmarshal(raw, &legacy); err != nil {
		return nil, fmt.Errorf("catalog migrate: parse v1: %w", err)
	}

	gen := Generation(legacy.Version)
	if gen == 0 {
		gen = 1
	}

	f := &File{
		FormatVersion: CurrentFormatVersion,
		Generation:    gen,
		NextTableID:   1,
		NextTypeID:    1,
		Types:         []Type{},
		Tables:        []Table{},
	}

	for _, lt := range legacy.Types {
		if lt.ID == 0 {
			return nil, fmt.Errorf("catalog migrate: type %q has zero id", lt.Spec.Name)
		}
		f.Types = append(f.Types, Type{
			TypeID:              TypeID(lt.ID),
			Name:                lt.Spec.Name,
			Kind:                "enum",
			Labels:              append([]string{}, lt.Spec.EnumLabels...),
			CreatedAtGeneration: gen,
			UpdatedAtGeneration: gen,
		})
		if TypeID(lt.ID) >= f.NextTypeID {
			f.NextTypeID = TypeID(lt.ID) + 1
		}
	}

	for _, lt := range legacy.Tables {
		if lt.ID == 0 {
			return nil, fmt.Errorf("catalog migrate: table %q has zero id", lt.Spec.Name)
		}
		tab, err := migrateTable(lt, gen)
		if err != nil {
			return nil, fmt.Errorf("catalog migrate: table %q: %w", lt.Spec.Name, err)
		}
		f.Tables = append(f.Tables, tab)
		if TableID(lt.ID) >= f.NextTableID {
			f.NextTableID = TableID(lt.ID) + 1
		}
	}

	if err := Validate(f); err != nil {
		return nil, fmt.Errorf("catalog migrate: validation failed: %w", err)
	}
	return f, nil
}

func migrateTable(lt legacyTableRec, gen Generation) (Table, error) {
	tab := Table{
		TableID:             TableID(lt.ID),
		Name:                lt.Spec.Name,
		SchemaVersion:       1,
		LegacyPath:          true,
		CreatedAtGeneration: gen,
		UpdatedAtGeneration: gen,
		Columns:             make([]Column, 0, len(lt.Spec.Columns)),
		PrimaryKey:          []ColumnID{},
		Constraints:         []Constraint{},
	}

	codecs := []ColumnCodec{}
	colByName := make(map[string]ColumnID, len(lt.Spec.Columns))
	for i, lc := range lt.Spec.Columns {
		colID := ColumnID(i + 1)
		typeStr, err := migrateType(lc.Type)
		if err != nil {
			return Table{}, fmt.Errorf("column %q: %w", lc.Name, err)
		}
		tab.Columns = append(tab.Columns, Column{
			ColumnID:          colID,
			Name:              lc.Name,
			Type:              typeStr,
			Nullable:          lc.Nullable,
			Ordinal:           i,
			AddedAtGeneration: gen,
		})
		colByName[lc.Name] = colID

		if codec, ok := v1Encodings[lc.Codec]; ok {
			if codec != "" {
				codecs = append(codecs, ColumnCodec{ColumnID: colID, Codec: codec})
			}
		} else {
			return Table{}, fmt.Errorf("column %q has unknown legacy codec %d", lc.Name, lc.Codec)
		}
	}
	tab.NextColumnID = ColumnID(len(lt.Spec.Columns) + 1)

	storage, ok := v1Storage[lt.Spec.Options.Storage]
	if !ok {
		return Table{}, fmt.Errorf("unknown legacy storage %d", lt.Spec.Options.Storage)
	}
	profile, ok := v1Profile[lt.Spec.Options.Profile]
	if !ok {
		return Table{}, fmt.Errorf("unknown legacy profile %d", lt.Spec.Options.Profile)
	}
	compression, ok := v1Compression[lt.Spec.Options.Compression]
	if !ok {
		return Table{}, fmt.Errorf("unknown legacy compression %d", lt.Spec.Options.Compression)
	}
	segRows := SegmentRows{Mode: "auto"}
	if !lt.Spec.Options.SegmentRows.Auto && lt.Spec.Options.SegmentRows.Rows > 0 {
		segRows = SegmentRows{Mode: "fixed", Rows: lt.Spec.Options.SegmentRows.Rows}
	}

	sortBy := []ColumnID{}
	for _, name := range lt.Spec.Options.SortBy {
		id, ok := colByName[name]
		if !ok {
			return Table{}, fmt.Errorf("sort_by references unknown column %q", name)
		}
		sortBy = append(sortBy, id)
	}
	var timeCol *ColumnID
	if lt.Spec.Options.TimeColumn != "" {
		id, ok := colByName[lt.Spec.Options.TimeColumn]
		if !ok {
			return Table{}, fmt.Errorf("time_column references unknown column %q", lt.Spec.Options.TimeColumn)
		}
		timeCol = &id
	}

	tab.StoragePolicy = StoragePolicy{
		Storage:      storage,
		Profile:      profile,
		SegmentRows:  segRows,
		SortBy:       sortBy,
		Compression:  compression,
		TimeColumnID: timeCol,
		ColumnCodecs: codecs,
	}
	return tab, nil
}

func migrateType(lt legacyType) (string, error) {
	name, ok := v1Kinds[lt.Kind]
	if !ok {
		return "", fmt.Errorf("unknown legacy kind %d", lt.Kind)
	}
	if name == "named" {
		if lt.Name == "" {
			return "", fmt.Errorf("named type missing name")
		}
		return "named:" + lt.Name, nil
	}
	return name, nil
}
