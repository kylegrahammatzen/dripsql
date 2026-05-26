package catalog

import (
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
)

func Validate(f *File) error {
	if f == nil {
		return fmt.Errorf("catalog: file is nil")
	}
	if f.FormatVersion == 0 {
		return fmt.Errorf("catalog: format_version is zero")
	}
	if f.FormatVersion > CurrentFormatVersion {
		return fmt.Errorf("catalog: format_version %d is newer than this build (%d)", f.FormatVersion, CurrentFormatVersion)
	}

	typesByID := make(map[TypeID]struct{}, len(f.Types))
	typesByName := make(map[string]struct{}, len(f.Types))
	for i := range f.Types {
		t := &f.Types[i]
		if t.TypeID == 0 {
			return fmt.Errorf("catalog: type %q has zero type_id", t.Name)
		}
		if _, dup := typesByID[t.TypeID]; dup {
			return fmt.Errorf("catalog: duplicate type_id %d", t.TypeID)
		}
		typesByID[t.TypeID] = struct{}{}
		key := schema.NormalizeName(t.Name)
		if key == "" {
			return fmt.Errorf("catalog: type %d has empty name", t.TypeID)
		}
		if _, dup := typesByName[key]; dup {
			return fmt.Errorf("catalog: duplicate type name %q", t.Name)
		}
		typesByName[key] = struct{}{}
		if t.Kind != "enum" {
			return fmt.Errorf("catalog: type %q has unknown kind %q", t.Name, t.Kind)
		}
		if len(t.Labels) == 0 {
			return fmt.Errorf("catalog: type %q has no labels", t.Name)
		}
		if t.TypeID >= f.NextTypeID {
			return fmt.Errorf("catalog: next_type_id %d must exceed all type_id (saw %d)", f.NextTypeID, t.TypeID)
		}
	}

	tablesByID := make(map[TableID]struct{}, len(f.Tables))
	tablesByName := make(map[string]struct{}, len(f.Tables))
	for i := range f.Tables {
		t := &f.Tables[i]
		if t.TableID == 0 {
			return fmt.Errorf("catalog: table %q has zero table_id", t.Name)
		}
		if _, dup := tablesByID[t.TableID]; dup {
			return fmt.Errorf("catalog: duplicate table_id %d", t.TableID)
		}
		tablesByID[t.TableID] = struct{}{}
		key := schema.NormalizeName(t.Name)
		if key == "" {
			return fmt.Errorf("catalog: table %d has empty name", t.TableID)
		}
		if _, dup := tablesByName[key]; dup {
			return fmt.Errorf("catalog: duplicate table name %q", t.Name)
		}
		tablesByName[key] = struct{}{}
		if t.TableID >= f.NextTableID {
			return fmt.Errorf("catalog: next_table_id %d must exceed all table_id (saw %d)", f.NextTableID, t.TableID)
		}
		if err := validateTable(t, typesByName); err != nil {
			return fmt.Errorf("catalog: table %q: %w", t.Name, err)
		}
	}
	return nil
}

func validateTable(t *Table, typesByName map[string]struct{}) error {
	if t.SchemaVersion == 0 {
		return fmt.Errorf("schema_version is zero")
	}
	colsByID := make(map[ColumnID]*Column, len(t.Columns))
	activeNames := make(map[string]struct{}, len(t.Columns))
	for i := range t.Columns {
		c := &t.Columns[i]
		if c.ColumnID == 0 {
			return fmt.Errorf("column %q has zero column_id", c.Name)
		}
		if _, dup := colsByID[c.ColumnID]; dup {
			return fmt.Errorf("duplicate column_id %d", c.ColumnID)
		}
		colsByID[c.ColumnID] = c
		if c.ColumnID >= t.NextColumnID {
			return fmt.Errorf("next_column_id %d must exceed all column_id (saw %d)", t.NextColumnID, c.ColumnID)
		}
		key := schema.NormalizeName(c.Name)
		if key == "" {
			return fmt.Errorf("column %d has empty name", c.ColumnID)
		}
		if c.DroppedAtGeneration == nil {
			if _, dup := activeNames[key]; dup {
				return fmt.Errorf("duplicate active column name %q", c.Name)
			}
			activeNames[key] = struct{}{}
		}
		parsed, err := schema.ParseType(c.Type)
		if err != nil {
			return fmt.Errorf("column %q: %w", c.Name, err)
		}
		if parsed.Kind == schema.KindNamed {
			if _, ok := typesByName[parsed.Name]; !ok {
				return fmt.Errorf("column %q references unknown named type %q", c.Name, parsed.Name)
			}
		}
	}

	for _, id := range t.PrimaryKey {
		if _, ok := colsByID[id]; !ok {
			return fmt.Errorf("primary_key references missing column_id %d", id)
		}
		if colsByID[id].DroppedAtGeneration != nil {
			return fmt.Errorf("primary_key references dropped column_id %d", id)
		}
	}
	for _, c := range t.Constraints {
		for _, id := range c.Columns {
			if _, ok := colsByID[id]; !ok {
				return fmt.Errorf("constraint %q references missing column_id %d", c.Name, id)
			}
		}
	}
	for _, idx := range t.Indexes {
		for _, id := range idx.Columns {
			if _, ok := colsByID[id]; !ok {
				return fmt.Errorf("index %q references missing column_id %d", idx.Name, id)
			}
		}
	}

	if _, ok := schema.ParseStorageKindStrict(t.StoragePolicy.Storage); !ok {
		return fmt.Errorf("unknown storage %q", t.StoragePolicy.Storage)
	}
	if _, ok := schema.ParseProfileStrict(t.StoragePolicy.Profile); !ok {
		return fmt.Errorf("unknown profile %q", t.StoragePolicy.Profile)
	}
	if _, ok := schema.ParseCompressionStrict(t.StoragePolicy.Compression); !ok {
		return fmt.Errorf("unknown compression %q", t.StoragePolicy.Compression)
	}
	switch t.StoragePolicy.SegmentRows.Mode {
	case "auto":
		if t.StoragePolicy.SegmentRows.Rows != 0 {
			return fmt.Errorf("segment_rows auto must have rows=0, got %d", t.StoragePolicy.SegmentRows.Rows)
		}
	case "fixed":
		if t.StoragePolicy.SegmentRows.Rows <= 0 {
			return fmt.Errorf("segment_rows fixed requires rows > 0, got %d", t.StoragePolicy.SegmentRows.Rows)
		}
	default:
		return fmt.Errorf("unknown segment_rows mode %q", t.StoragePolicy.SegmentRows.Mode)
	}
	for _, id := range t.StoragePolicy.SortBy {
		if _, ok := colsByID[id]; !ok {
			return fmt.Errorf("storage_policy.sort_by references missing column_id %d", id)
		}
	}
	if t.StoragePolicy.TimeColumnID != nil {
		if _, ok := colsByID[*t.StoragePolicy.TimeColumnID]; !ok {
			return fmt.Errorf("storage_policy.time_column_id references missing column_id %d", *t.StoragePolicy.TimeColumnID)
		}
	}
	seenCodecCol := make(map[ColumnID]struct{}, len(t.StoragePolicy.ColumnCodecs))
	for _, cc := range t.StoragePolicy.ColumnCodecs {
		if _, dup := seenCodecCol[cc.ColumnID]; dup {
			return fmt.Errorf("storage_policy.column_codecs has duplicate column_id %d", cc.ColumnID)
		}
		seenCodecCol[cc.ColumnID] = struct{}{}
		if _, ok := colsByID[cc.ColumnID]; !ok {
			return fmt.Errorf("storage_policy.column_codecs references missing column_id %d", cc.ColumnID)
		}
		if _, ok := schema.ParseEncodingStrict(cc.Codec); !ok {
			return fmt.Errorf("storage_policy.column_codecs has unknown codec %q for column_id %d", cc.Codec, cc.ColumnID)
		}
	}
	return nil
}
