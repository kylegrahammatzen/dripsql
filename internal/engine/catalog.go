// Engine-side shim over internal/catalog. Converts between the persisted catalog.Table
// shape and the schema.TableSpec shape the binder produces and the runtime consumes.
package engine

import (
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

// indexTypes/indexTables build by-name maps over the catalog file's slices. Maps share
// pointers with the file so mutations under db.mu are visible through both.
func indexTypes(f *catalog.File) map[string]*catalog.Type {
	out := make(map[string]*catalog.Type, len(f.Types))
	for i := range f.Types {
		out[schema.NormalizeName(f.Types[i].Name)] = &f.Types[i]
	}
	return out
}

func indexTables(f *catalog.File) map[string]*catalog.Table {
	out := make(map[string]*catalog.Table, len(f.Tables))
	for i := range f.Tables {
		out[schema.NormalizeName(f.Tables[i].Name)] = &f.Tables[i]
	}
	return out
}

// optionsFromPolicy maps a catalog StoragePolicy back to the in-memory TableOptions
// shape that BoundTableDef carries. Returns zero-valued TableOptions on any unknown
// enum so callers always receive a well-formed value.
func optionsFromPolicy(p catalog.StoragePolicy, cols []catalog.Column) schema.TableOptions {
	out := schema.TableOptions{}
	if v, ok := schema.ParseStorageKindStrict(p.Storage); ok {
		out.Storage = v
	}
	if v, ok := schema.ParseProfileStrict(p.Profile); ok {
		out.Profile = v
	}
	if v, ok := schema.ParseCompressionStrict(p.Compression); ok {
		out.Compression = v
	}
	switch p.SegmentRows.Mode {
	case "auto":
		out.SegmentRows = schema.AutoSegmentRows
	case "fixed":
		out.SegmentRows = schema.SegmentRows(p.SegmentRows.Rows)
	}
	if len(p.SortBy) > 0 {
		names := make([]string, 0, len(p.SortBy))
		for _, id := range p.SortBy {
			if name := columnName(cols, id); name != "" {
				names = append(names, name)
			}
		}
		out.SortBy = names
	}
	if p.TimeColumnID != nil {
		out.TimeColumn = columnName(cols, *p.TimeColumnID)
	}
	return out
}

func columnName(cols []catalog.Column, id catalog.ColumnID) string {
	for i := range cols {
		if cols[i].ColumnID == id {
			return cols[i].Name
		}
	}
	return ""
}

// segmentIdentity builds a SegmentIdentity for a fresh write. ColumnIDs follow the
// declaration order of def.Columns so the segment's footer columns line up with the
// catalog identity at index i.
func (db *DB) segmentIdentity(def sql.BoundTableDef) storage.SegmentIdentity {
	ids := make([]uint64, len(def.Columns))
	for i, c := range def.Columns {
		ids[i] = uint64(c.ID)
	}
	return storage.SegmentIdentity{
		TableID:          uint64(def.ID),
		SchemaGeneration: uint64(db.catalog.Generation),
		ColumnIDs:        ids,
	}
}

// codecForColumn returns the configured Encoding for a column, or EncInvalid if none.
func codecForColumn(p catalog.StoragePolicy, id catalog.ColumnID) schema.Encoding {
	for _, cc := range p.ColumnCodecs {
		if cc.ColumnID != id {
			continue
		}
		if v, ok := schema.ParseEncodingStrict(cc.Codec); ok {
			return v
		}
	}
	return schema.EncInvalid
}

// buildTable converts a binder-side schema.TableSpec into a persisted catalog.Table,
// allocating stable column IDs from the per-table next_column_id. The catalog file is
// not yet aware of the new table; the caller commits it under registerTable.
func buildTable(file *catalog.File, spec schema.TableSpec, gen catalog.Generation) (catalog.Table, error) {
	tab := catalog.Table{
		TableID:             file.NextTableID,
		Name:                spec.Name,
		SchemaVersion:       1,
		NextColumnID:        1,
		CreatedAtGeneration: gen,
		UpdatedAtGeneration: gen,
		Columns:             make([]catalog.Column, 0, len(spec.Columns)),
		PrimaryKey:          []catalog.ColumnID{},
		Constraints:         []catalog.Constraint{},
		Indexes:             []catalog.Index{},
	}
	codecs := []catalog.ColumnCodec{}
	colByName := make(map[string]catalog.ColumnID, len(spec.Columns))
	for i, col := range spec.Columns {
		typeStr, err := schema.TypeString(col.Type)
		if err != nil {
			return catalog.Table{}, fmt.Errorf("column %q: %w", col.Name, err)
		}
		id := tab.NextColumnID
		tab.NextColumnID++
		tab.Columns = append(tab.Columns, catalog.Column{
			ColumnID:          id,
			Name:              col.Name,
			Type:              typeStr,
			Nullable:          col.Nullable,
			Ordinal:           i,
			AddedAtGeneration: gen,
		})
		colByName[col.Name] = id
		if col.Codec != schema.EncInvalid {
			codecs = append(codecs, catalog.ColumnCodec{ColumnID: id, Codec: col.Codec.String()})
		}
	}

	policy := catalog.StoragePolicy{
		Storage:      spec.Options.Storage.String(),
		Profile:      spec.Options.Profile.String(),
		Compression:  spec.Options.Compression.String(),
		SortBy:       []catalog.ColumnID{},
		ColumnCodecs: codecs,
	}
	if spec.Options.SegmentRows.Auto {
		policy.SegmentRows = catalog.SegmentRows{Mode: "auto"}
	} else if spec.Options.SegmentRows.Rows > 0 {
		policy.SegmentRows = catalog.SegmentRows{Mode: "fixed", Rows: spec.Options.SegmentRows.Rows}
	} else {
		policy.SegmentRows = catalog.SegmentRows{Mode: "auto"}
	}
	for _, name := range spec.Options.SortBy {
		id, ok := colByName[schema.NormalizeName(name)]
		if !ok {
			return catalog.Table{}, fmt.Errorf("sort_by references unknown column %q", name)
		}
		policy.SortBy = append(policy.SortBy, id)
	}
	if spec.Options.TimeColumn != "" {
		id, ok := colByName[schema.NormalizeName(spec.Options.TimeColumn)]
		if !ok {
			return catalog.Table{}, fmt.Errorf("time_column references unknown column %q", spec.Options.TimeColumn)
		}
		policy.TimeColumnID = &id
	}
	tab.StoragePolicy = policy
	return tab, nil
}
