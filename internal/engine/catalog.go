// Engine glue between persisted catalog.Table and binder-side schema.TableSpec.
package engine

import (
	"encoding/json"
	"fmt"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/storage"
)

// Maps share pointers with the catalog file so mutations under db.mu are visible through both.
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

// ColumnIDs are ordered to match the segment writer so footer column index lines up with the identity table.
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

func decodeColumnDefault(col catalog.Column, t schema.Type) sql.BoundDefault {
	if col.InitialDefault == nil {
		return sql.BoundDefault{}
	}
	raw := []byte(*col.InitialDefault)
	if string(raw) == "null" {
		return sql.BoundDefault{Set: true, Null: true}
	}
	switch t.Kind {
	case schema.KindBool:
		var b bool
		if err := json.Unmarshal(raw, &b); err != nil {
			return sql.BoundDefault{}
		}
		return sql.BoundDefault{Set: true, Bool: b}
	case schema.KindInt16, schema.KindInt32, schema.KindInt64,
		schema.KindTimestamp, schema.KindTime, schema.KindDate, schema.KindDecimal:
		var n int64
		if err := json.Unmarshal(raw, &n); err != nil {
			return sql.BoundDefault{}
		}
		return sql.BoundDefault{Set: true, I64: n}
	case schema.KindFloat32, schema.KindFloat64:
		var f float64
		if err := json.Unmarshal(raw, &f); err != nil {
			return sql.BoundDefault{}
		}
		return sql.BoundDefault{Set: true, F64: f}
	case schema.KindText, schema.KindBytes, schema.KindUUID, schema.KindJSON:
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return sql.BoundDefault{}
		}
		return sql.BoundDefault{Set: true, Bytes: []byte(s)}
	}
	return sql.BoundDefault{}
}

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
