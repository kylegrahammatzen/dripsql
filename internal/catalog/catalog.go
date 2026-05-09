// Package catalog contains in-memory SQL catalog metadata.
package catalog

import (
	"fmt"
	"slices"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
)

type TableID uint64
type ColumnID uint64
type TypeID uint64
type SchemaVersion uint64

type TypeDef struct {
	ID      TypeID
	Name    string
	Labels  []string
	Version SchemaVersion
}

type ColumnDef struct {
	ID       ColumnID
	Name     string
	Type     sqltype.Type
	Labels   []string
	Nullable bool
}

type TableDef struct {
	ID      TableID
	Name    string
	Columns []ColumnDef
	Options schema.TableOptions
	Path    string
	Version SchemaVersion
}

type Catalog struct {
	nextTableID  TableID
	nextColumnID ColumnID
	nextTypeID   TypeID
	version      SchemaVersion
	tables       map[string]TableDef
	types        map[string]TypeDef
}

func New() *Catalog {
	return &Catalog{
		nextTableID:  1,
		nextColumnID: 1,
		nextTypeID:   1,
		tables:       make(map[string]TableDef),
		types:        make(map[string]TypeDef),
	}
}

func (c *Catalog) Version() SchemaVersion {
	if c == nil {
		return 0
	}
	return c.version
}

func (c *Catalog) CreateType(spec schema.TypeSpec) (TypeDef, bool, error) {
	if c == nil {
		return TypeDef{}, false, fmt.Errorf("catalog is nil")
	}
	if err := spec.Validate(); err != nil {
		return TypeDef{}, false, err
	}
	name := normalizeName(spec.Name)
	if existing, ok := c.types[name]; ok {
		if spec.IfNotExists {
			return cloneTypeDef(existing), true, nil
		}
		return TypeDef{}, false, fmt.Errorf("type %q already exists", name)
	}

	c.version++
	def := TypeDef{ID: c.nextTypeID, Name: name, Labels: slices.Clone(spec.EnumLabels), Version: c.version}
	c.nextTypeID++
	c.types[name] = def
	return cloneTypeDef(def), false, nil
}

func (c *Catalog) CreateTable(spec schema.TableSpec) (TableDef, bool, error) {
	if c == nil {
		return TableDef{}, false, fmt.Errorf("catalog is nil")
	}
	if err := spec.Validate(); err != nil {
		return TableDef{}, false, err
	}
	name := normalizeName(spec.Name)
	if existing, ok := c.tables[name]; ok {
		if spec.IfNotExists {
			return cloneTableDef(existing), true, nil
		}
		return TableDef{}, false, fmt.Errorf("table %q already exists", name)
	}

	columns, err := c.bindColumns(spec.Columns)
	if err != nil {
		return TableDef{}, false, fmt.Errorf("table %q: %w", name, err)
	}

	c.version++
	def := TableDef{ID: c.nextTableID, Name: name, Columns: columns, Options: spec.Options, Version: c.version}
	c.nextTableID++
	c.nextColumnID += ColumnID(len(columns))
	c.tables[name] = def
	return cloneTableDef(def), false, nil
}

func (c *Catalog) Table(name string) (TableDef, bool) {
	if c == nil {
		return TableDef{}, false
	}
	def, ok := c.tables[normalizeName(name)]
	if !ok {
		return TableDef{}, false
	}
	return cloneTableDef(def), true
}

func (c *Catalog) Type(name string) (TypeDef, bool) {
	if c == nil {
		return TypeDef{}, false
	}
	def, ok := c.types[normalizeName(name)]
	if !ok {
		return TypeDef{}, false
	}
	return cloneTypeDef(def), true
}

func (c *Catalog) bindColumns(specs []schema.ColumnSpec) ([]ColumnDef, error) {
	columns := make([]ColumnDef, 0, len(specs))
	nextColumnID := c.nextColumnID
	for _, spec := range specs {
		typ := spec.Type
		var labels []string
		if typ.IsNamed() {
			typeName := normalizeName(typ.Name)
			typeDef, ok := c.types[typeName]
			if !ok {
				return nil, fmt.Errorf("column %q references missing type %q", spec.Name, typ.Name)
			}
			typ = sqltype.Named(typeName)
			labels = slices.Clone(typeDef.Labels)
		}
		columns = append(columns, ColumnDef{ID: nextColumnID, Name: normalizeName(spec.Name), Type: typ, Labels: labels, Nullable: spec.Nullable})
		nextColumnID++
	}
	return columns, nil
}

func normalizeName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func cloneTypeDef(def TypeDef) TypeDef {
	def.Labels = slices.Clone(def.Labels)
	return def
}

func cloneTableDef(def TableDef) TableDef {
	def.Columns = slices.Clone(def.Columns)
	for i := range def.Columns {
		def.Columns[i].Labels = slices.Clone(def.Columns[i].Labels)
	}
	def.Options.SortBy = slices.Clone(def.Options.SortBy)
	return def
}
