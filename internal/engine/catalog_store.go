package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/kylegrahammatzen/dripsql/internal/catalog"
	"github.com/kylegrahammatzen/dripsql/internal/schema"
)

const (
	catalogFileName = "catalog.json"
	catalogVersion  = 1
)

type catalogStore struct {
	Version int                `json:"version"`
	Types   []schema.TypeSpec  `json:"types,omitempty"`
	Tables  []schema.TableSpec `json:"tables,omitempty"`
}

func loadCatalog(path string) (catalogStore, *catalog.Catalog, error) {
	store := catalogStore{Version: catalogVersion}
	data, err := os.ReadFile(filepath.Join(path, catalogFileName))
	if os.IsNotExist(err) {
		return store, catalog.New(), nil
	}
	if err != nil {
		return catalogStore{}, nil, err
	}
	if err := json.Unmarshal(data, &store); err != nil {
		return catalogStore{}, nil, err
	}
	cat, err := catalogFromStore(store)
	if err != nil {
		return catalogStore{}, nil, err
	}
	return store, cat, nil
}

func catalogFromStore(store catalogStore) (*catalog.Catalog, error) {
	if store.Version != catalogVersion {
		return nil, fmt.Errorf("unsupported catalog version %d", store.Version)
	}
	cat := catalog.New()
	for _, spec := range store.Types {
		spec.IfNotExists = false
		if _, _, err := cat.CreateType(spec); err != nil {
			return nil, fmt.Errorf("load type %q: %w", spec.Name, err)
		}
	}
	for _, spec := range store.Tables {
		spec.IfNotExists = false
		if _, _, err := cat.CreateTable(spec); err != nil {
			return nil, fmt.Errorf("load table %q: %w", spec.Name, err)
		}
	}
	return cat, nil
}

func (db *DB) saveCatalog() error {
	db.store.Version = catalogVersion
	data, err := json.MarshalIndent(db.store, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmpPath := filepath.Join(db.path, catalogFileName+".tmp")
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpPath, filepath.Join(db.path, catalogFileName))
}

func (db *DB) restoreCatalog(store catalogStore) {
	cat, err := catalogFromStore(store)
	if err != nil {
		return
	}
	db.catalog = cat
	db.store = store
}

func typeSpecFromDef(def catalog.TypeDef) schema.TypeSpec {
	return schema.TypeSpec{Name: def.Name, EnumLabels: slices.Clone(def.Labels)}
}

func tableSpecFromDef(def catalog.TableDef) schema.TableSpec {
	columns := make([]schema.ColumnSpec, len(def.Columns))
	for i, col := range def.Columns {
		columns[i] = schema.ColumnSpec{Name: col.Name, Type: col.Type, Nullable: col.Nullable}
	}
	spec := schema.TableSpec{Name: def.Name, Columns: columns, Options: def.Options}
	spec.Options.SortBy = slices.Clone(def.Options.SortBy)
	return spec
}

func cloneCatalogStore(store catalogStore) catalogStore {
	out := catalogStore{
		Version: store.Version,
		Types:   make([]schema.TypeSpec, len(store.Types)),
		Tables:  make([]schema.TableSpec, len(store.Tables)),
	}
	for i, spec := range store.Types {
		spec.EnumLabels = slices.Clone(spec.EnumLabels)
		out.Types[i] = spec
	}
	for i, spec := range store.Tables {
		spec.Columns = slices.Clone(spec.Columns)
		spec.Options.SortBy = slices.Clone(spec.Options.SortBy)
		out.Tables[i] = spec
	}
	return out
}
