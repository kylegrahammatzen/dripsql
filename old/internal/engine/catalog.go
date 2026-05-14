package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	v3sql "github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

const catalogFileName = "catalog.json"

type catalogFile struct {
	Version v3sql.SchemaVersion `json:"version"`
	Types   []catalogTypeEntry  `json:"types,omitempty"`
	Tables  []catalogTableEntry `json:"tables,omitempty"`
}

type catalogTypeEntry struct {
	ID   v3sql.TypeID   `json:"id"`
	Spec types.TypeSpec `json:"spec"`
}

type catalogTableEntry struct {
	ID   v3sql.TableID   `json:"id"`
	Spec types.TableSpec `json:"spec"`
}

func loadCatalog(root string) (map[string]typeEntry, map[string]tableEntry, v3sql.SchemaVersion, error) {
	path := filepath.Join(root, catalogFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]typeEntry{}, map[string]tableEntry{}, 0, nil
		}
		return nil, nil, 0, err
	}

	var file catalogFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, nil, 0, fmt.Errorf("load catalog: %w", err)
	}

	typesByName := make(map[string]typeEntry, len(file.Types))
	for i, entry := range file.Types {
		spec := normalizeCatalogType(entry.Spec)
		if err := spec.Validate(); err != nil {
			return nil, nil, 0, fmt.Errorf("catalog type %d: %w", i, err)
		}
		if _, exists := typesByName[spec.Name]; exists {
			return nil, nil, 0, fmt.Errorf("duplicate catalog type %q", spec.Name)
		}
		id := entry.ID
		if id == 0 {
			id = v3sql.TypeID(i + 1)
		}
		typesByName[spec.Name] = typeEntry{id: id, spec: spec}
	}

	tablesByName := make(map[string]tableEntry, len(file.Tables))
	for i, entry := range file.Tables {
		spec := normalizeCatalogTable(entry.Spec)
		for _, col := range spec.Columns {
			if col.Type.Kind == types.KindNamed {
				if _, ok := typesByName[col.Type.Name]; !ok {
					return nil, nil, 0, fmt.Errorf("catalog table %q references unknown type %q", spec.Name, col.Type.Name)
				}
			}
		}
		if err := spec.Validate(); err != nil {
			return nil, nil, 0, fmt.Errorf("catalog table %d: %w", i, err)
		}
		if _, exists := tablesByName[spec.Name]; exists {
			return nil, nil, 0, fmt.Errorf("duplicate catalog table %q", spec.Name)
		}
		id := entry.ID
		if id == 0 {
			id = v3sql.TableID(i + 1)
		}
		tablesByName[spec.Name] = tableEntry{id: id, spec: spec}
	}

	return typesByName, tablesByName, file.Version, nil
}

func normalizeCatalogType(spec types.TypeSpec) types.TypeSpec {
	spec.Name = normalizeName(spec.Name)
	return spec
}

func normalizeCatalogTable(spec types.TableSpec) types.TableSpec {
	spec.Name = normalizeName(spec.Name)
	for i := range spec.Columns {
		col := &spec.Columns[i]
		col.Name = normalizeName(col.Name)
		if col.Type.Kind == types.KindNamed {
			col.Type = types.Named(normalizeName(col.Type.Name))
		}
	}
	return spec
}

func (db *DB) saveCatalog() error {
	file := catalogFile{
		Version: db.version,
		Types:   make([]catalogTypeEntry, 0, len(db.types)),
		Tables:  make([]catalogTableEntry, 0, len(db.tables)),
	}
	for _, entry := range db.types {
		file.Types = append(file.Types, catalogTypeEntry{ID: entry.id, Spec: entry.spec})
	}
	for _, entry := range db.tables {
		file.Tables = append(file.Tables, catalogTableEntry{ID: entry.id, Spec: entry.spec})
	}
	data, err := json.Marshal(file)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	path := filepath.Join(db.path, catalogFileName)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
