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
	typesByName := make(map[string]typeEntry)
	tablesByName := make(map[string]tableEntry)
	path := filepath.Join(root, catalogFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return typesByName, tablesByName, 0, nil
		}
		return nil, nil, 0, err
	}

	var file catalogFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, nil, 0, fmt.Errorf("load catalog: %w", err)
	}
	for i, entry := range file.Types {
		spec := entry.Spec
		spec.Name = normalizeName(spec.Name)
		if err := spec.Validate(); err != nil {
			return nil, nil, 0, fmt.Errorf("catalog type %d: %w", i, err)
		}
		id := entry.ID
		if id == 0 {
			id = v3sql.TypeID(i + 1)
		}
		typesByName[spec.Name] = typeEntry{id: id, spec: spec}
	}
	for i, entry := range file.Tables {
		spec := entry.Spec
		spec.Name = normalizeName(spec.Name)
		for colIndex := range spec.Columns {
			col := &spec.Columns[colIndex]
			col.Name = normalizeName(col.Name)
			if col.Type.Kind == types.KindNamed {
				col.Type = types.Named(normalizeName(col.Type.Name))
				if _, ok := typesByName[col.Type.Name]; !ok {
					return nil, nil, 0, fmt.Errorf("catalog table %q references unknown type %q", spec.Name, col.Type.Name)
				}
			}
		}
		if err := spec.Validate(); err != nil {
			return nil, nil, 0, fmt.Errorf("catalog table %d: %w", i, err)
		}
		id := entry.ID
		if id == 0 {
			id = v3sql.TableID(i + 1)
		}
		tablesByName[spec.Name] = tableEntry{id: id, spec: spec}
	}
	return typesByName, tablesByName, file.Version, nil
}

func (db *DB) saveCatalog() error {
	file := catalogFile{Version: db.version}
	for _, entry := range db.types {
		file.Types = append(file.Types, catalogTypeEntry{ID: entry.id, Spec: entry.spec})
	}
	for _, entry := range db.tables {
		file.Tables = append(file.Tables, catalogTableEntry{ID: entry.id, Spec: entry.spec})
	}
	data, err := json.MarshalIndent(file, "", "  ")
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
