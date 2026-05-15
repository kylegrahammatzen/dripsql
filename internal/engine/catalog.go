// Catalog persistence: catalog.json at the DB root carries types + tables + the schema version.
// Atomic temp+rename on every save so a crashed write never leaves a half-written file.
package engine

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/kylegrahammatzen/dripsql/internal/sql"
	"github.com/kylegrahammatzen/dripsql/internal/types"
)

const catalogFile = "catalog.json"

type catalogFileShape struct {
	Version uint64          `json:"version"`
	Types   []typeRecord    `json:"types"`
	Tables  []tableRecord   `json:"tables"`
}

type typeRecord struct {
	ID   sql.TypeID     `json:"id"`
	Spec types.TypeSpec `json:"spec"`
}

type tableRecord struct {
	ID   sql.TableID     `json:"id"`
	Spec types.TableSpec `json:"spec"`
}

type typeEntry struct {
	id   sql.TypeID
	spec types.TypeSpec
}

type tableEntry struct {
	id   sql.TableID
	spec types.TableSpec
}

func loadCatalog(root string) (typesByName map[string]typeEntry, tablesByName map[string]tableEntry, version sql.SchemaVersion, err error) {
	typesByName = make(map[string]typeEntry)
	tablesByName = make(map[string]tableEntry)
	path := filepath.Join(root, catalogFile)
	data, readErr := os.ReadFile(path)
	if os.IsNotExist(readErr) {
		return typesByName, tablesByName, 0, nil
	}
	if readErr != nil {
		return nil, nil, 0, readErr
	}
	var shape catalogFileShape
	if err := json.Unmarshal(data, &shape); err != nil {
		return nil, nil, 0, fmt.Errorf("catalog: parse %s: %w", path, err)
	}
	for _, rec := range shape.Types {
		typesByName[types.NormalizeName(rec.Spec.Name)] = typeEntry{id: rec.ID, spec: rec.Spec}
	}
	for _, rec := range shape.Tables {
		tablesByName[types.NormalizeName(rec.Spec.Name)] = tableEntry{id: rec.ID, spec: rec.Spec}
	}
	return typesByName, tablesByName, sql.SchemaVersion(shape.Version), nil
}

func saveCatalog(root string, typesByName map[string]typeEntry, tablesByName map[string]tableEntry, version sql.SchemaVersion) error {
	shape := catalogFileShape{Version: uint64(version)}
	for _, e := range typesByName {
		shape.Types = append(shape.Types, typeRecord{ID: e.id, Spec: e.spec})
	}
	for _, e := range tablesByName {
		shape.Tables = append(shape.Tables, tableRecord{ID: e.id, Spec: e.spec})
	}
	data, err := json.MarshalIndent(shape, "", "  ")
	if err != nil {
		return err
	}
	target := filepath.Join(root, catalogFile)
	tmp := target + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, target)
}

