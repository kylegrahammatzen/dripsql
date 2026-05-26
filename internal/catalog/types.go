// Persisted catalog file types. The on-disk shape is a deliberate contract, not a
// reflection dump of binder structs. All enum-shaped fields are strings.
package catalog

import (
	"encoding/json"
)

const CurrentFormatVersion uint32 = 2

type TableID uint64
type TypeID uint64
type ColumnID uint64
type Generation uint64
type SchemaVersion uint64

type File struct {
	FormatVersion uint32     `json:"format_version"`
	Generation    Generation `json:"generation"`
	NextTableID   TableID    `json:"next_table_id"`
	NextTypeID    TypeID     `json:"next_type_id"`
	Types         []Type     `json:"types"`
	Tables        []Table    `json:"tables"`
}

type Type struct {
	TypeID              TypeID     `json:"type_id"`
	Name                string     `json:"name"`
	Kind                string     `json:"kind"`
	Labels              []string   `json:"labels"`
	CreatedAtGeneration Generation `json:"created_at_generation"`
	UpdatedAtGeneration Generation `json:"updated_at_generation"`
}

type Table struct {
	TableID             TableID       `json:"table_id"`
	Name                string        `json:"name"`
	SchemaVersion       SchemaVersion `json:"schema_version"`
	NextColumnID        ColumnID      `json:"next_column_id"`
	CreatedAtGeneration Generation    `json:"created_at_generation"`
	UpdatedAtGeneration Generation    `json:"updated_at_generation"`
	// LegacyPath signals that this table's segment data lives under the v1 directory
	// layout (segments/<normalized_name>). New tables use the table_id-keyed layout
	// (tables/<padded_table_id>) so RENAME TABLE can be metadata only later.
	LegacyPath          bool          `json:"legacy_path,omitempty"`
	Columns             []Column      `json:"columns"`
	PrimaryKey          []ColumnID    `json:"primary_key"`
	Constraints         []Constraint  `json:"constraints"`
	Indexes             []Index       `json:"indexes"`
	StoragePolicy       StoragePolicy `json:"storage_policy"`
}

type Column struct {
	ColumnID            ColumnID         `json:"column_id"`
	Name                string           `json:"name"`
	Type                string           `json:"type"`
	Nullable            bool             `json:"nullable"`
	Ordinal             int              `json:"ordinal"`
	AddedAtGeneration   Generation       `json:"added_at_generation"`
	DroppedAtGeneration *Generation      `json:"dropped_at_generation,omitempty"`
	InitialDefault      *json.RawMessage `json:"initial_default,omitempty"`
}

type StoragePolicy struct {
	Storage       string        `json:"storage"`
	Profile       string        `json:"profile"`
	SegmentRows   SegmentRows   `json:"segment_rows"`
	SortBy        []ColumnID    `json:"sort_by"`
	Compression   string        `json:"compression"`
	TimeColumnID  *ColumnID     `json:"time_column_id"`
	ColumnCodecs  []ColumnCodec `json:"column_codecs"`
}

// Mode is "auto" or "fixed". Rows is meaningful only when Mode == "fixed".
type SegmentRows struct {
	Mode string `json:"mode"`
	Rows int    `json:"rows,omitempty"`
}

type ColumnCodec struct {
	ColumnID ColumnID `json:"column_id"`
	Codec    string   `json:"codec"`
}

type Constraint struct {
	Name      string     `json:"name"`
	Kind      string     `json:"kind"`
	Columns   []ColumnID `json:"columns"`
	Predicate string     `json:"predicate,omitempty"`
}

type Index struct {
	Name    string     `json:"name"`
	Kind    string     `json:"kind"`
	Columns []ColumnID `json:"columns"`
	Unique  bool       `json:"unique"`
}

// MarshalJSON forces empty slices to render as [] rather than null. The catalog file is
// human-read often enough that null in the slice slots is a sustained source of noise.
func (f File) MarshalJSON() ([]byte, error) {
	type alias File
	out := alias(f)
	if out.Types == nil {
		out.Types = []Type{}
	}
	if out.Tables == nil {
		out.Tables = []Table{}
	}
	return json.Marshal(out)
}

func (t Table) MarshalJSON() ([]byte, error) {
	type alias Table
	out := alias(t)
	if out.Columns == nil {
		out.Columns = []Column{}
	}
	if out.PrimaryKey == nil {
		out.PrimaryKey = []ColumnID{}
	}
	if out.Constraints == nil {
		out.Constraints = []Constraint{}
	}
	if out.Indexes == nil {
		out.Indexes = []Index{}
	}
	return json.Marshal(out)
}

func (s StoragePolicy) MarshalJSON() ([]byte, error) {
	type alias StoragePolicy
	out := alias(s)
	if out.SortBy == nil {
		out.SortBy = []ColumnID{}
	}
	if out.ColumnCodecs == nil {
		out.ColumnCodecs = []ColumnCodec{}
	}
	return json.Marshal(out)
}

func (t Type) MarshalJSON() ([]byte, error) {
	type alias Type
	out := alias(t)
	if out.Labels == nil {
		out.Labels = []string{}
	}
	return json.Marshal(out)
}
