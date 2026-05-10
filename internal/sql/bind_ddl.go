package sql

import (
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

var (
	storageValues = map[string]types.StorageKind{
		"default":  types.StorageDefault,
		"columnar": types.StorageColumnar,
		"row":      types.StorageRow,
		"hybrid":   types.StorageHybrid,
	}
	profileValues = map[string]types.TableProfile{
		"default":         types.ProfileDefault,
		"event_analytics": types.ProfileEventAnalytics,
		"time_series":     types.ProfileTimeSeries,
		"dimension_table": types.ProfileDimensionTable,
		"log_analytics":   types.ProfileLogAnalytics,
	}
	compressionValues = map[string]types.CompressionPolicy{
		"default": types.CompressionDefault,
		"auto":    types.CompressionAuto,
		"none":    types.CompressionNone,
		"fast":    types.CompressionFast,
		"best":    types.CompressionBest,
	}
)

func BindCreateType(stmt *CreateTypeStmt) (*CreateTypePlan, error) {
	spec, err := BindCreateTypeSpec(stmt)
	if err != nil {
		return nil, err
	}
	return &CreateTypePlan{Spec: spec}, nil
}

func BindCreateTypeSpec(stmt *CreateTypeStmt) (types.TypeSpec, error) {
	if stmt == nil {
		return types.TypeSpec{}, fmt.Errorf("CREATE TYPE statement is nil")
	}
	spec := types.TypeSpec{
		Name:        normalizeName(stmt.Name),
		IfNotExists: stmt.IfNotExists,
		EnumLabels:  slices.Clone(stmt.EnumLabels),
	}
	if err := spec.Validate(); err != nil {
		return types.TypeSpec{}, err
	}
	return spec, nil
}

func BindCreateTable(stmt *CreateTableStmt) (*CreateTablePlan, error) {
	spec, err := BindCreateTableSpec(stmt)
	if err != nil {
		return nil, err
	}
	return &CreateTablePlan{Spec: spec}, nil
}

func BindCreateTableSpec(stmt *CreateTableStmt) (types.TableSpec, error) {
	if stmt == nil {
		return types.TableSpec{}, fmt.Errorf("CREATE TABLE statement is nil")
	}

	columns := make([]types.ColumnSpec, 0, len(stmt.Columns))
	for _, col := range stmt.Columns {
		columns = append(columns, types.ColumnSpec{
			Name:     normalizeName(col.Name),
			Type:     types.Parse(normalizeName(col.Type)),
			Nullable: !col.NotNull,
		})
	}

	options, err := bindTableOptions(stmt.Options)
	if err != nil {
		return types.TableSpec{}, err
	}
	spec := types.TableSpec{
		Name:        normalizeName(stmt.Name),
		IfNotExists: stmt.IfNotExists,
		Columns:     columns,
		Options:     options,
	}
	if err := spec.Validate(); err != nil {
		return types.TableSpec{}, err
	}
	return spec, nil
}

func bindTableOptions(options []TableOption) (types.TableOptions, error) {
	var out types.TableOptions
	seen := make(map[string]struct{}, len(options))
	for _, opt := range options {
		name := normalizeName(opt.Name)
		if name == "" {
			return out, fmt.Errorf("table option name is required")
		}
		if _, dup := seen[name]; dup {
			return out, fmt.Errorf("duplicate table option %q", name)
		}
		seen[name] = struct{}{}

		switch name {
		case "storage":
			text, err := optionText(opt)
			if err != nil {
				return out, err
			}
			v, ok := storageValues[text]
			if !ok {
				return out, fmt.Errorf("unsupported storage option %q", text)
			}
			out.Storage = v
		case "profile":
			text, err := optionText(opt)
			if err != nil {
				return out, err
			}
			v, ok := profileValues[text]
			if !ok {
				return out, fmt.Errorf("unsupported profile option %q", text)
			}
			out.Profile = v
		case "compression":
			text, err := optionText(opt)
			if err != nil {
				return out, err
			}
			v, ok := compressionValues[text]
			if !ok {
				return out, fmt.Errorf("unsupported compression option %q", text)
			}
			out.Compression = v
		case "segment_rows":
			switch opt.Value.Kind {
			case ValueIdent, ValueString:
				text, err := optionText(opt)
				if err != nil {
					return out, err
				}
				if text != "auto" {
					return out, fmt.Errorf("unsupported segment_rows option %q", text)
				}
				out.SegmentRows = types.AutoSegmentRows
			case ValueInt:
				n := opt.Value.Int
				if n <= 0 || n > math.MaxInt32 {
					return out, fmt.Errorf("segment_rows must be positive or auto")
				}
				out.SegmentRows = types.SegmentRows(int(n))
			default:
				return out, fmt.Errorf("segment_rows must be positive or auto")
			}
		case "sort_by":
			text, err := optionText(opt)
			if err != nil {
				return out, err
			}
			for c := range strings.SplitSeq(text, ",") {
				if c = normalizeName(c); c != "" {
					out.SortBy = append(out.SortBy, c)
				}
			}
			if len(out.SortBy) == 0 {
				return out, fmt.Errorf("sort_by requires at least one column")
			}
		case "time_column":
			text, err := optionText(opt)
			if err != nil {
				return out, err
			}
			out.TimeColumn = text
		default:
			return out, fmt.Errorf("unknown table option %q", name)
		}
	}
	return out, nil
}

func optionText(opt TableOption) (string, error) {
	if opt.Value.Kind != ValueIdent && opt.Value.Kind != ValueString {
		return "", fmt.Errorf("table option %q requires an identifier or string value", normalizeName(opt.Name))
	}
	text := normalizeName(opt.Value.String)
	if text == "" {
		return "", fmt.Errorf("table option %q requires a non-empty value", normalizeName(opt.Name))
	}
	return text, nil
}
