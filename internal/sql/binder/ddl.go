// Package binder lowers DripSQL SQL AST nodes into typed engine specs.
package binder

import (
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/kylegrahammatzen/dripsql/internal/schema"
	"github.com/kylegrahammatzen/dripsql/internal/sql/ast"
	"github.com/kylegrahammatzen/dripsql/internal/sqltype"
)

var (
	storageValues = map[string]schema.StorageKind{
		"default":  schema.StorageDefault,
		"columnar": schema.StorageColumnar,
		"row":      schema.StorageRow,
		"hybrid":   schema.StorageHybrid,
	}
	profileValues = map[string]schema.TableProfile{
		"default":         schema.ProfileDefault,
		"event_analytics": schema.ProfileEventAnalytics,
		"time_series":     schema.ProfileTimeSeries,
		"dimension_table": schema.ProfileDimensionTable,
		"log_analytics":   schema.ProfileLogAnalytics,
	}
	compressionValues = map[string]schema.CompressionPolicy{
		"default": schema.CompressionDefault,
		"auto":    schema.CompressionAuto,
		"none":    schema.CompressionNone,
		"fast":    schema.CompressionFast,
		"best":    schema.CompressionBest,
	}
)

func BindCreateType(stmt *ast.CreateTypeStmt) (schema.TypeSpec, error) {
	if stmt == nil {
		return schema.TypeSpec{}, fmt.Errorf("CREATE TYPE statement is nil")
	}
	spec := schema.TypeSpec{
		Name:        normalizeName(stmt.Name),
		IfNotExists: stmt.IfNotExists,
		EnumLabels:  slices.Clone(stmt.EnumLabels),
	}
	if err := spec.Validate(); err != nil {
		return schema.TypeSpec{}, err
	}
	return spec, nil
}

func BindCreateTable(stmt *ast.CreateTableStmt) (schema.TableSpec, error) {
	if stmt == nil {
		return schema.TableSpec{}, fmt.Errorf("CREATE TABLE statement is nil")
	}

	columns := make([]schema.ColumnSpec, 0, len(stmt.Columns))
	for _, col := range stmt.Columns {
		columns = append(columns, schema.ColumnSpec{
			Name:     normalizeName(col.Name),
			Type:     sqltype.Parse(normalizeName(col.Type)),
			Nullable: !col.NotNull,
		})
	}

	options, err := bindTableOptions(stmt.Options)
	if err != nil {
		return schema.TableSpec{}, err
	}
	spec := schema.TableSpec{
		Name:        normalizeName(stmt.Name),
		IfNotExists: stmt.IfNotExists,
		Columns:     columns,
		Options:     options,
	}
	if err := spec.Validate(); err != nil {
		return schema.TableSpec{}, err
	}
	return spec, nil
}

func bindTableOptions(options []ast.TableOption) (schema.TableOptions, error) {
	var out schema.TableOptions
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
			case ast.ValueIdent, ast.ValueString:
				text, err := optionText(opt)
				if err != nil {
					return out, err
				}
				if text != "auto" {
					return out, fmt.Errorf("unsupported segment_rows option %q", text)
				}
				out.SegmentRows = schema.AutoSegmentRows
			case ast.ValueInt:
				n := opt.Value.Int
				if n <= 0 || n > math.MaxInt32 {
					return out, fmt.Errorf("segment_rows must be positive or auto")
				}
				out.SegmentRows = schema.SegmentRows(int(n))
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

func optionText(opt ast.TableOption) (string, error) {
	if opt.Value.Kind != ast.ValueIdent && opt.Value.Kind != ast.ValueString {
		return "", fmt.Errorf("table option %q requires an identifier or string value", normalizeName(opt.Name))
	}
	text := normalizeName(opt.Value.String)
	if text == "" {
		return "", fmt.Errorf("table option %q requires a non-empty value", normalizeName(opt.Name))
	}
	return text, nil
}

func normalizeName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}
