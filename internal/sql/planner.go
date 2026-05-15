// Planner turns a parsed Stmt into a bound *Plan. Single-table and joined SELECTs flow
// through one planQuery tail; planFrom builds the source *Rel and scope for both shapes.
// DDL/DML/SELECT/EXPLAIN binders all live in this file alongside the planner dispatcher.
package sql

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/kylegrahammatzen/dripsql/internal/types"
)

type Planner struct {
	resolve TableResolver
}

func NewPlanner(resolve TableResolver) *Planner {
	return &Planner{resolve: resolve}
}

// scope tracks the source columns visible to expression binding within a SELECT. For a
// single-table query columns are bare names; for a joined query columns are qualified
// alias.col strings. The joined flag selects the appropriate output and ORDER BY rules.
type scope struct {
	sources []joinedSource
	columns map[string]BoundColumnDef
	joined  bool
}

func (p *Planner) Plan(stmt Stmt) (*Plan, error) {
	switch s := stmt.(type) {
	case *CreateTypeStmt:
		return BindCreateType(s)
	case *CreateTableStmt:
		return BindCreateTable(s)
	case *InsertStmt:
		def, err := p.resolveTable(s.Table)
		if err != nil {
			return nil, err
		}
		return BindInsertPlan(s, def)
	case *DeleteStmt:
		def, err := p.resolveTable(s.Table)
		if err != nil {
			return nil, err
		}
		return BindDelete(s, def)
	case *UpdateStmt:
		def, err := p.resolveTable(s.Table)
		if err != nil {
			return nil, err
		}
		return BindUpdate(s, def)
	case *SelectStmt:
		return p.planSelect(s)
	case *ExplainStmt:
		inner, ok := s.Inner.(*SelectStmt)
		if !ok {
			return nil, fmt.Errorf("EXPLAIN supports SELECT only")
		}
		innerPlan, err := p.planSelect(inner)
		if err != nil {
			return nil, err
		}
		return &Plan{Kind: PlanExplain, Inner: innerPlan, Analyze: s.Analyze}, nil
	}
	return nil, fmt.Errorf("planner: unsupported statement %T", stmt)
}

func (p *Planner) planSelect(stmt *SelectStmt) (*Plan, error) {
	if stmt == nil {
		return nil, fmt.Errorf("SELECT statement is nil")
	}
	rel, sc, err := p.planFrom(stmt)
	if err != nil {
		return nil, err
	}
	rel, err = planQuery(stmt, rel, sc)
	if err != nil {
		return nil, err
	}
	pruneJoinedScans(rel)
	return &Plan{Kind: PlanQuery, Rel: rel}, nil
}

// pruneJoinedScans walks the rel tree and narrows each joined RelScan to the columns its
// alias is actually referenced under. The single-table scan already prunes through
// scanProjectionColumnIDs; joined scans build with allColumnIDs and rely on this post-pass.
func pruneJoinedScans(root *Rel) {
	if root == nil {
		return
	}
	scans := map[string]*Rel{}
	var collectScans func(r *Rel)
	collectScans = func(r *Rel) {
		if r == nil {
			return
		}
		if r.Op == RelScan && r.Alias != "" {
			scans[types.NormalizeName(r.Alias)] = r
		}
		for _, in := range r.Inputs {
			collectScans(in)
		}
	}
	collectScans(root)
	if len(scans) == 0 {
		return
	}
	refs := make(map[string]map[ColumnID]struct{}, len(scans))
	addRef := func(qualified string) {
		dot := -1
		for i := range len(qualified) {
			if qualified[i] == '.' {
				dot = i
				break
			}
		}
		if dot < 0 {
			return
		}
		alias := types.NormalizeName(qualified[:dot])
		colName := types.NormalizeName(qualified[dot+1:])
		scan, ok := scans[alias]
		if !ok {
			return
		}
		for _, c := range scan.Table.Columns {
			if types.NormalizeName(c.Name) == colName {
				set := refs[alias]
				if set == nil {
					set = map[ColumnID]struct{}{}
					refs[alias] = set
				}
				set[c.ID] = struct{}{}
				return
			}
		}
	}
	var walkExpr func(e BoundExpr)
	walkExpr = func(e BoundExpr) {
		if e.Op == ExprColumn {
			addRef(e.Column)
			return
		}
		for _, a := range e.Args {
			walkExpr(a)
		}
	}
	var walkRel func(r *Rel)
	walkRel = func(r *Rel) {
		if r == nil {
			return
		}
		switch r.Op {
		case RelProject:
			for _, p := range r.Projection {
				walkExpr(p.Expr)
			}
		case RelFilter:
			walkExpr(r.Predicate)
		case RelSort:
			for _, k := range r.SortKeys {
				walkExpr(k.Expr)
			}
		case RelJoin:
			for _, k := range r.JoinKeys {
				walkExpr(k.Left)
				walkExpr(k.Right)
			}
		case RelAggregate:
			for _, g := range r.GroupBy {
				walkExpr(g)
			}
			if r.Having != nil {
				walkExpr(*r.Having)
			}
			for _, spec := range r.Aggregates {
				if !spec.Star && spec.ArgName != "" {
					addRef(spec.ArgName)
				}
			}
			for _, spec := range r.Hidden {
				if !spec.Star && spec.ArgName != "" {
					addRef(spec.ArgName)
				}
			}
		case RelScan:
			if r.Where != nil {
				walkExpr(*r.Where)
			}
		}
		for _, in := range r.Inputs {
			walkRel(in)
		}
	}
	walkRel(root)
	for alias, scan := range scans {
		used := refs[alias]
		if len(used) == 0 {
			continue
		}
		narrowed := make([]ColumnID, 0, len(used))
		for _, id := range scan.Columns {
			if _, ok := used[id]; ok {
				narrowed = append(narrowed, id)
			}
		}
		narrowScanColumns(scan, narrowed)
	}
}

// planFrom builds the source *Rel (RelScan for single-table, RelJoin chain for joined) and
// the matching scope. The leftmost table is always resolved; joined sources are added
// left-deep with alias-uniqueness and ON-clause splitting per step.
func (p *Planner) planFrom(stmt *SelectStmt) (*Rel, *scope, error) {
	primaryName, joins, err := stmt.flattenFrom()
	if err != nil {
		return nil, nil, err
	}
	primary, err := p.resolveTable(primaryName.Name)
	if err != nil {
		return nil, nil, err
	}
	if len(joins) == 0 {
		sc := &scope{
			sources: []joinedSource{{Alias: primaryName.Name, Def: primary}},
			columns: buildColumnIndex(primary.Columns),
		}
		return scanRel(primary, "", allColumnIDs(primary.Columns), nil), sc, nil
	}
	primaryAlias := primaryName.Alias
	if primaryAlias == "" {
		primaryAlias = primaryName.Name
	}
	sources := []joinedSource{{Alias: primaryAlias, Def: primary}}
	leftAliases := map[string]bool{types.NormalizeName(primaryAlias): true}
	src := scanRel(primary, primaryAlias, allColumnIDs(primary.Columns), nil)
	for _, join := range joins {
		switch join.Kind {
		case JoinInner, JoinLeft, JoinRight, JoinFull:
		default:
			return nil, nil, fmt.Errorf("unsupported join kind %v", join.Kind)
		}
		rightName, ok := join.Right.(*TableName)
		if !ok {
			return nil, nil, fmt.Errorf("JOIN right side must be a table name")
		}
		right, err := p.resolveTable(rightName.Name)
		if err != nil {
			return nil, nil, err
		}
		rightAlias := rightName.Alias
		if rightAlias == "" {
			rightAlias = rightName.Name
		}
		normRight := types.NormalizeName(rightAlias)
		if leftAliases[normRight] {
			return nil, nil, fmt.Errorf("join alias %q is already used in this query", rightAlias)
		}
		sources = append(sources, joinedSource{Alias: rightAlias, Def: right})
		joinedColumns := buildJoinedColumnIndex(sources)
		on, err := bindExpr(joinedColumns, join.On)
		if err != nil {
			return nil, nil, fmt.Errorf("join ON: %w", err)
		}
		leftKeys, rightKeys, err := splitJoinKeys(on, leftAliases, normRight)
		if err != nil {
			return nil, nil, err
		}
		rightScan := scanRel(right, rightAlias, allColumnIDs(right.Columns), nil)
		keys := make([]JoinKey, len(leftKeys))
		for i := range leftKeys {
			keys[i] = JoinKey{Left: leftKeys[i], Right: rightKeys[i]}
		}
		src = joinRel(join.Kind, src, rightScan, keys)
		leftAliases[normRight] = true
	}
	sc := &scope{
		sources: sources,
		columns: buildJoinedColumnIndex(sources),
		joined:  true,
	}
	return src, sc, nil
}

func (p *Planner) resolveTable(name string) (BoundTableDef, error) {
	if p.resolve == nil {
		return BoundTableDef{}, fmt.Errorf("planner: no table resolver registered")
	}
	return p.resolve(name)
}

// planQuery is the single SELECT tail. It applies WHERE, GROUP/aggregate/HAVING when
// present, and finally projection + ORDER BY + LIMIT/OFFSET. Returns the root *Rel; the
// caller wraps it in a *Plan with Kind=PlanQuery.
func planQuery(stmt *SelectStmt, src *Rel, sc *scope) (*Rel, error) {
	if stmt.Where != nil {
		where, err := bindWhereExpr(sc.columns, stmt.Where)
		if err != nil {
			return nil, err
		}
		if src.Op == RelScan && !sc.joined {
			src.Where = &where
		} else {
			src = filterRel(src, where)
		}
	}

	groupExpr, err := bindGroupExpr(sc.columns, stmt.GroupBy)
	if err != nil {
		return nil, err
	}
	hasGroup := groupExpr != nil
	aggregates, hasAgg, err := bindSelectAggregates(stmt.Select, sc.columns, hasGroup)
	if err != nil {
		return nil, err
	}

	if hasGroup || hasAgg {
		return planAggregateTail(stmt, src, sc, groupExpr, aggregates, hasGroup, hasAgg)
	}
	return planScanTail(stmt, src, sc)
}

func planAggregateTail(stmt *SelectStmt, src *Rel, sc *scope, groupExpr *BoundExpr, aggregates []AggSpec, hasGroup, hasAgg bool) (*Rel, error) {
	if !hasAgg {
		if sc.joined {
			return nil, fmt.Errorf("only count, sum, min, max aggregates are supported in joined SELECT")
		}
		return nil, fmt.Errorf("only count(*), count(column), sum(column), min(column), and max(column) SELECT queries are supported")
	}
	outputs := make([]BoundOutput, 0, len(stmt.Select))
	var group []BoundExpr
	if hasGroup {
		selectFirst, err := bindExpr(sc.columns, stmt.Select[0].Expr)
		if err != nil {
			return nil, err
		}
		if !boundExprEqual(selectFirst, *groupExpr) {
			return nil, fmt.Errorf("selected group expression must match GROUP BY expression")
		}
		if groupExpr.Op != ExprColumn && stmt.Select[0].Alias == "" {
			return nil, fmt.Errorf("GROUP BY computed expressions require a selected alias")
		}
		if !groupableKind(groupExpr.Type.Kind) {
			if sc.joined {
				return nil, fmt.Errorf("GROUP BY expression is %s, want a groupable kind", groupExpr.Type)
			}
			return nil, fmt.Errorf("GROUP BY expression is %s, want text, bytes, uuid, int16, int32, int64, bool, date, timestamp, or enum", groupExpr.Type)
		}
		group = []BoundExpr{*groupExpr}
		outputs = append(outputs, BoundOutput{Alias: stmt.Select[0].Alias, Expr: *groupExpr})
	}
	for _, spec := range aggregates {
		outputs = append(outputs, BoundOutput{Alias: spec.Alias, Expr: BoundExpr{Op: ExprColumn, Type: types.Int64, Column: aggregateOutputName(spec)}})
	}
	var having *BoundExpr
	var hidden []AggSpec
	if stmt.Having != nil {
		hv, hd, err := bindHaving(aggregates, outputs, sc.columns, stmt.Having)
		if err != nil {
			return nil, err
		}
		having = &hv
		hidden = hd
	}
	if src.Op == RelScan && !sc.joined {
		src.Columns = aggregateScanColumnIDs(group, aggregates, hidden, src.Where)
	}
	aggRel := aggregateRel(src, group, aggregates, hidden, having, outputs)
	return applyOrderLimit(aggRel, stmt, outputs, sc.columns)
}

func planScanTail(stmt *SelectStmt, src *Rel, sc *scope) (*Rel, error) {
	if len(stmt.GroupBy) != 0 {
		return nil, fmt.Errorf("GROUP BY requires an aggregate query")
	}
	if stmt.Having != nil {
		return nil, fmt.Errorf("HAVING requires an aggregate query")
	}
	if sc.joined {
		if len(stmt.OrderBy) != 0 {
			keys, err := bindJoinedSortKeys(stmt.OrderBy, sc.columns)
			if err != nil {
				return nil, err
			}
			src = sortRel(src, keys, 0, 0)
		}
		outputs, err := bindJoinedOutputs(stmt, sc.sources, sc.columns)
		if err != nil {
			return nil, err
		}
		root := projectRel(src, outputs)
		if stmt.Limit != nil || stmt.Offset != nil {
			limit := int64(-1)
			offset := int64(0)
			if stmt.Limit != nil {
				if *stmt.Limit < 0 {
					return nil, fmt.Errorf("LIMIT must be non-negative")
				}
				limit = *stmt.Limit
			}
			if stmt.Offset != nil {
				if *stmt.Offset < 0 {
					return nil, fmt.Errorf("OFFSET must be non-negative")
				}
				offset = *stmt.Offset
			}
			root = limitRel(root, limit, offset)
		}
		return root, nil
	}
	outputs, err := bindScanOutputs(stmt, sc.sources[0].Def, sc.columns)
	if err != nil {
		return nil, err
	}
	return applyOrderLimit(src, stmt, outputs, sc.columns)
}

// DDL binding lowers CREATE TYPE / CREATE TABLE statements into validated types.TypeSpec / types.TableSpec.
// Option vocab (storage/profile/compression/segment_rows/sort_by/time_column) is enforced here, not at parse time.


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

func BindCreateType(stmt *CreateTypeStmt) (*Plan, error) {
	spec, err := BindCreateTypeSpec(stmt)
	if err != nil {
		return nil, err
	}
	return &Plan{Kind: PlanCreateType, TypeSpec: spec}, nil
}

func BindCreateTypeSpec(stmt *CreateTypeStmt) (types.TypeSpec, error) {
	if stmt == nil {
		return types.TypeSpec{}, fmt.Errorf("CREATE TYPE statement is nil")
	}
	spec := types.TypeSpec{
		Name:        types.NormalizeName(stmt.Name),
		IfNotExists: stmt.IfNotExists,
		EnumLabels:  slices.Clone(stmt.EnumLabels),
	}
	if err := spec.Validate(); err != nil {
		return types.TypeSpec{}, err
	}
	return spec, nil
}

func BindCreateTable(stmt *CreateTableStmt) (*Plan, error) {
	spec, err := BindCreateTableSpec(stmt)
	if err != nil {
		return nil, err
	}
	return &Plan{Kind: PlanCreateTable, TableSpec: spec}, nil
}

func BindCreateTableSpec(stmt *CreateTableStmt) (types.TableSpec, error) {
	if stmt == nil {
		return types.TableSpec{}, fmt.Errorf("CREATE TABLE statement is nil")
	}

	columns := make([]types.ColumnSpec, 0, len(stmt.Columns))
	for _, col := range stmt.Columns {
		columns = append(columns, types.ColumnSpec{
			Name:     types.NormalizeName(col.Name),
			Type:     types.Parse(types.NormalizeName(col.Type)),
			Nullable: !col.NotNull,
		})
	}

	options, err := bindTableOptions(stmt.Options)
	if err != nil {
		return types.TableSpec{}, err
	}
	spec := types.TableSpec{
		Name:        types.NormalizeName(stmt.Name),
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
		name := types.NormalizeName(opt.Name)
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
				if c = types.NormalizeName(c); c != "" {
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
		return "", fmt.Errorf("table option %q requires an identifier or string value", types.NormalizeName(opt.Name))
	}
	text := types.NormalizeName(opt.Value.String)
	if text == "" {
		return "", fmt.Errorf("table option %q requires a non-empty value", types.NormalizeName(opt.Name))
	}
	return text, nil
}

// INSERT binding validates VALUES rows against a catalog-resolved BoundTableDef.
// Enum literals are validated as known labels; numeric/bool/null literals are range-checked per kind.


type InsertValues struct {
	Table    string
	Columns  []InsertColumn
	RowCount int
}

type InsertColumn struct {
	Name      string
	ID        ColumnID
	Type      types.Type
	Labels    []string
	Nullable  bool
	Values    []Value
	NullCount int
}

func BindInsertPlan(stmt *InsertStmt, def BoundTableDef) (*Plan, error) {
	bound, err := BindInsertValues(stmt, def)
	if err != nil {
		return nil, err
	}
	return &Plan{Kind: PlanInsert, Table: def, Values: bound}, nil
}

func BindInsertValues(stmt *InsertStmt, def BoundTableDef) (InsertValues, error) {
	if stmt == nil {
		return InsertValues{}, fmt.Errorf("INSERT statement is nil")
	}
	if def.Name != "" && types.NormalizeName(stmt.Table) != types.NormalizeName(def.Name) {
		return InsertValues{}, fmt.Errorf("INSERT target %q does not match table %q", stmt.Table, def.Name)
	}
	if len(def.Columns) == 0 {
		return InsertValues{}, fmt.Errorf("table %q has no columns", def.Name)
	}
	if len(stmt.Values) == 0 {
		return InsertValues{}, fmt.Errorf("INSERT requires at least one row")
	}

	sourceIndexes, sourceCount, err := insertSourceIndexes(stmt.Columns, def.Columns)
	if err != nil {
		return InsertValues{}, err
	}

	for rowIndex, row := range stmt.Values {
		if len(row) != sourceCount {
			return InsertValues{}, fmt.Errorf("INSERT row %d has %d values, want %d", rowIndex+1, len(row), sourceCount)
		}
	}

	bound := InsertValues{Table: def.Name, RowCount: len(stmt.Values), Columns: make([]InsertColumn, 0, len(def.Columns))}
	for tableIndex, col := range def.Columns {
		sourceIndex := sourceIndexes[tableIndex]
		boundCol := InsertColumn{Name: col.Name, ID: col.ID, Type: col.Type, Labels: col.Labels, Nullable: col.Nullable, Values: make([]Value, len(stmt.Values))}
		for rowIndex, row := range stmt.Values {
			lit := row[sourceIndex]
			if lit.Kind == ValueNull {
				if !col.Nullable {
					return InsertValues{}, fmt.Errorf("row %d: column %q is NOT NULL", rowIndex+1, col.Name)
				}
				boundCol.NullCount++
				boundCol.Values[rowIndex] = lit
				continue
			}
			resolved, err := validateLiteralForColumn(col, lit)
			if err != nil {
				return InsertValues{}, fmt.Errorf("row %d: %w", rowIndex+1, err)
			}
			boundCol.Values[rowIndex] = resolved
		}
		bound.Columns = append(bound.Columns, boundCol)
	}
	return bound, nil
}

func insertSourceIndexes(names []string, columns []BoundColumnDef) ([]int, int, error) {
	if len(names) == 0 {
		indexes := make([]int, len(columns))
		for i := range indexes {
			indexes[i] = i
		}
		return indexes, len(columns), nil
	}
	if len(names) != len(columns) {
		return nil, 0, fmt.Errorf("INSERT column count %d does not match table column count %d", len(names), len(columns))
	}

	byName := make(map[string]int, len(columns))
	for i, col := range columns {
		byName[types.NormalizeName(col.Name)] = i
	}
	sourceByTableIndex := make([]int, len(columns))
	usedAt := make(map[string]int, len(names))
	for sourceIndex, name := range names {
		norm := types.NormalizeName(name)
		if prev, ok := usedAt[norm]; ok {
			return nil, 0, fmt.Errorf("duplicate INSERT column %q (positions %d and %d)", name, prev+1, sourceIndex+1)
		}
		usedAt[norm] = sourceIndex

		tableIndex, ok := byName[norm]
		if !ok {
			return nil, 0, fmt.Errorf("unknown INSERT column %q", name)
		}
		sourceByTableIndex[tableIndex] = sourceIndex
	}
	return sourceByTableIndex, len(names), nil
}

func validateLiteralForColumn(col BoundColumnDef, lit Value) (Value, error) {
	if col.Type.Kind != types.KindNamed {
		return lit, validateLiteralForType(col.Name, lit, col.Type)
	}
	label, err := bindStringLiteral(col.Name, lit)
	if err != nil {
		return lit, err
	}
	code, ok := enumCodeForLabel(label, col.Labels)
	if !ok {
		return lit, fmt.Errorf("column %q invalid enum label %q", col.Name, label)
	}
	return Value{Kind: ValueEnum, Enum: code, String: label}, nil
}

func validateLiteralForType(column string, lit Value, typ types.Type) error {
	switch typ.Kind {
	case types.KindBool:
		_, err := bindBoolLiteral(column, lit)
		return err
	case types.KindInt16:
		value, err := bindInt64Literal(column, lit)
		if err != nil {
			return err
		}
		if value < math.MinInt16 || value > math.MaxInt16 {
			return fmt.Errorf("column %q int16 literal out of range", column)
		}
		return nil
	case types.KindInt32:
		_, err := bindInt32Literal(column, lit)
		return err
	case types.KindInt64:
		_, err := bindInt64Literal(column, lit)
		return err
	case types.KindFloat32, types.KindFloat64:
		value, err := bindFloat64Literal(column, lit)
		if err != nil {
			return err
		}
		if typ.Kind == types.KindFloat32 && (value < -math.MaxFloat32 || value > math.MaxFloat32) {
			return fmt.Errorf("column %q float32 literal out of range", column)
		}
		return nil
	case types.KindDecimal:
		if lit.Kind != ValueInt && lit.Kind != ValueString {
			return fmt.Errorf("column %q expects decimal literal", column)
		}
		return nil
	case types.KindText, types.KindJSON, types.KindNamed:
		_, err := bindStringLiteral(column, lit)
		return err
	case types.KindBytes:
		if lit.Kind != ValueString {
			return fmt.Errorf("column %q expects string literal", column)
		}
		return nil
	case types.KindUUID:
		s, err := bindStringLiteral(column, lit)
		if err != nil {
			return err
		}
		if _, err := types.ParseUUID(s); err != nil {
			return fmt.Errorf("column %q invalid uuid literal %q", column, s)
		}
		return nil
	case types.KindTimestamp:
		s, err := bindStringLiteral(column, lit)
		if err != nil {
			return err
		}
		if _, err := time.Parse(time.RFC3339Nano, s); err != nil {
			return fmt.Errorf("column %q invalid timestamp literal %q", column, s)
		}
		return nil
	case types.KindDate:
		s, err := bindStringLiteral(column, lit)
		if err != nil {
			return err
		}
		if _, err := time.Parse("2006-01-02", s); err != nil {
			return fmt.Errorf("column %q invalid date literal %q", column, s)
		}
		return nil
	case types.KindTime:
		s, err := bindStringLiteral(column, lit)
		if err != nil {
			return err
		}
		if _, err := time.Parse("15:04:05.999999999", s); err != nil {
			return fmt.Errorf("column %q invalid time literal %q", column, s)
		}
		return nil
	default:
		return fmt.Errorf("column %q has unsupported INSERT type %s", column, typ)
	}
}

func enumCodeForLabel(value string, labels []string) (uint32, bool) {
	for i, label := range labels {
		if value == label {
			return uint32(i + 1), true
		}
	}
	return 0, false
}

func bindInt64Literal(column string, lit Value) (int64, error) {
	if lit.Kind != ValueInt {
		return 0, fmt.Errorf("column %q expects int64 literal", column)
	}
	return lit.Int, nil
}

func bindInt32Literal(column string, lit Value) (int32, error) {
	value, err := bindInt64Literal(column, lit)
	if err != nil {
		return 0, err
	}
	if value < math.MinInt32 || value > math.MaxInt32 {
		return 0, fmt.Errorf("column %q int32 literal out of range", column)
	}
	return int32(value), nil
}

func bindFloat64Literal(column string, lit Value) (float64, error) {
	var value float64
	switch lit.Kind {
	case ValueInt:
		value = float64(lit.Int)
	case ValueFloat:
		value = lit.Float
	default:
		return 0, fmt.Errorf("column %q expects float literal", column)
	}
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, fmt.Errorf("column %q float literal out of range", column)
	}
	return value, nil
}

func bindBoolLiteral(column string, lit Value) (bool, error) {
	if lit.Kind != ValueBool {
		return false, fmt.Errorf("column %q expects bool literal", column)
	}
	return lit.Bool, nil
}

func bindStringLiteral(column string, lit Value) (string, error) {
	if lit.Kind != ValueString {
		return "", fmt.Errorf("column %q expects string literal", column)
	}
	return lit.String, nil
}

// BindDelete validates DELETE against a BoundTableDef and binds the optional WHERE expression.
// No-WHERE form means "delete every row in the table" and is allowed.


func BindDelete(stmt *DeleteStmt, def BoundTableDef) (*Plan, error) {
	if stmt == nil {
		return nil, fmt.Errorf("DELETE statement is nil")
	}
	if def.Name != "" && types.NormalizeName(stmt.Table) != types.NormalizeName(def.Name) {
		return nil, fmt.Errorf("DELETE target %q does not match table %q", stmt.Table, def.Name)
	}
	plan := &Plan{Kind: PlanDelete, Table: def}
	if stmt.Where != nil {
		columns := buildColumnIndex(def.Columns)
		where, err := bindWhereExpr(columns, stmt.Where)
		if err != nil {
			return nil, err
		}
		plan.Where = &where
	}
	return plan, nil
}

// BindUpdate validates UPDATE against a BoundTableDef and binds assignments and WHERE.
// Each SET target must be a distinct table column and the literal must match the column type.


func BindUpdate(stmt *UpdateStmt, def BoundTableDef) (*Plan, error) {
	if stmt == nil {
		return nil, fmt.Errorf("UPDATE statement is nil")
	}
	if def.Name != "" && types.NormalizeName(stmt.Table) != types.NormalizeName(def.Name) {
		return nil, fmt.Errorf("UPDATE target %q does not match table %q", stmt.Table, def.Name)
	}
	if len(stmt.Assignments) == 0 {
		return nil, fmt.Errorf("UPDATE requires at least one assignment")
	}
	columnsByName := make(map[string]BoundColumnDef, len(def.Columns))
	for _, c := range def.Columns {
		columnsByName[types.NormalizeName(c.Name)] = c
	}
	seen := make(map[string]struct{}, len(stmt.Assignments))
	assignments := make([]BoundAssignment, 0, len(stmt.Assignments))
	for _, a := range stmt.Assignments {
		key := types.NormalizeName(a.Column)
		if _, dup := seen[key]; dup {
			return nil, fmt.Errorf("UPDATE assigns column %q twice", a.Column)
		}
		seen[key] = struct{}{}
		col, ok := columnsByName[key]
		if !ok {
			return nil, fmt.Errorf("UPDATE column %q does not exist", a.Column)
		}
		if a.Value.Kind == ValueNull {
			if !col.Nullable {
				return nil, fmt.Errorf("UPDATE column %q is NOT NULL", col.Name)
			}
		}
		resolved := a.Value
		if a.Value.Kind != ValueNull {
			r, err := validateLiteralForColumn(col, a.Value)
			if err != nil {
				return nil, err
			}
			resolved = r
		}
		assignments = append(assignments, BoundAssignment{Column: col, Value: resolved})
	}
	plan := &Plan{Kind: PlanUpdate, Table: def, Assignments: assignments}
	if stmt.Where != nil {
		index := buildColumnIndex(def.Columns)
		where, err := bindWhereExpr(index, stmt.Where)
		if err != nil {
			return nil, err
		}
		plan.Where = &where
	}
	return plan, nil
}

// Joined SELECT helpers: ON-clause splitting, qualified output expansion, and qualified
// ORDER BY binding. The join chain itself is built in planner.go's planFrom.


type TableResolver func(name string) (BoundTableDef, error)

// splitJoinKeys requires ON to be one or more column = column equalities (AND-joined) with
// each side from a different source. Returns (leftKeys, rightKeys) tagged so left references
// the build-side alias set.
func splitJoinKeys(on BoundExpr, leftAliases map[string]bool, rightAlias string) ([]BoundExpr, []BoundExpr, error) {
	var conjuncts []BoundExpr
	if err := collectAndConjuncts(on, &conjuncts); err != nil {
		return nil, nil, err
	}
	if len(conjuncts) == 0 {
		return nil, nil, fmt.Errorf("ON must contain at least one column equality")
	}
	var leftKeys, rightKeys []BoundExpr
	for _, c := range conjuncts {
		l, r, err := splitJoinEquality(c, leftAliases, rightAlias)
		if err != nil {
			return nil, nil, err
		}
		leftKeys = append(leftKeys, l)
		rightKeys = append(rightKeys, r)
	}
	return leftKeys, rightKeys, nil
}

func collectAndConjuncts(expr BoundExpr, out *[]BoundExpr) error {
	if expr.Op == ExprAnd {
		if len(expr.Args) != 2 {
			return fmt.Errorf("ON: malformed AND")
		}
		if err := collectAndConjuncts(expr.Args[0], out); err != nil {
			return err
		}
		return collectAndConjuncts(expr.Args[1], out)
	}
	*out = append(*out, expr)
	return nil
}

func splitJoinEquality(on BoundExpr, leftAliases map[string]bool, rightAlias string) (BoundExpr, BoundExpr, error) {
	if on.Op != ExprEqual {
		return BoundExpr{}, BoundExpr{}, fmt.Errorf("ON conjuncts must be column equalities")
	}
	if len(on.Args) != 2 {
		return BoundExpr{}, BoundExpr{}, fmt.Errorf("ON malformed")
	}
	left, right := on.Args[0], on.Args[1]
	if left.Op != ExprColumn || right.Op != ExprColumn {
		return BoundExpr{}, BoundExpr{}, fmt.Errorf("ON must reference one column from each side")
	}
	leftSide := columnAlias(left.Column)
	rightSide := columnAlias(right.Column)
	if leftAliases[leftSide] && rightSide == rightAlias {
		return left, right, nil
	}
	if leftAliases[rightSide] && leftSide == rightAlias {
		return right, left, nil
	}
	return BoundExpr{}, BoundExpr{}, fmt.Errorf("ON columns must reference both join sides; got %q and %q", leftSide, rightSide)
}

func bindJoinedSortKeys(orderBy []OrderExpr, columns map[string]BoundColumnDef) ([]SortKey, error) {
	keys := make([]SortKey, 0, len(orderBy))
	for _, order := range orderBy {
		if order.Expr == nil {
			return nil, fmt.Errorf("ORDER BY %q must be a qualified column reference on joined queries", order.Name)
		}
		bound, err := bindExpr(columns, order.Expr)
		if err != nil {
			return nil, err
		}
		keys = append(keys, SortKey{Expr: bound, Desc: order.Desc})
	}
	return keys, nil
}

func columnAlias(qualified string) string {
	for i, r := range qualified {
		if r == '.' {
			return qualified[:i]
		}
	}
	return ""
}

func bindJoinedOutputs(stmt *SelectStmt, sources []joinedSource, columns map[string]BoundColumnDef) ([]BoundOutput, error) {
	if len(stmt.Select) == 1 {
		if _, ok := stmt.Select[0].Expr.(*StarRef); ok {
			var outputs []BoundOutput
			for _, s := range sources {
				alias := types.NormalizeName(s.Alias)
				for _, col := range s.Def.Columns {
					qual := alias + "." + types.NormalizeName(col.Name)
					outputs = append(outputs, BoundOutput{Expr: BoundExpr{Op: ExprColumn, Type: col.Type, Column: qual}})
				}
			}
			return outputs, nil
		}
	}
	outputs := make([]BoundOutput, 0, len(stmt.Select))
	for _, sel := range stmt.Select {
		bound, err := bindExpr(columns, sel.Expr)
		if err != nil {
			return nil, err
		}
		alias := sel.Alias
		if alias == "" {
			if bound.Op != ExprColumn {
				return nil, fmt.Errorf("joined SELECT computed expressions require an alias")
			}
			alias = bound.Column
		}
		outputs = append(outputs, BoundOutput{Alias: alias, Expr: bound})
	}
	return outputs, nil
}

// SELECT binding helpers: scan output expansion, WHERE/HAVING walkers, GROUP BY and
// aggregate extraction, ORDER BY/LIMIT lowering. The single-table BindSelect wrapper exists
// for tests that supply a known def directly; production planning runs through planner.go.


func BindSelect(stmt *SelectStmt, def BoundTableDef) (*Plan, error) {
	if stmt == nil {
		return nil, fmt.Errorf("SELECT statement is nil")
	}
	primary, joins, err := stmt.flattenFrom()
	if err != nil {
		return nil, err
	}
	if len(joins) != 0 {
		return nil, fmt.Errorf("BindSelect does not accept joined queries")
	}
	if def.Name != "" && types.NormalizeName(primary.Name) != types.NormalizeName(def.Name) {
		return nil, fmt.Errorf("SELECT target %q does not match table %q", primary.Name, def.Name)
	}
	sc := &scope{
		sources: []joinedSource{{Alias: primary.Name, Def: def}},
		columns: buildColumnIndex(def.Columns),
	}
	src := scanRel(def, "", allColumnIDs(def.Columns), nil)
	rel, err := planQuery(stmt, src, sc)
	if err != nil {
		return nil, err
	}
	return &Plan{Kind: PlanQuery, Rel: rel}, nil
}

func aggregateScanColumnIDs(group []BoundExpr, aggregates, hidden []AggSpec, where *BoundExpr) []ColumnID {
	seen := make(map[ColumnID]struct{})
	ids := []ColumnID{}
	var walk func(expr BoundExpr)
	walk = func(expr BoundExpr) {
		if expr.Op == ExprColumn {
			id := expr.ColumnID
			if id == 0 {
				return
			}
			if _, ok := seen[id]; ok {
				return
			}
			seen[id] = struct{}{}
			ids = append(ids, id)
			return
		}
		for _, arg := range expr.Args {
			walk(arg)
		}
	}
	for _, g := range group {
		walk(g)
	}
	addAgg := func(spec AggSpec) {
		if spec.Star || spec.ArgColumn == 0 {
			return
		}
		if _, ok := seen[spec.ArgColumn]; ok {
			return
		}
		seen[spec.ArgColumn] = struct{}{}
		ids = append(ids, spec.ArgColumn)
	}
	for _, spec := range aggregates {
		addAgg(spec)
	}
	for _, spec := range hidden {
		addAgg(spec)
	}
	if where != nil {
		walk(*where)
	}
	return ids
}

func BindExplain(stmt *ExplainStmt, def BoundTableDef) (*Plan, error) {
	if stmt == nil {
		return nil, fmt.Errorf("EXPLAIN statement is nil")
	}
	selectStmt, ok := stmt.Inner.(*SelectStmt)
	if !ok {
		return nil, fmt.Errorf("EXPLAIN supports SELECT only")
	}
	inner, err := BindSelect(selectStmt, def)
	if err != nil {
		return nil, err
	}
	return &Plan{Kind: PlanExplain, Inner: inner, Analyze: stmt.Analyze}, nil
}

func bindScanOutputs(stmt *SelectStmt, def BoundTableDef, columns map[string]BoundColumnDef) ([]BoundOutput, error) {
	if len(stmt.Select) == 1 {
		if _, ok := stmt.Select[0].Expr.(*StarRef); ok {
			outputs := make([]BoundOutput, 0, len(def.Columns))
			for _, col := range def.Columns {
				outputs = append(outputs, BoundOutput{Expr: BoundExpr{Op: ExprColumn, Type: col.Type, Column: col.Name, ColumnID: col.ID}})
			}
			return outputs, nil
		}
	}
	outputs := make([]BoundOutput, 0, len(stmt.Select))
	for _, sel := range stmt.Select {
		out, err := bindScanOutputExpr(columns, sel)
		if err != nil {
			return nil, err
		}
		outputs = append(outputs, out)
	}
	return outputs, nil
}

func bindScanOutputExpr(columns map[string]BoundColumnDef, sel SelectExpr) (BoundOutput, error) {
	bound, err := bindExpr(columns, sel.Expr)
	if err != nil {
		return BoundOutput{}, err
	}
	switch bound.Op {
	case ExprColumn:
		return BoundOutput{Alias: sel.Alias, Expr: bound}, nil
	case ExprLiteral:
		if sel.Alias == "" {
			return BoundOutput{}, fmt.Errorf("scan SELECT literal expressions require an alias")
		}
		return BoundOutput{Alias: sel.Alias, Expr: bound}, nil
	}
	if sel.Alias == "" {
		return BoundOutput{}, fmt.Errorf("scan SELECT computed expressions require an alias")
	}
	if !isScanComputedOp(bound.Op) {
		return BoundOutput{}, fmt.Errorf("scan SELECT only supports column, literal, and computed expressions")
	}
	return BoundOutput{Alias: sel.Alias, Expr: bound}, nil
}

func isScanComputedOp(op ExprOp) bool {
	switch op {
	case ExprAdd, ExprSubtract, ExprMultiply, ExprDivide, ExprModulo, ExprIntDivide,
		ExprConcat, ExprLower, ExprUpper, ExprJSONGet, ExprJSONGetText, ExprLength,
		ExprCoalesce, ExprSubstring:
		return true
	default:
		return false
	}
}

func bindWhereExpr(columns map[string]BoundColumnDef, expr Expr) (BoundExpr, error) {
	bound, err := bindWhereLogicalExpr(columns, expr)
	if err == nil {
		return bound, nil
	}
	if col := whereColumnRef(expr); col != "" {
		if _, ok := findColumn(columns, col); !ok {
			return BoundExpr{}, fmt.Errorf("missing WHERE column %q", col)
		}
	}
	return BoundExpr{}, err
}

func whereColumnRef(expr Expr) string {
	switch expr := expr.(type) {
	case *BinaryExpr:
		col, _ := expr.Left.(*ColumnRef)
		if col == nil {
			return ""
		}
		return col.Name
	case *BetweenExpr:
		col, _ := expr.Expr.(*ColumnRef)
		if col == nil {
			return ""
		}
		return col.Name
	case *InExpr:
		col, _ := expr.Expr.(*ColumnRef)
		if col == nil {
			return ""
		}
		return col.Name
	default:
		return ""
	}
}

func bindWhereLogicalExpr(columns map[string]BoundColumnDef, expr Expr) (BoundExpr, error) {
	switch expr := expr.(type) {
	case *AndExpr:
		left, err := bindWhereLogicalExpr(columns, expr.Left)
		if err != nil {
			return BoundExpr{}, err
		}
		right, err := bindWhereLogicalExpr(columns, expr.Right)
		if err != nil {
			return BoundExpr{}, err
		}
		return BoundExpr{Op: ExprAnd, Type: types.Bool, Args: []BoundExpr{left, right}}, nil
	case *OrExpr:
		left, err := bindWhereLogicalExpr(columns, expr.Left)
		if err != nil {
			return BoundExpr{}, err
		}
		right, err := bindWhereLogicalExpr(columns, expr.Right)
		if err != nil {
			return BoundExpr{}, err
		}
		return BoundExpr{Op: ExprOr, Type: types.Bool, Args: []BoundExpr{left, right}}, nil
	case *NotExpr:
		child, err := bindWhereLogicalExpr(columns, expr.Expr)
		if err != nil {
			return BoundExpr{}, err
		}
		return BoundExpr{Op: ExprNot, Type: types.Bool, Args: []BoundExpr{child}}, nil
	case *BinaryExpr:
		op, err := bindBinaryOp(expr.Op)
		if err != nil {
			return BoundExpr{}, fmt.Errorf("unsupported WHERE expression")
		}
		left, right, err := bindBinary(columns, expr.Left, expr.Right)
		if err != nil {
			return BoundExpr{}, err
		}
		if err := validateWhereComparison(left, op, right); err != nil {
			return BoundExpr{}, err
		}
		normalizeComparison(&left, &right)
		return BoundExpr{Op: op, Type: types.Bool, Args: []BoundExpr{left, right}}, nil
	case *BetweenExpr:
		target, err := bindExpr(columns, expr.Expr)
		if err != nil {
			return BoundExpr{}, err
		}
		low, err := bindExpr(columns, expr.Low)
		if err != nil {
			return BoundExpr{}, err
		}
		high, err := bindExpr(columns, expr.High)
		if err != nil {
			return BoundExpr{}, err
		}
		if err := validateBetween(target, low, high, whereBetweenRules); err != nil {
			return BoundExpr{}, err
		}
		normalizeBound(target, &low)
		normalizeBound(target, &high)
		return BoundExpr{Op: ExprBetween, Type: types.Bool, Args: []BoundExpr{target, low, high}}, nil
	case *InExpr:
		target, err := bindExpr(columns, expr.Expr)
		if err != nil {
			return BoundExpr{}, err
		}
		args := make([]BoundExpr, 0, len(expr.Values)+1)
		args = append(args, target)
		for _, value := range expr.Values {
			bound, err := bindExpr(columns, value)
			if err != nil {
				return BoundExpr{}, err
			}
			if err := validateWhereInValue(target, bound); err != nil {
				return BoundExpr{}, err
			}
			normalizeBound(target, &bound)
			args = append(args, bound)
		}
		return BoundExpr{Op: ExprIn, Type: types.Bool, Args: args, Not: expr.Not}, nil
	default:
		return BoundExpr{}, fmt.Errorf("unsupported WHERE expression")
	}
}

// hiddenAggs collects HAVING-only aggregates discovered while binding the HAVING expression.
// The aggregateRel constructor takes the final slice; existing selected aggregates are
// matched against the visible Aggregates list first so the binder never duplicates them.
type hiddenAggs struct {
	selected []AggSpec
	hidden   []AggSpec
}

func bindHaving(aggregates []AggSpec, outputs []BoundOutput, baseColumns map[string]BoundColumnDef, having Expr) (BoundExpr, []AggSpec, error) {
	columns := make([]BoundColumnDef, 0, len(outputs))
	for i, output := range outputs {
		name := outputExprName(output)
		if name == "" {
			continue
		}
		columns = append(columns, BoundColumnDef{ID: ColumnID(i + 1), Name: name, Type: output.Expr.Type})
	}
	acc := &hiddenAggs{selected: aggregates}
	expr, err := bindHavingLogicalExpr(acc, buildColumnIndex(columns), baseColumns, having)
	if err != nil {
		return BoundExpr{}, nil, err
	}
	return expr, acc.hidden, nil
}

func bindHavingLogicalExpr(acc *hiddenAggs, columns map[string]BoundColumnDef, baseColumns map[string]BoundColumnDef, having Expr) (BoundExpr, error) {
	switch expr := having.(type) {
	case *AndExpr:
		left, err := bindHavingLogicalExpr(acc, columns, baseColumns, expr.Left)
		if err != nil {
			return BoundExpr{}, err
		}
		right, err := bindHavingLogicalExpr(acc, columns, baseColumns, expr.Right)
		if err != nil {
			return BoundExpr{}, err
		}
		return BoundExpr{Op: ExprAnd, Type: types.Bool, Args: []BoundExpr{left, right}}, nil
	case *OrExpr:
		left, err := bindHavingLogicalExpr(acc, columns, baseColumns, expr.Left)
		if err != nil {
			return BoundExpr{}, err
		}
		right, err := bindHavingLogicalExpr(acc, columns, baseColumns, expr.Right)
		if err != nil {
			return BoundExpr{}, err
		}
		return BoundExpr{Op: ExprOr, Type: types.Bool, Args: []BoundExpr{left, right}}, nil
	case *NotExpr:
		child, err := bindHavingLogicalExpr(acc, columns, baseColumns, expr.Expr)
		if err != nil {
			return BoundExpr{}, err
		}
		return BoundExpr{Op: ExprNot, Type: types.Bool, Args: []BoundExpr{child}}, nil
	case *BinaryExpr:
		if isArithmeticOp(expr.Op) {
			return BoundExpr{}, fmt.Errorf("HAVING requires a predicate expression")
		}
		left, err := bindHavingScalarExpr(acc, columns, baseColumns, expr.Left)
		if err != nil {
			return BoundExpr{}, err
		}
		right, err := bindHavingScalarExpr(acc, columns, baseColumns, expr.Right)
		if err != nil {
			return BoundExpr{}, err
		}
		op, err := bindBinaryOp(expr.Op)
		if err != nil {
			return BoundExpr{}, err
		}
		if err := validateComparison(left, op, right, havingCmpRules); err != nil {
			return BoundExpr{}, err
		}
		normalizeComparison(&left, &right)
		return BoundExpr{Op: op, Type: types.Bool, Args: []BoundExpr{left, right}}, nil
	case *BetweenExpr:
		target, err := bindHavingScalarExpr(acc, columns, baseColumns, expr.Expr)
		if err != nil {
			return BoundExpr{}, err
		}
		low, err := bindHavingScalarExpr(acc, columns, baseColumns, expr.Low)
		if err != nil {
			return BoundExpr{}, err
		}
		high, err := bindHavingScalarExpr(acc, columns, baseColumns, expr.High)
		if err != nil {
			return BoundExpr{}, err
		}
		if err := validateBetween(target, low, high, havingBetweenRules); err != nil {
			return BoundExpr{}, err
		}
		normalizeBound(target, &low)
		normalizeBound(target, &high)
		return BoundExpr{Op: ExprBetween, Type: types.Bool, Args: []BoundExpr{target, low, high}}, nil
	case *InExpr:
		target, err := bindHavingScalarExpr(acc, columns, baseColumns, expr.Expr)
		if err != nil {
			return BoundExpr{}, err
		}
		args := make([]BoundExpr, 0, len(expr.Values)+1)
		args = append(args, target)
		for _, value := range expr.Values {
			bound, err := bindHavingScalarExpr(acc, columns, baseColumns, value)
			if err != nil {
				return BoundExpr{}, err
			}
			if err := validateWhereInValue(target, bound); err != nil {
				return BoundExpr{}, err
			}
			normalizeBound(target, &bound)
			args = append(args, bound)
		}
		return BoundExpr{Op: ExprIn, Type: types.Bool, Args: args, Not: expr.Not}, nil
	default:
		return BoundExpr{}, fmt.Errorf("unsupported HAVING expression")
	}
}

func bindHavingScalarExpr(acc *hiddenAggs, columns map[string]BoundColumnDef, baseColumns map[string]BoundColumnDef, expr Expr) (BoundExpr, error) {
	switch expr := expr.(type) {
	case *ColumnRef:
		col, ok := findColumn(columns, expr.Name)
		if !ok {
			return BoundExpr{}, fmt.Errorf("HAVING column %q must be a selected output column", expr.Name)
		}
		return BoundExpr{Op: ExprColumn, Type: col.Type, Column: col.Name, ColumnID: col.ID}, nil
	case *FuncCall:
		if _, ok := aggregateFuncByName(expr.Name); !ok {
			return bindScalarCall(columns, expr)
		}
		return bindHavingAggregate(acc, baseColumns, expr)
	case *Literal:
		return literalExpr(expr.Value), nil
	case *BinaryExpr:
		if expr.Op == BinaryConcat {
			return bindBinaryExpr(columns, expr)
		}
		if !isArithmeticOp(expr.Op) {
			return BoundExpr{}, fmt.Errorf("HAVING scalar expression contains a predicate operator")
		}
		left, err := bindHavingScalarExpr(acc, columns, baseColumns, expr.Left)
		if err != nil {
			return BoundExpr{}, err
		}
		right, err := bindHavingScalarExpr(acc, columns, baseColumns, expr.Right)
		if err != nil {
			return BoundExpr{}, err
		}
		if !isIntegerExpr(left) || !isIntegerExpr(right) {
			return BoundExpr{}, fmt.Errorf("HAVING arithmetic expressions require integer operands")
		}
		op, ok := arithmeticOp(expr.Op)
		if !ok {
			return BoundExpr{}, fmt.Errorf("unsupported HAVING arithmetic operator")
		}
		return BoundExpr{Op: op, Type: types.Int64, Args: []BoundExpr{left, right}}, nil
	default:
		return BoundExpr{}, fmt.Errorf("unsupported HAVING scalar expression")
	}
}

func bindHavingAggregate(acc *hiddenAggs, baseColumns map[string]BoundColumnDef, call *FuncCall) (BoundExpr, error) {
	if len(acc.selected) == 0 {
		return BoundExpr{}, fmt.Errorf("HAVING aggregate requires an aggregate query")
	}
	want, argName, star, err := decomposeAggregateCall(call)
	if err != nil {
		return BoundExpr{}, err
	}
	for _, selected := range acc.selected {
		if selected.Func == want && selected.Star == star && types.NormalizeName(selected.ArgName) == types.NormalizeName(argName) {
			return BoundExpr{Op: ExprColumn, Type: types.Int64, Column: aggregateOutputName(selected)}, nil
		}
	}
	hidden, err := bindHiddenHavingAggregate(baseColumns, want, argName, star)
	if err != nil {
		return BoundExpr{}, err
	}
	for _, existing := range acc.hidden {
		if existing.Func == hidden.Func && existing.ArgColumn == hidden.ArgColumn && existing.Star == hidden.Star {
			return BoundExpr{Op: ExprColumn, Type: types.Int64, Column: existing.Alias}, nil
		}
	}
	acc.hidden = append(acc.hidden, hidden)
	return BoundExpr{Op: ExprColumn, Type: types.Int64, Column: hidden.Alias}, nil
}

func bindHiddenHavingAggregate(columns map[string]BoundColumnDef, fn AggregateFunc, argName string, star bool) (AggSpec, error) {
	if fn == AggregateCount && star {
		return AggSpec{Func: fn, Star: true, Alias: hiddenHavingAggregateName(fn, "", true)}, nil
	}
	col, ok := findColumn(columns, argName)
	if !ok {
		return AggSpec{}, fmt.Errorf("missing HAVING aggregate column %q", argName)
	}
	if fn != AggregateCount {
		if err := validateAggregateColumn(fn, col.Name, columns); err != nil {
			return AggSpec{}, err
		}
	}
	return AggSpec{Func: fn, ArgColumn: col.ID, ArgName: col.Name, Alias: hiddenHavingAggregateName(fn, col.Name, false)}, nil
}

func hiddenHavingAggregateName(fn AggregateFunc, column string, star bool) string {
	if fn == AggregateCount && star {
		return "__having_count_star"
	}
	return "__having_" + defaultAggregateName(fn) + "_" + types.NormalizeName(column)
}

func decomposeAggregateCall(call *FuncCall) (AggregateFunc, string, bool, error) {
	fn, ok := aggregateFuncByName(call.Name)
	if !ok {
		return AggregateInvalid, "", false, fmt.Errorf("unsupported HAVING aggregate %q", call.Name)
	}
	if call.Star {
		if fn != AggregateCount || len(call.Args) != 0 {
			return AggregateInvalid, "", false, fmt.Errorf("HAVING aggregate star is only supported for count(*)")
		}
		return fn, "", true, nil
	}
	if len(call.Args) != 1 {
		return AggregateInvalid, "", false, fmt.Errorf("HAVING aggregate must have one argument")
	}
	col, ok := call.Args[0].(*ColumnRef)
	if !ok {
		return AggregateInvalid, "", false, fmt.Errorf("HAVING aggregate argument must be a column")
	}
	return fn, columnRefKey(col), false, nil
}

func bindGroupExpr(columns map[string]BoundColumnDef, groupBy []Expr) (*BoundExpr, error) {
	if len(groupBy) == 0 {
		return nil, nil
	}
	if len(groupBy) != 1 {
		return nil, fmt.Errorf("only one GROUP BY expression is supported")
	}
	bound, err := bindExpr(columns, groupBy[0])
	if err != nil {
		if col, ok := groupBy[0].(*ColumnRef); ok {
			return nil, fmt.Errorf("missing GROUP BY column %q", col.Name)
		}
		return nil, err
	}
	if bound.Type.Kind == types.KindInvalid {
		return nil, fmt.Errorf("unsupported GROUP BY expression")
	}
	return &bound, nil
}

func bindSelectAggregates(exprs []SelectExpr, columns map[string]BoundColumnDef, grouped bool) ([]AggSpec, bool, error) {
	start := 0
	if grouped {
		start = 1
	}
	if start >= len(exprs) {
		return nil, false, nil
	}
	specs := make([]AggSpec, 0, len(exprs)-start)
	for i := start; i < len(exprs); i++ {
		spec, ok, err := bindSelectAggregate(exprs[i], columns)
		if err != nil {
			return nil, true, err
		}
		if !ok {
			if len(specs) == 0 {
				return nil, false, nil
			}
			return nil, true, fmt.Errorf("aggregate SELECT expressions must be aggregate calls")
		}
		if err := validateAggregateColumn(spec.Func, spec.ArgName, columns); err != nil {
			return nil, true, err
		}
		specs = append(specs, spec)
	}
	return specs, len(specs) != 0, nil
}

func bindSelectAggregate(sel SelectExpr, columns map[string]BoundColumnDef) (AggSpec, bool, error) {
	call, ok := sel.Expr.(*FuncCall)
	if !ok {
		return AggSpec{}, false, nil
	}
	fn, ok := aggregateFuncByName(call.Name)
	if !ok {
		return AggSpec{}, false, nil
	}
	if call.Star {
		if fn != AggregateCount || len(call.Args) != 0 {
			return AggSpec{}, true, fmt.Errorf("aggregate star is only supported for count(*)")
		}
		return AggSpec{Func: fn, Star: true, Alias: sel.Alias}, true, nil
	}
	if len(call.Args) != 1 {
		return AggSpec{}, true, fmt.Errorf("aggregate must have one argument")
	}
	colRef, ok := call.Args[0].(*ColumnRef)
	if !ok {
		return AggSpec{}, true, fmt.Errorf("aggregate argument must be a column")
	}
	key := columnRefKey(colRef)
	def, ok := findColumn(columns, key)
	if !ok {
		return AggSpec{}, true, fmt.Errorf("missing aggregate column %q", key)
	}
	return AggSpec{Func: fn, ArgColumn: def.ID, ArgName: def.Name, Alias: sel.Alias}, true, nil
}

func columnRefKey(c *ColumnRef) string {
	if c.Qualifier != "" {
		return c.Qualifier + "." + c.Name
	}
	return c.Name
}

func validateAggregateColumn(agg AggregateFunc, column string, columns map[string]BoundColumnDef) error {
	if column == "" || agg == AggregateCount {
		return nil
	}
	col, ok := columns[types.NormalizeName(column)]
	if !ok {
		return nil
	}
	switch col.Type.Kind {
	case types.KindInt32, types.KindInt64:
		return nil
	default:
		return fmt.Errorf("%s column %q is %s, want int32 or int64", strings.ToUpper(defaultAggregateName(agg)), col.Name, col.Type)
	}
}

// applyOrderLimit wraps src in Sort -> Project -> Limit as needed. Sort sits BELOW Project so
// its key expressions resolve against base columns; LIMIT/OFFSET are fused into Sort.K/Offset
// when both are present (single-table only), otherwise a separate Limit rel is added.
func applyOrderLimit(src *Rel, stmt *SelectStmt, outputs []BoundOutput, columns map[string]BoundColumnDef) (*Rel, error) {
	limit := int64(-1)
	offset := int64(0)
	hasLimit := stmt.Limit != nil || stmt.Offset != nil
	if stmt.Limit != nil {
		if *stmt.Limit < 0 {
			return nil, fmt.Errorf("LIMIT must be non-negative")
		}
		limit = *stmt.Limit
	}
	if stmt.Offset != nil {
		if *stmt.Offset < 0 {
			return nil, fmt.Errorf("OFFSET must be non-negative")
		}
		offset = *stmt.Offset
	}

	var keys []SortKey
	if len(stmt.OrderBy) != 0 {
		bound, err := bindSortKeys(stmt.OrderBy, outputs, columns)
		if err != nil {
			return nil, err
		}
		keys = bound
	}
	if src.Op == RelScan {
		all, predOnly := scanProjectionColumnIDs(outputs, keys, src.Where)
		narrowScanColumns(src, all)
		src.PredicateOnly = predOnly
	}
	if len(keys) != 0 {
		k, off := int64(0), int64(0)
		if hasLimit && limit >= 0 {
			k = limit
			off = offset
			hasLimit = false
		}
		src = sortRel(src, keys, k, off)
	}
	root := projectRel(src, outputs)
	if hasLimit {
		root = limitRel(root, limit, offset)
	}
	return root, nil
}

func bindSortKeys(orderBy []OrderExpr, outputs []BoundOutput, columns map[string]BoundColumnDef) ([]SortKey, error) {
	keys := make([]SortKey, 0, len(orderBy))
	for _, order := range orderBy {
		name := types.NormalizeName(order.Name)
		matched := false
		for _, output := range outputs {
			if name == types.NormalizeName(outputExprName(output)) {
				keys = append(keys, SortKey{Name: outputExprName(output), Expr: output.Expr, Desc: order.Desc})
				matched = true
				break
			}
		}
		if matched {
			continue
		}
		if order.Expr == nil {
			return nil, fmt.Errorf("ORDER BY column %q must be a selected output column", order.Name)
		}
		bound, err := bindExpr(columns, order.Expr)
		if err != nil {
			return nil, err
		}
		keys = append(keys, SortKey{Expr: bound, Desc: order.Desc})
	}
	return keys, nil
}

func outputExprName(output BoundOutput) string {
	if output.Alias != "" {
		return output.Alias
	}
	if output.Expr.Op == ExprColumn {
		return output.Expr.Column
	}
	return ""
}

func defaultAggregateName(aggregate AggregateFunc) string {
	switch aggregate {
	case AggregateSum:
		return "sum"
	case AggregateMin:
		return "min"
	case AggregateMax:
		return "max"
	default:
		return "count"
	}
}

func aggregateOutputName(spec AggSpec) string {
	if spec.Alias != "" {
		return spec.Alias
	}
	return defaultAggregateName(spec.Func)
}

func groupableKind(kind types.Kind) bool {
	switch kind {
	case types.KindText, types.KindBytes, types.KindUUID, types.KindInt16, types.KindInt32, types.KindInt64, types.KindBool, types.KindDate, types.KindTimestamp, types.KindNamed:
		return true
	default:
		return false
	}
}

func boundExprEqual(left BoundExpr, right BoundExpr) bool {
	if left.Op != right.Op || left.ColumnID != right.ColumnID {
		return false
	}
	switch left.Op {
	case ExprColumn:
		return types.NormalizeName(left.Column) == types.NormalizeName(right.Column)
	case ExprLiteral:
		return left.Literal == right.Literal
	}
	if len(left.Args) != len(right.Args) {
		return false
	}
	for i := range left.Args {
		if !boundExprEqual(left.Args[i], right.Args[i]) {
			return false
		}
	}
	return true
}

// scanProjectionColumnIDs collects ColumnIDs referenced by the SELECT outputs,
// ORDER BY keys, and WHERE predicate of a single-table scan tail. Used to
// narrow a RelScan to only the columns the rest of the plan actually reads.
// predicateOnly lists IDs that come from WHERE alone so the executor can drop
// them from the scan's emit set when the predicate pushes into storage.
func scanProjectionColumnIDs(outputs []BoundOutput, keys []SortKey, where *BoundExpr) (all, predicateOnly []ColumnID) {
	seen := make(map[ColumnID]struct{})
	outputSeen := make(map[ColumnID]struct{})
	var walk func(expr BoundExpr, dst *[]ColumnID, track map[ColumnID]struct{})
	walk = func(expr BoundExpr, dst *[]ColumnID, track map[ColumnID]struct{}) {
		if expr.Op == ExprColumn {
			id := expr.ColumnID
			if id == 0 {
				return
			}
			if _, ok := track[id]; ok {
				return
			}
			track[id] = struct{}{}
			*dst = append(*dst, id)
			return
		}
		for _, arg := range expr.Args {
			walk(arg, dst, track)
		}
	}
	for _, o := range outputs {
		walk(o.Expr, &all, seen)
	}
	for _, k := range keys {
		walk(k.Expr, &all, seen)
	}
	for id := range seen {
		outputSeen[id] = struct{}{}
	}
	if where != nil {
		walk(*where, &all, seen)
	}
	for _, id := range all {
		if _, ok := outputSeen[id]; !ok {
			predicateOnly = append(predicateOnly, id)
		}
	}
	return all, predicateOnly
}

// narrowScanColumns trims a RelScan's Columns and Outputs to the given set
// while preserving table order. A nil or empty ids slice leaves the scan
// untouched so callers do not have to guard against the no-reference case.
func narrowScanColumns(rel *Rel, ids []ColumnID) {
	if rel == nil || rel.Op != RelScan || len(ids) == 0 {
		return
	}
	keep := make(map[ColumnID]struct{}, len(ids))
	for _, id := range ids {
		keep[id] = struct{}{}
	}
	cols := make([]ColumnID, 0, len(ids))
	for _, id := range rel.Columns {
		if _, ok := keep[id]; ok {
			cols = append(cols, id)
		}
	}
	if len(cols) == 0 || len(cols) == len(rel.Columns) {
		return
	}
	outs := make([]BoundOutput, 0, len(cols))
	for _, o := range rel.Outputs {
		if _, ok := keep[o.Expr.ColumnID]; ok {
			outs = append(outs, o)
		}
	}
	rel.Columns = cols
	rel.Outputs = outs
}

func allColumnIDs(columns []BoundColumnDef) []ColumnID {
	ids := make([]ColumnID, 0, len(columns))
	for _, col := range columns {
		ids = append(ids, col.ID)
	}
	return ids
}

