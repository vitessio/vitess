/*
Copyright 2026 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package engine

import (
	"fmt"
	"math"
	"slices"
	"strconv"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
)

// unionSortLayout describes the output of a rewritten arm. Original helper
// columns belong to width; keys contains only the newly appended sort keys.
type unionSortLayout struct {
	width int
	keys  []int
	pads  []int
}

// Only execution-local copies gain sort keys. A cached plan can execute with
// different session collations concurrently without changing its routes or ASTs.
func (ms *MemorySort) unionSortInput(metadata *unionSortMetadata, count int, env *collations.Environment) (Primitive, *unionSortLayout, error) {
	input, ok := ms.Input.(*Concatenate)
	if !ok {
		return ms.Input, nil, nil
	}
	// Evalengine's JSON comparison differs from MySQL's JSON ORDER BY. An
	// arm-local LIMIT would therefore discard rows using a different order.
	if unionSortHasJSON(ms.OrderBy) {
		return ms.Input, nil, nil
	}
	if metadata == nil {
		// Known numeric UNIONs need neither metadata nor binary string keys.
		if !unionSortNonText(ms.OrderBy) {
			if unionSortCanCapWithoutMetadata(ms.OrderBy) {
				return unionSortPlainInput(input, ms.OrderBy, count, nil, env), nil, nil
			}
			return ms.Input, nil, nil
		}
		if !unionSortNeedsNumericKeys(ms.OrderBy) {
			return unionSortPlainInput(input, ms.OrderBy, count, nil, env), nil, nil
		}
		if !unionSortSimpleSources(input) {
			return ms.Input, nil, nil
		}
		metadata = &unionSortMetadata{common: make([]collations.TypedCollation, len(ms.OrderBy)), arms: make([][]collations.TypedCollation, len(input.Sources)), types: make([]sqltypes.Type, len(ms.OrderBy)), scales: make([]int32, len(ms.OrderBy)), same: true}
		for i, key := range ms.OrderBy {
			metadata.common[i] = collations.TypedCollation{Collation: key.Type.Collation()}
			metadata.types[i], metadata.scales[i] = key.Type.Type(), key.Type.Scale()
		}
	}
	resolved, arms := metadata.common, metadata.arms
	if len(arms) != len(input.Sources) {
		return nil, nil, vterrors.VT13001("UNION sort metadata does not match its arms")
	}
	global := slices.Clone(ms.OrderBy)
	for i := range global {
		global[i].Type = evalengine.NewTypeEx(metadata.types[i], resolved[i].Collation, true, 0, metadata.scales[i], nil)
	}
	if !unionSortNeedsNumericKeys(global) && unionSortSimpleSources(input) && (unionSortNonText(global) || unionSortRepeatableSources(input) && unionSortReusable(ms.OrderBy, metadata, env)) {
		return unionSortPlainInput(input, global, count, metadata, env), nil, nil
	}
	if metadata.same && unionSortSimpleSources(input) && unionSortRepeatableSources(input) {
		return unionSortNativeInput(input, global, metadata, count, env)
	}
	clone := *input
	clone.Sources = make([]Primitive, len(input.Sources))
	cols := make([]int, len(ms.OrderBy))
	for i, order := range ms.OrderBy {
		cols[i] = order.Col
	}
	var layout *unionSortLayout
	for i, source := range input.Sources {
		arm, current, err := unionSortSource(source, global, resolved, arms[i], count, env)
		if err != nil {
			return nil, nil, err
		}
		// Arm-local sorts can leave different helper columns above a Limit.
		// They have served their purpose at this boundary. Keep the prefix
		// needed by the public result and the global comparison, then keys.
		if ms.TruncateColumnCount > 0 {
			width := ms.TruncateColumnCount
			for _, col := range cols {
				width = max(width, col+1)
			}
			if current.width < width {
				return nil, nil, vterrors.VT13001("UNION sort arm is missing result columns")
			}
			if current.width > width {
				columns := make([]int, width)
				for j := range columns {
					columns[j] = j
				}
				arm = unionSortProject(arm, columns, nil, current)
				current = current.projected(width)
			}
		}
		if layout != nil && (layout.width != current.width || !slices.Equal(layout.keys, current.keys)) {
			return nil, nil, vterrors.VT13001("UNION sort arms have different column layouts")
		}
		clone.Sources[i] = arm
		layout = current
	}
	return &clone, layout, nil
}

func unionSortSource(source Primitive, global evalengine.Comparison, resolved []collations.TypedCollation, armOrders []collations.TypedCollation, count int, env *collations.Environment) (Primitive, *unionSortLayout, error) {
	switch source := source.(type) {
	case *Limit:
		clone := *source
		input, layout, err := unionSortSource(source.Input, global, resolved, armOrders, math.MaxInt, env)
		clone.Input = input
		return &clone, layout, err
	case *Filter:
		input, layout, err := unionSortSource(source.Input, global, resolved, armOrders, math.MaxInt, env)
		if err != nil {
			return nil, nil, err
		}
		clone := *source
		clone.Input = input
		if source.Truncate == 0 {
			return &clone, layout, nil
		}
		if source.Truncate > layout.width {
			return nil, nil, vterrors.VT13001("UNION filter truncation is outside its input")
		}
		clone.Truncate = 0
		columns := make([]int, source.Truncate)
		for i := range columns {
			columns[i] = i
		}
		return unionSortProject(&clone, columns, nil, layout), layout.projected(len(columns)), nil
	case *SimpleProjection:
		mapped := slices.Clone(global)
		if source.Cols != nil {
			for i, order := range global {
				col := order.Col
				if col < 0 || col >= len(source.Cols) {
					return nil, nil, vterrors.VT13001("UNION ordering column is outside its projection")
				}
				mapped[i].Col = source.Cols[col]
			}
		}
		input, layout, err := unionSortSource(source.Input, mapped, resolved, armOrders, count, env)
		if err != nil {
			return nil, nil, err
		}
		columns := source.Cols
		if columns == nil {
			columns = make([]int, layout.width)
			for i := range columns {
				columns[i] = i
			}
		}
		return unionSortProject(input, columns, source.ColNames, layout), layout.projected(len(columns)), nil
	case *VindexLookup:
		route, projection, layout, err := unionSortRoute(source.SendTo, global, resolved, armOrders, count, env)
		if err != nil {
			return nil, nil, err
		}
		clone := *source
		clone.SendTo = route
		projection.Input = &clone
		return projection, layout, nil
	case *Route:
		route, projection, layout, err := unionSortRoute(source, global, resolved, armOrders, count, env)
		if err != nil {
			return nil, nil, err
		}
		projection.Input = route
		return projection, layout, nil
	default:
		return nil, nil, vterrors.VT13001("unsupported UNION sort source")
	}
}

func (layout *unionSortLayout) columnCount() int {
	count := layout.width
	for _, col := range layout.keys {
		count = max(count, col+1)
	}
	for _, col := range layout.pads {
		count = max(count, col+1)
	}
	return count
}

func (layout *unionSortLayout) projected(width int) *unionSortLayout {
	projected := &unionSortLayout{width: width, keys: slices.Clone(layout.keys), pads: slices.Clone(layout.pads)}
	for i := range projected.keys {
		projected.keys[i] += width - layout.width
	}
	for i, col := range projected.pads {
		if col > 0 {
			projected.pads[i] += width - layout.width
		}
	}
	return projected
}

func unionSortProject(input Primitive, columns []int, names []string, layout *unionSortLayout) *SimpleProjection {
	hidden := make([]int, layout.columnCount()-layout.width)
	for i := range hidden {
		hidden[i] = layout.width + i
	}
	projection := &SimpleProjection{Input: input, Cols: append(slices.Clone(columns), hidden...), ColNames: make([]string, len(columns)+len(hidden))}
	copy(projection.ColNames, names)
	return projection
}

func unionSortRoute(source *Route, global evalengine.Comparison, resolved []collations.TypedCollation, armOrders []collations.TypedCollation, count int, env *collations.Environment) (*Route, *SimpleProjection, *unionSortLayout, error) {
	cols := make([]int, len(global))
	for i, order := range global {
		cols[i] = order.Col
	}
	statement, ok := source.QueryStatement.(*sqlparser.Select)
	if !ok {
		return nil, nil, nil, vterrors.VT13001("UNION sort route is not a SELECT")
	}
	physicalWidth := statement.GetColumnCount()
	if len(armOrders) != len(source.OrderBy) {
		return nil, nil, nil, vterrors.VT13001("UNION arm ordering metadata does not match its route")
	}
	if source.TruncateColumnCount > physicalWidth {
		return nil, nil, nil, vterrors.VT13001("UNION route truncation is outside its projection")
	}
	for _, order := range source.OrderBy {
		if order.Col < 0 || order.Col >= physicalWidth {
			return nil, nil, nil, vterrors.VT13001("UNION arm ordering column is outside its route")
		}
	}
	inner := sqlparser.CloneRefOfSelect(statement)
	derived := &sqlparser.AliasedTableExpr{Expr: &sqlparser.DerivedTable{Select: inner}, As: sqlparser.NewIdentifierCS("__union_values")}
	outer := &sqlparser.Select{From: []sqlparser.TableExpr{derived}}
	// Evaluate the original projections once, before character_set_results
	// encoding. Materialization also keeps volatile expressions and arm-local
	// LIMIT row selection shared between the output and its hidden keys.
	outer.SetComments(statement.GetParsedComments().MySQLStatementHints("NO_MERGE(__union_values)"))
	ref := func(name sqlparser.IdentifierCI) *sqlparser.ColName {
		return &sqlparser.ColName{Name: name, Qualifier: sqlparser.TableName{Name: derived.As}}
	}
	for i, expr := range statement.GetColumns() {
		column, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, nil, nil, vterrors.VT13001("UNION sort projection is not expanded")
		}
		name := sqlparser.NewIdentifierCI(fmt.Sprintf("c%d", i))
		derived.Columns = append(derived.Columns, name)
		alias := column.As
		if alias.IsEmpty() {
			if col, ok := column.Expr.(*sqlparser.ColName); ok {
				alias = col.Name
			} else {
				alias = sqlparser.NewIdentifierCI(sqlparser.String(column.Expr))
			}
		}
		outer.AddSelectExpr(&sqlparser.AliasedExpr{Expr: ref(name), As: alias})
	}
	for _, order := range source.OrderBy {
		direction := sqlparser.AscOrder
		if order.Desc {
			direction = sqlparser.DescOrder
		}
		outer.OrderBy = append(outer.OrderBy, &sqlparser.Order{Expr: ref(derived.Columns[order.Col]), Direction: direction})
	}
	width := physicalWidth
	if source.TruncateColumnCount > 0 {
		width = source.TruncateColumnCount
	}
	projection := &SimpleProjection{Cols: make([]int, width), ColNames: make([]string, width+len(cols))}
	for i := range projection.Cols {
		projection.Cols[i] = i
	}
	layout := &unionSortLayout{width: width, keys: make([]int, len(cols)), pads: make([]int, len(cols))}
	for i, col := range cols {
		if col < 0 || col >= width {
			return nil, nil, nil, vterrors.VT13001("UNION ordering column is outside its route")
		}
		outer.AddSelectExpr(&sqlparser.AliasedExpr{Expr: unionSortValueKey(ref(derived.Columns[col]), global[i].Type, resolved[i].Collation, env)})
		projection.Cols = append(projection.Cols, outer.GetColumnCount()-1)
		layout.keys[i] = len(projection.Cols) - 1
		if unionSortNeedsServerSpace(env, resolved[i].Collation) {
			outer.AddSelectExpr(&sqlparser.AliasedExpr{Expr: unionSortKey(sqlparser.NewStrLiteral(" "), resolved[i].Collation, env)})
			projection.Cols = append(projection.Cols, outer.GetColumnCount()-1)
			layout.pads[i] = len(projection.Cols) - 1
		}
	}
	if count < math.MaxInt && statement.Limit == nil {
		outer.OrderBy = nil
		for i, order := range global {
			direction := sqlparser.AscOrder
			if order.Desc {
				direction = sqlparser.DescOrder
			}
			outer.OrderBy = append(outer.OrderBy, &sqlparser.Order{Expr: unionSortValueExpr(ref(derived.Columns[order.Col]), order.Type, resolved[i].Collation, env), Direction: direction})
		}
		outer.Limit = &sqlparser.Limit{Rowcount: sqlparser.NewIntLiteral(strconv.Itoa(count))}
		// Each shard now returns globally ordered rows, so the Route must
		// merge with the same keys before Concatenate sees the arm.
		armOrders = nil
	}
	clone := *source
	clone.unionSortOriginal = source
	clone.OrderBy = slices.Clone(source.OrderBy)
	if outer.Limit != nil {
		clone.OrderBy = slices.Clone(global)
		for i := range clone.OrderBy {
			order := &clone.OrderBy[i]
			order.Col = projection.Cols[layout.keys[i]]
			order.WeightStringCol = -1
			order.PadSpaceWeight = nil
			order.PadSpaceWeightCol = 0
			if sqltypes.IsTextOrBinary(global[i].Type.Type()) {
				order.Type = evalengine.NewType(sqltypes.VarBinary, collations.CollationBinaryID)
				var err error
				order.PadSpaceWeight, err = unionSortPadWeight(env, resolved[i].Collation)
				if err != nil {
					return nil, nil, nil, err
				}
				if layout.pads[i] > 0 {
					order.PadSpaceWeightCol = projection.Cols[layout.pads[i]]
				}
			}
		}
	}
	for i, collation := range armOrders {
		if collation.Collation == collations.CollationBinaryID {
			continue
		}
		order := &clone.OrderBy[i]
		weight, err := unionSortPadWeight(env, collation.Collation)
		if err != nil {
			return nil, nil, nil, err
		}
		outer.AddSelectExpr(&sqlparser.AliasedExpr{Expr: unionSortKey(ref(derived.Columns[order.Col]), collation.Collation, env)})
		order.Col = outer.GetColumnCount() - 1
		order.WeightStringCol = -1
		order.Type = evalengine.NewType(sqltypes.VarBinary, collations.CollationBinaryID)
		order.PadSpaceWeight = weight
		if unionSortNeedsServerSpace(env, collation.Collation) {
			outer.AddSelectExpr(&sqlparser.AliasedExpr{Expr: unionSortKey(sqlparser.NewStrLiteral(" "), collation.Collation, env)})
			order.PadSpaceWeightCol = outer.GetColumnCount() - 1
		}
	}
	clone.QueryStatement = outer
	clone.Query = sqlparser.String(outer)
	clone.FieldQuery = sqlparser.NewTrackedBuffer(sqlparser.FormatImpossibleQuery).WriteNode(outer).ParsedQuery().Query
	clone.TruncateColumnCount = 0
	projection.ColNames = make([]string, len(projection.Cols))
	return &clone, projection, layout, nil
}

func (layout *unionSortLayout) validate(result *sqltypes.Result) error {
	width := layout.columnCount()
	if len(result.Fields) != 0 && len(result.Fields) != width {
		return vterrors.VT13001("UNION sort fields do not match its column layout")
	}
	for _, row := range result.Rows {
		if len(row) != width {
			return vterrors.VT13001("UNION sort row does not match its column layout")
		}
	}
	return nil
}

func unionSortKey(value sqlparser.Expr, id collations.ID, env *collations.Environment) sqlparser.Expr {
	if id == collations.CollationBinaryID {
		return &sqlparser.CastExpr{Expr: value, Type: &sqlparser.ConvertType{Type: "binary"}}
	}
	name, charset := env.LookupKnownCollation(id)
	return &sqlparser.WeightStringFuncExpr{Expr: &sqlparser.CollateExpr{Expr: &sqlparser.ConvertUsingExpr{Expr: value, Type: charset}, Collation: name}}
}

func unionSortNonText(order evalengine.Comparison) bool {
	for _, key := range order {
		if key.Type.Type() == sqltypes.Unknown || key.Type.Type() == sqltypes.TypeJSON || sqltypes.IsTextOrBinary(key.Type.Type()) {
			return false
		}
	}
	return len(order) > 0
}

func unionSortHasJSON(order evalengine.Comparison) bool {
	for _, key := range order {
		if key.Type.Type() == sqltypes.TypeJSON {
			return true
		}
	}
	return false
}

func unionSortCanCapWithoutMetadata(order evalengine.Comparison) bool {
	for _, key := range order {
		if (key.Type.Type() == sqltypes.Unknown || sqltypes.IsTextOrBinary(key.Type.Type())) && key.WeightStringCol < 0 {
			return false
		}
	}
	return true
}

// Floating-point UNION conversion can collapse distinct native values into a
// tie. Generate separate converted keys before applying the global top-N, while
// retaining the original selected expressions and their public field metadata.
func unionSortNeedsNumericKeys(order evalengine.Comparison) bool {
	for _, key := range order {
		if sqltypes.IsFloat(key.Type.Type()) {
			return true
		}
	}
	return false
}

func unionSortSimpleSources(input *Concatenate) bool {
	for _, source := range input.Sources {
		switch source := source.(type) {
		case *VindexLookup:
			sourceRoute := source.SendTo
			if !unionSortPlainRoute(sourceRoute) {
				return false
			}
		case *Route:
			if !unionSortPlainRoute(source) {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func unionSortPlainRoute(route *Route) bool {
	stmt, ok := route.QueryStatement.(*sqlparser.Select)
	return ok && stmt.Limit == nil && !stmt.Distinct && stmt.GroupBy == nil && !sqlparser.ContainsAggregation(stmt) && len(route.OrderBy) == 0
}

func unionSortReusable(order evalengine.Comparison, metadata *unionSortMetadata, env *collations.Environment) bool {
	if !metadata.same {
		return false
	}
	for i, key := range order {
		if sqltypes.IsNumber(metadata.types[i]) {
			continue
		}
		if key.WeightStringCol < 0 || unionSortNeedsServerSpace(env, metadata.common[i].Collation) {
			return false
		}
	}
	return true
}

func unionSortPlainInput(input *Concatenate, order evalengine.Comparison, count int, metadata *unionSortMetadata, env *collations.Environment) Primitive {
	clone := *input
	clone.Sources = make([]Primitive, len(input.Sources))
	for i, source := range input.Sources {
		clone.Sources[i] = unionSortPlainSource(source, order, count, metadata, env)
	}
	return &clone
}

func unionSortPlainSource(source Primitive, order evalengine.Comparison, count int, metadata *unionSortMetadata, env *collations.Environment) Primitive {
	switch source := source.(type) {
	case *Concatenate:
		clone := *source
		clone.Sources = make([]Primitive, len(source.Sources))
		for i, arm := range source.Sources {
			clone.Sources[i] = unionSortPlainSource(arm, order, count, metadata, env)
		}
		return &clone
	case *SimpleProjection:
		mapped := slices.Clone(order)
		if source.Cols != nil {
			for i := range mapped {
				mapped[i].Col = source.Cols[mapped[i].Col]
				if mapped[i].WeightStringCol >= 0 {
					mapped[i].WeightStringCol = source.Cols[mapped[i].WeightStringCol]
				}
			}
		}
		clone := *source
		clone.Input = unionSortPlainSource(source.Input, mapped, count, metadata, env)
		return &clone
	case *VindexLookup:
		clone := *source
		clone.SendTo = unionSortPlainSource(source.SendTo, order, count, metadata, env).(*Route)
		return &clone
	case *Route:
		stmt, ok := source.QueryStatement.(*sqlparser.Select)
		if !ok || stmt.Limit != nil || stmt.Distinct || stmt.GroupBy != nil || sqlparser.ContainsAggregation(stmt) || len(source.OrderBy) > 0 || count == math.MaxInt {
			return source
		}
		if metadata == nil {
			for _, key := range order {
				if (key.Type.Type() == sqltypes.Unknown || sqltypes.IsTextOrBinary(key.Type.Type())) && (key.WeightStringCol < 0 || key.WeightStringCol >= stmt.GetColumnCount()) {
					return source
				}
			}
		}
		query := sqlparser.CloneRefOfSelect(stmt)
		query.OrderBy = nil
		clone := *source
		clone.unionSortOriginal = source
		clone.OrderBy = slices.Clone(order)
		for i, key := range order {
			orderCol := key.Col
			if metadata == nil && (key.Type.Type() == sqltypes.Unknown || sqltypes.IsTextOrBinary(key.Type.Type())) {
				clone.OrderBy[i].Col = key.WeightStringCol
				clone.OrderBy[i].WeightStringCol = -1
				clone.OrderBy[i].Type = evalengine.NewType(sqltypes.VarBinary, collations.CollationBinaryID)
				orderCol = key.WeightStringCol
			}
			if metadata != nil && sqltypes.IsTextOrBinary(key.Type.Type()) {
				clone.OrderBy[i].Col = key.WeightStringCol
				clone.OrderBy[i].WeightStringCol = -1
				clone.OrderBy[i].Type = evalengine.NewType(sqltypes.VarBinary, collations.CollationBinaryID)
				clone.OrderBy[i].PadSpaceWeight, _ = unionSortPadWeight(env, metadata.common[i].Collation)
			}
			direction := sqlparser.AscOrder
			if key.Desc {
				direction = sqlparser.DescOrder
			}
			query.OrderBy = append(query.OrderBy, &sqlparser.Order{Expr: sqlparser.NewIntLiteral(strconv.Itoa(orderCol + 1)), Direction: direction})
		}
		query.Limit = &sqlparser.Limit{Rowcount: sqlparser.NewIntLiteral(strconv.Itoa(count))}
		clone.QueryStatement = query
		clone.Query = sqlparser.String(query)
		clone.FieldQuery = sqlparser.NewTrackedBuffer(sqlparser.FormatImpossibleQuery).WriteNode(query).ParsedQuery().Query
		return &clone
	default:
		return source
	}
}

func unionSortValueExpr(value sqlparser.Expr, typ evalengine.Type, id collations.ID, env *collations.Environment) sqlparser.Expr {
	if sqltypes.IsNumber(typ.Type()) {
		cast := &sqlparser.ConvertType{Type: "signed"}
		switch {
		case sqltypes.IsUnsigned(typ.Type()):
			cast.Type = "unsigned"
		case typ.Type() == sqltypes.Decimal:
			cast.Type = "decimal"
			length, scale := 65, int(typ.Scale())
			cast.Length = &length
			cast.Scale = &scale
		case sqltypes.IsFloat(typ.Type()):
			cast.Type = "double"
		}
		return &sqlparser.CastExpr{Expr: value, Type: cast}
	}
	if !sqltypes.IsTextOrBinary(typ.Type()) && typ.Type() != sqltypes.Unknown {
		return value
	}
	if id == collations.CollationBinaryID {
		return &sqlparser.CastExpr{Expr: value, Type: &sqlparser.ConvertType{Type: "binary"}}
	}
	name, charset := env.LookupKnownCollation(id)
	return &sqlparser.CollateExpr{Expr: &sqlparser.ConvertUsingExpr{Expr: value, Type: charset}, Collation: name}
}

func unionSortValueKey(value sqlparser.Expr, typ evalengine.Type, id collations.ID, env *collations.Environment) sqlparser.Expr {
	if !sqltypes.IsTextOrBinary(typ.Type()) && typ.Type() != sqltypes.Unknown {
		return unionSortValueExpr(value, typ, id, env)
	}
	return unionSortKey(value, id, env)
}

// Shared-collation column projections can generate keys directly, without
// forcing a derived table to materialize. Computed/volatile projections keep
// the materialized path so each selected value is evaluated exactly once.
func unionSortRepeatableSources(input *Concatenate) bool {
	for _, source := range input.Sources {
		route, ok := source.(*Route)
		if !ok {
			route = source.(*VindexLookup).SendTo
		}
		safe := true
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
			if expr, ok := node.(*sqlparser.AliasedExpr); ok {
				switch value := expr.Expr.(type) {
				case *sqlparser.ColName, *sqlparser.Literal, *sqlparser.Argument:
				case *sqlparser.WeightStringFuncExpr:
					if _, ok := value.Expr.(*sqlparser.ColName); !ok {
						safe = false
					}
				default:
					safe = false
				}
			}
			return safe, nil
		}, route.QueryStatement)
		if !safe {
			return false
		}
	}
	return true
}

func unionSortNativeInput(input *Concatenate, global evalengine.Comparison, metadata *unionSortMetadata, count int, env *collations.Environment) (Primitive, *unionSortLayout, error) {
	clone := *input
	clone.Sources = make([]Primitive, len(input.Sources))
	var commonLayout *unionSortLayout
	for n, source := range input.Sources {
		route, ok := source.(*Route)
		if !ok {
			route = source.(*VindexLookup).SendTo
		}
		query := sqlparser.CloneRefOfSelect(route.QueryStatement.(*sqlparser.Select))
		physical := query.GetColumnCount()
		width := physical
		if route.TruncateColumnCount > 0 {
			width = route.TruncateColumnCount
		}
		layout := &unionSortLayout{width: width, keys: make([]int, len(global)), pads: make([]int, len(global))}
		projection := &SimpleProjection{Cols: make([]int, width)}
		for i := range projection.Cols {
			projection.Cols[i] = i
		}
		copyRoute := *route
		copyRoute.unionSortOriginal = route
		copyRoute.OrderBy = slices.Clone(global)
		for i, key := range global {
			value := sqlparser.Clone(query.GetColumns()[key.Col].(*sqlparser.AliasedExpr).Expr)
			query.AddSelectExpr(&sqlparser.AliasedExpr{Expr: unionSortValueKey(value, key.Type, metadata.common[i].Collation, env)})
			physicalKey := query.GetColumnCount() - 1
			projection.Cols = append(projection.Cols, physicalKey)
			layout.keys[i] = len(projection.Cols) - 1
			order := &copyRoute.OrderBy[i]
			order.Col = physicalKey
			order.WeightStringCol = -1
			order.PadSpaceWeight = nil
			order.PadSpaceWeightCol = 0
			if !sqltypes.IsNumber(key.Type.Type()) {
				order.Type = evalengine.NewType(sqltypes.VarBinary, collations.CollationBinaryID)
				var err error
				order.PadSpaceWeight, err = unionSortPadWeight(env, metadata.common[i].Collation)
				if err != nil {
					return nil, nil, err
				}
				if unionSortNeedsServerSpace(env, metadata.common[i].Collation) {
					query.AddSelectExpr(&sqlparser.AliasedExpr{Expr: unionSortKey(sqlparser.NewStrLiteral(" "), metadata.common[i].Collation, env)})
					order.PadSpaceWeightCol = query.GetColumnCount() - 1
					projection.Cols = append(projection.Cols, order.PadSpaceWeightCol)
					layout.pads[i] = len(projection.Cols) - 1
				}
			}
		}
		if count < math.MaxInt {
			query.OrderBy = nil
			for i, key := range global {
				column := key.Col
				if sqltypes.IsNumber(key.Type.Type()) {
					column = copyRoute.OrderBy[i].Col
				}
				direction := sqlparser.AscOrder
				if key.Desc {
					direction = sqlparser.DescOrder
				}
				query.OrderBy = append(query.OrderBy, &sqlparser.Order{Expr: sqlparser.NewIntLiteral(strconv.Itoa(column + 1)), Direction: direction})
			}
			query.Limit = &sqlparser.Limit{Rowcount: sqlparser.NewIntLiteral(strconv.Itoa(count))}
		} else {
			copyRoute.OrderBy = nil
		}
		copyRoute.QueryStatement = query
		copyRoute.Query = sqlparser.String(query)
		copyRoute.FieldQuery = sqlparser.NewTrackedBuffer(sqlparser.FormatImpossibleQuery).WriteNode(query).ParsedQuery().Query
		copyRoute.TruncateColumnCount = 0
		projection.Input = &copyRoute
		if lookup, ok := source.(*VindexLookup); ok {
			copyLookup := *lookup
			copyLookup.SendTo = &copyRoute
			projection.Input = &copyLookup
		}
		projection.ColNames = make([]string, len(projection.Cols))
		clone.Sources[n] = projection
		if commonLayout != nil && (commonLayout.width != layout.width || !slices.Equal(commonLayout.keys, layout.keys) || !slices.Equal(commonLayout.pads, layout.pads)) {
			return nil, nil, vterrors.VT13001("UNION native sort arms have different layouts")
		}
		commonLayout = layout
	}
	return &clone, commonLayout, nil
}
