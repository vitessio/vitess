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
	"slices"

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
}

// Only execution-local copies gain sort keys. A cached plan can execute with
// different session collations concurrently without changing its routes or ASTs.
func (ms *MemorySort) unionSortInput(resolved []collations.TypedCollation, arms [][]collations.TypedCollation, env *collations.Environment) (Primitive, *unionSortLayout, error) {
	if len(resolved) == 0 {
		return ms.Input, nil, nil
	}
	input, ok := ms.Input.(*Concatenate)
	if !ok {
		return nil, nil, vterrors.VT13001("UNION sort input is not a Concatenate")
	}
	if len(arms) != len(input.Sources) {
		return nil, nil, vterrors.VT13001("UNION sort metadata does not match its arms")
	}
	clone := *input
	clone.Sources = make([]Primitive, len(input.Sources))
	cols := make([]int, len(ms.OrderBy))
	for i, order := range ms.OrderBy {
		cols[i] = order.Col
	}
	var layout *unionSortLayout
	for i, source := range input.Sources {
		arm, current, err := unionSortSource(source, cols, resolved, arms[i], env)
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
				current = unionSortProjectedLayout(width, len(current.keys))
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

func unionSortSource(source Primitive, cols []int, resolved []collations.TypedCollation, armOrders []collations.TypedCollation, env *collations.Environment) (Primitive, *unionSortLayout, error) {
	switch source := source.(type) {
	case *Limit:
		clone := *source
		input, layout, err := unionSortSource(source.Input, cols, resolved, armOrders, env)
		clone.Input = input
		return &clone, layout, err
	case *Filter:
		input, layout, err := unionSortSource(source.Input, cols, resolved, armOrders, env)
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
		return unionSortProject(&clone, columns, nil, layout), unionSortProjectedLayout(len(columns), len(layout.keys)), nil
	case *SimpleProjection:
		mapped := slices.Clone(cols)
		if source.Cols != nil {
			for i, col := range cols {
				if col < 0 || col >= len(source.Cols) {
					return nil, nil, vterrors.VT13001("UNION ordering column is outside its projection")
				}
				mapped[i] = source.Cols[col]
			}
		}
		input, layout, err := unionSortSource(source.Input, mapped, resolved, armOrders, env)
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
		return unionSortProject(input, columns, source.ColNames, layout), unionSortProjectedLayout(len(columns), len(layout.keys)), nil
	case *VindexLookup:
		route, projection, keys, err := unionSortRoute(source.SendTo, cols, resolved, armOrders, env)
		if err != nil {
			return nil, nil, err
		}
		clone := *source
		clone.SendTo = route
		projection.Input = &clone
		return projection, &unionSortLayout{width: len(projection.Cols) - len(keys), keys: keys}, nil
	case *Route:
		route, projection, keys, err := unionSortRoute(source, cols, resolved, armOrders, env)
		if err != nil {
			return nil, nil, err
		}
		projection.Input = route
		return projection, &unionSortLayout{width: len(projection.Cols) - len(keys), keys: keys}, nil
	default:
		return nil, nil, vterrors.VT13001("unsupported UNION sort source")
	}
}

func unionSortProjectedLayout(width, keyCount int) *unionSortLayout {
	layout := &unionSortLayout{width: width, keys: make([]int, keyCount)}
	for i := range layout.keys {
		layout.keys[i] = width + i
	}
	return layout
}

func unionSortProject(input Primitive, columns []int, names []string, layout *unionSortLayout) *SimpleProjection {
	projection := &SimpleProjection{Input: input, Cols: append(slices.Clone(columns), layout.keys...), ColNames: make([]string, len(columns)+len(layout.keys))}
	copy(projection.ColNames, names)
	return projection
}

func unionSortRoute(source *Route, cols []int, resolved []collations.TypedCollation, armOrders []collations.TypedCollation, env *collations.Environment) (*Route, *SimpleProjection, []int, error) {
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
	keys := make([]int, len(cols))
	for i, col := range cols {
		if col < 0 || col >= width {
			return nil, nil, nil, vterrors.VT13001("UNION ordering column is outside its route")
		}
		outer.AddSelectExpr(&sqlparser.AliasedExpr{Expr: unionSortKey(ref(derived.Columns[col]), resolved[i].Collation, env)})
		projection.Cols = append(projection.Cols, physicalWidth+i)
		keys[i] = width + i
	}
	clone := *source
	clone.OrderBy = slices.Clone(source.OrderBy)
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
	}
	clone.QueryStatement = outer
	clone.Query = sqlparser.String(outer)
	clone.FieldQuery = sqlparser.NewTrackedBuffer(sqlparser.FormatImpossibleQuery).WriteNode(outer).ParsedQuery().Query
	clone.TruncateColumnCount = 0
	return &clone, projection, keys, nil
}

func (layout *unionSortLayout) validate(result *sqltypes.Result) error {
	width := layout.width + len(layout.keys)
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
	return &sqlparser.WeightStringFuncExpr{Expr: &sqlparser.CollateExpr{Expr: &sqlparser.ConvertUsingExpr{Expr: value, Type: env.LookupCharsetName(id)}, Collation: env.LookupName(id)}}
}
