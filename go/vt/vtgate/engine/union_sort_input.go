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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

// Only execution-local copies gain sort keys. A cached plan can execute with
// different session collations concurrently without changing its routes or ASTs.
func (ms *MemorySort) unionSortInput(resolved []collations.TypedCollation, env *collations.Environment) (Primitive, error) {
	if len(resolved) == 0 {
		return ms.Input, nil
	}
	input, ok := ms.Input.(*Concatenate)
	if !ok {
		return nil, vterrors.VT13001("UNION sort input is not a Concatenate")
	}
	clone := *input
	clone.Sources = make([]Primitive, len(input.Sources))
	cols := make([]int, len(ms.OrderBy))
	for i, order := range ms.OrderBy {
		cols[i] = order.Col
	}
	for i, source := range input.Sources {
		var err error
		clone.Sources[i], _, err = unionSortSource(source, cols, resolved, env)
		if err != nil {
			return nil, err
		}
	}
	return &clone, nil
}

func unionSortSource(source Primitive, cols []int, resolved []collations.TypedCollation, env *collations.Environment) (Primitive, []int, error) {
	switch source := source.(type) {
	case *Limit:
		clone := *source
		input, keys, err := unionSortSource(source.Input, cols, resolved, env)
		clone.Input = input
		return &clone, keys, err
	case *Filter:
		clone := *source
		input, keys, err := unionSortSource(source.Input, cols, resolved, env)
		clone.Input = input
		return &clone, keys, err
	case *SimpleProjection:
		mapped := slices.Clone(cols)
		if source.Cols != nil {
			for i, col := range cols {
				mapped[i] = source.Cols[col]
			}
		}
		input, keys, err := unionSortSource(source.Input, mapped, resolved, env)
		if err != nil {
			return nil, nil, err
		}
		clone := *source
		clone.Input = input
		clone.Cols = slices.Clone(source.Cols)
		if clone.Cols == nil {
			clone.Cols = make([]int, keys[0])
			for i := range clone.Cols {
				clone.Cols[i] = i
			}
		}
		width := len(clone.Cols)
		clone.Cols = append(clone.Cols, keys...)
		clone.ColNames = make([]string, len(clone.Cols))
		copy(clone.ColNames, source.ColNames)
		for i := range keys {
			keys[i] = width + i
		}
		return &clone, keys, nil
	case *VindexLookup:
		route, projection, keys, err := unionSortRoute(source.SendTo, cols, resolved, env)
		if err != nil {
			return nil, nil, err
		}
		clone := *source
		clone.SendTo = route
		projection.Input = &clone
		return projection, keys, nil
	case *Route:
		route, projection, keys, err := unionSortRoute(source, cols, resolved, env)
		if err != nil {
			return nil, nil, err
		}
		projection.Input = route
		return projection, keys, nil
	default:
		return nil, nil, vterrors.VT13001("unsupported UNION sort source")
	}
}

func unionSortRoute(source *Route, cols []int, resolved []collations.TypedCollation, env *collations.Environment) (*Route, *SimpleProjection, []int, error) {
	statement, ok := source.QueryStatement.(*sqlparser.Select)
	if !ok {
		return nil, nil, nil, vterrors.VT13001("UNION sort route is not a SELECT")
	}
	physicalWidth := statement.GetColumnCount()
	inner := sqlparser.CloneRefOfSelect(statement)
	derived := &sqlparser.AliasedTableExpr{Expr: &sqlparser.DerivedTable{Select: inner}, As: sqlparser.NewIdentifierCS("__union_values")}
	outer := &sqlparser.Select{From: []sqlparser.TableExpr{derived}}
	// Evaluate the original projections once, before character_set_results
	// encoding. Materialization also keeps volatile expressions and arm-local
	// LIMIT row selection shared between the output and its hidden keys.
	outer.SetComments(sqlparser.Comments{"/*+ NO_MERGE(__union_values) */"})
	comments := statement.GetParsedComments()
	for _, name := range comments.GetMySQLSetVarNames() {
		outer.SetComments(outer.GetParsedComments().SetMySQLSetVarValue(name, comments.GetMySQLSetVarValue(name)))
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
		outer.AddSelectExpr(&sqlparser.AliasedExpr{Expr: &sqlparser.ColName{Name: name}, As: alias})
	}
	for _, order := range source.OrderBy {
		direction := sqlparser.AscOrder
		if order.Desc {
			direction = sqlparser.DescOrder
		}
		outer.OrderBy = append(outer.OrderBy, &sqlparser.Order{Expr: &sqlparser.ColName{Name: derived.Columns[order.Col]}, Direction: direction})
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
		value := &sqlparser.ColName{Name: derived.Columns[col]}
		var key sqlparser.Expr
		if resolved[i].Collation == collations.CollationBinaryID {
			key = &sqlparser.CastExpr{Expr: value, Type: &sqlparser.ConvertType{Type: "binary"}}
		} else {
			cs := env.LookupCharsetName(resolved[i].Collation)
			key = &sqlparser.WeightStringFuncExpr{Expr: &sqlparser.CollateExpr{
				Expr:      &sqlparser.ConvertUsingExpr{Expr: value, Type: cs},
				Collation: env.LookupName(resolved[i].Collation),
			}}
		}
		outer.AddSelectExpr(&sqlparser.AliasedExpr{Expr: key})
		projection.Cols = append(projection.Cols, physicalWidth+i)
		keys[i] = width + i
	}
	clone := *source
	clone.OrderBy = slices.Clone(source.OrderBy)
	clone.QueryStatement = outer
	clone.Query = sqlparser.String(outer)
	clone.FieldQuery = sqlparser.NewTrackedBuffer(sqlparser.FormatImpossibleQuery).WriteNode(outer).ParsedQuery().Query
	clone.TruncateColumnCount = 0
	return &clone, projection, keys, nil
}
