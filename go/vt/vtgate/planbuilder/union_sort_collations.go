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

package planbuilder

import (
	"fmt"
	"strings"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// A split UNION cannot compare arm-local weight strings: MySQL computes them
// under each expression's own collation. Resolve those collations separately
// from result fields, whose charset describes character_set_results instead.
func addUnionSortCollationQueries(ctx *plancontext.PlanningContext, sort *engine.MemorySort) error {
	union, ok := sort.Input.(*engine.Concatenate)
	if !ok {
		return nil
	}
	needed := false
	for _, order := range sort.OrderBy {
		needed = needed || order.Type.Type() == sqltypes.Unknown || sqltypes.IsText(order.Type.Type())
	}
	if !needed {
		return nil
	}
	var queries []engine.Primitive
	for _, source := range union.Sources {
		columns := make([]int, len(sort.OrderBy))
		for i, order := range sort.OrderBy {
			columns[i] = order.Col
		}
		query, err := unionSortCollationQuery(ctx, source, columns)
		if err != nil {
			return err
		}
		if query == nil {
			return nil
		}
		queries = append(queries, query)
	}
	sort.CollationQueries = queries
	return nil
}

// Limits and filters select rows without changing expression collations.
// Projections can reorder offsets, and a lookup must keep ownership of the
// destination it resolves before its route is executed.
func unionSortCollationQuery(ctx *plancontext.PlanningContext, source engine.Primitive, offsets []int) (engine.Primitive, error) {
	switch source := source.(type) {
	case *engine.Limit:
		return unionSortCollationQuery(ctx, source.Input, offsets)
	case *engine.Filter:
		return unionSortCollationQuery(ctx, source.Input, offsets)
	case *engine.SimpleProjection:
		if source.Cols != nil {
			mapped := make([]int, len(offsets))
			for i, offset := range offsets {
				if offset < 0 || offset >= len(source.Cols) {
					return nil, vterrors.VT13001("UNION ordering column is outside its projection")
				}
				mapped[i] = source.Cols[offset]
			}
			offsets = mapped
		}
		return unionSortCollationQuery(ctx, source.Input, offsets)
	case *engine.VindexLookup:
		query, err := unionSortCollationQuery(ctx, source.SendTo, offsets)
		if err != nil || query == nil {
			return nil, err
		}
		lookup := *source
		lookup.SendTo = query.(*engine.Route)
		return &lookup, nil
	case *engine.Route:
		route := source
		// FieldQuery already removes row predicates and their shard-specific
		// IN bindings. A zero-destination metadata read can therefore use the
		// ordinary arbitrary-shard fallback without needing those bindings.
		statement, err := ctx.VSchema.Environment().Parser().Parse(route.FieldQuery)
		if err != nil {
			return nil, vterrors.Wrapf(err, "building UNION collation query")
		}
		selectStmt, ok := statement.(sqlparser.SelectStatement)
		if !ok {
			return nil, vterrors.VT13001("UNION sort input is not a SELECT")
		}
		selectStmt.SetLimit(&sqlparser.Limit{Rowcount: sqlparser.NewIntLiteral("0")})
		selectStmt.SetLock(sqlparser.NoLock)
		columns := make([]string, selectStmt.GetColumnCount())
		for i := range columns {
			columns[i] = fmt.Sprintf("c%d", i)
		}
		arm := sqlparser.String(selectStmt)
		var expressions []string
		for _, offset := range offsets {
			if offset < 0 || offset >= len(columns) {
				return nil, vterrors.VT13001("UNION ordering column is outside its arm")
			}
			scalar := fmt.Sprintf("(select %s from (%s) as __union_sort(%s))", columns[offset], arm, strings.Join(columns, ", "))
			expressions = append(expressions, "collation("+scalar+")", "coercibility("+scalar+")")
		}
		query := "select " + strings.Join(expressions, ", ")
		metadata := *route
		metadata.Query, metadata.FieldQuery = query, query
		metadata.OrderBy = nil
		metadata.TruncateColumnCount = 0
		metadata.NoRoutesSpecialHandling = false
		metadata.FetchLastInsertID = false
		return &metadata, nil
	default:
		return nil, nil
	}
}
