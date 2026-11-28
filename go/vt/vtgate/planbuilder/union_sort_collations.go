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

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
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
	if sort.UnionCollationKnown {
		return nil
	}
	needed := false
	for _, order := range sort.OrderBy {
		needed = needed || order.Type.Type() == sqltypes.Unknown || sqltypes.IsTextOrBinary(order.Type.Type())
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
		probe, err := unionSortCollationQuery(ctx, source.SendTo, offsets)
		if err != nil || probe == nil {
			return probe, err
		}
		route, ok := probe.(*engine.Route)
		if !ok {
			return nil, nil
		}
		// Preserve destination resolution. MemorySort shares the lookup result
		// with the corresponding data arm for this execution.
		return engine.NewUnionSortLookupProbe(source, route), nil
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
		// FieldQuery deliberately drops predicates and DISTINCT. Eligibility
		// must be checked on the original statement before that transformation.
		original := route.QueryStatement
		if original == nil {
			original = selectStmt
		}
		supported := !sqlparser.ContainsAggregation(original)
		_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
			switch node := node.(type) {
			case *sqlparser.Union:
				supported = false
			case *sqlparser.FuncExpr:
				if node.Name.EqualsAnyString(ctx.VSchema.GetAggregateUDFs()) {
					supported = false
				}
			case *sqlparser.Select:
				if node.GroupBy != nil {
					supported = false
				}
			}
			return supported, nil
		}, original)
		if !supported {
			return nil, nil
		}
		selectStmt.SetLimit(&sqlparser.Limit{Rowcount: sqlparser.NewIntLiteral("0")})
		selectStmt.SetLock(sqlparser.NoLock)
		columns := make([]string, selectStmt.GetColumnCount())
		for i := range columns {
			columns[i] = fmt.Sprintf("c%d", i)
		}
		// Retain arm-local ordering metadata too: Route's shard merge runs
		// before an arm-local LIMIT and must use that arm's own collation.
		offsets = append([]int(nil), offsets...)
		for _, order := range route.OrderBy {
			offsets = append(offsets, order.Col)
		}
		arm := sqlparser.String(selectStmt)
		var expressions []string
		for _, offset := range offsets {
			if offset < 0 || offset >= len(columns) {
				return nil, vterrors.VT13001("UNION ordering column is outside its arm")
			}
			scalar := fmt.Sprintf("(select __union_sort.%s from (%s) as __union_sort(%s))", columns[offset], arm, strings.Join(columns, ", "))
			expressions = append(expressions, "collation("+scalar+")", "coercibility("+scalar+")", scalar)
		}
		query := "select " + strings.Join(expressions, ", ")
		statement, err = ctx.VSchema.Environment().Parser().Parse(query)
		if err != nil {
			return nil, vterrors.Wrapf(err, "parsing UNION collation query")
		}
		outer := statement.(*sqlparser.Select)
		outer.SetComments(selectStmt.GetParsedComments().MySQLStatementHints())
		query = sqlparser.String(outer)
		metadata := *route
		metadata.Query, metadata.FieldQuery = query, query
		metadata.QueryStatement = outer
		// The executor chooses between this arm's routing and an arbitrary
		// shard based on whether the session is transactional/reserved.
		metadata.RoutingParameters = route.RoutingParameters
		metadata.OrderBy = nil
		metadata.TruncateColumnCount = 0
		metadata.NoRoutesSpecialHandling = false
		metadata.FetchLastInsertID = false
		return &metadata, nil
	default:
		return nil, nil
	}
}

// Only plain column references are session-independent. Literal expressions
// and explicit COLLATE/CONVERT expressions keep runtime coercibility probing.
func unionSortKnownColumns(ctx *plancontext.PlanningContext, source operators.Operator, sort *engine.MemorySort) bool {
	union, ok := source.(*operators.Union)
	if !ok {
		return false
	}
	input, ok := sort.Input.(*engine.Concatenate)
	if !ok || len(input.Sources) != len(union.Selects) {
		return false
	}
	for i, columns := range union.Selects {
		route, ok := input.Sources[i].(*engine.Route)
		if !ok || len(route.OrderBy) > 0 {
			return false
		}
		stmt, ok := route.QueryStatement.(*sqlparser.Select)
		if !ok || stmt.Limit != nil || ctx.ContainsAggr(stmt) {
			return false
		}
		for _, key := range sort.OrderBy {
			if key.Col >= len(columns) {
				return false
			}
			expr, ok := columns[key.Col].(*sqlparser.AliasedExpr)
			if !ok {
				return false
			}
			if _, ok := expr.Expr.(*sqlparser.ColName); !ok {
				return false
			}
			typ, found := ctx.TypeForExpr(expr.Expr)
			if !found || typ.Type() == sqltypes.Unknown || typ.Type() != key.Type.Type() || typ.Collation() != key.Type.Collation() {
				return false
			}
			if name, _ := ctx.VSchema.Environment().CollationEnv().LookupKnownCollation(typ.Collation()); name == "" {
				return false
			}
		}
	}
	return true
}

// UNION ORDER BY aliases and ordinals both refer to output columns. Infer a
// sort type only when every arm has the same known type and collation. Keeping
// this local to MemorySort avoids changing semantic typing for DISTINCT and
// other consumers of a UNION's projected columns.
func unionSortSetKnownOutputTypes(ctx *plancontext.PlanningContext, source operators.Operator, sort *engine.MemorySort) {
	union, ok := source.(*operators.Union)
	if !ok || len(union.Selects) == 0 {
		return
	}
	for i := range sort.OrderBy {
		col := sort.OrderBy[i].Col
		var common evalengine.Type
		typeKnown, collationKnown := true, true
		for arm, columns := range union.Selects {
			if col < 0 || col >= len(columns) {
				typeKnown = false
				break
			}
			expr, ok := columns[col].(*sqlparser.AliasedExpr)
			if !ok {
				typeKnown = false
				break
			}
			typ, found := ctx.TypeForExpr(expr.Expr)
			if !found || typ.Type() == sqltypes.Unknown {
				typeKnown = false
				break
			}
			if arm == 0 {
				common = typ
			} else {
				if typ.Type() != common.Type() || typ.Scale() != common.Scale() {
					typeKnown = false
					break
				}
				collationKnown = collationKnown && typ.Collation() == common.Collation()
			}
		}
		if typeKnown {
			// UNION result aggregation treats ENUM and SET as VARCHAR. Keeping
			// their arm-local types here can incorrectly push an ENUM-index
			// ordered LIMIT into each arm before the UNION string comparison.
			if common.Type() == sqltypes.Enum || common.Type() == sqltypes.Set {
				common = evalengine.NewTypeEx(sqltypes.VarChar, common.Collation(), common.Nullable(), common.Size(), common.Scale(), nil)
			}
			if !collationKnown {
				common = evalengine.NewType(common.Type(), collations.Unknown)
			}
			sort.OrderBy[i].Type = common
		}
	}
}
