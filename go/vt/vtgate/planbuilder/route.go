/*
Copyright 2021 The Vitess Authors.

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
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// WireupRoute returns an engine primitive for the given route.
func WireupRoute(ctx *plancontext.PlanningContext, eroute *engine.Route, sel sqlparser.SelectStatement) (engine.Primitive, error) {
	// prepare the queries we will pass down
	eroute.Query = sqlparser.String(sel)
	buffer := sqlparser.NewTrackedBuffer(sqlparser.FormatImpossibleQuery)
	node := buffer.WriteNode(sel)
	eroute.FieldQuery = node.ParsedQuery().Query

	// if we have a planable vindex lookup, let's extract it into its own primitive
	planableVindex, ok := eroute.RoutingParameters.Vindex.(vindexes.LookupPlanable)
	if !ok {
		return eroute, nil
	}

	query, args := planableVindex.Query()
	stmt, reserved, err := ctx.VSchema.Environment().Parser().Parse2(query)
	if err != nil {
		return nil, err
	}
	reservedVars := sqlparser.NewReservedVars("vtg", reserved)

	lookupPrimitive, err := gen4SelectStmtPlanner(query, querypb.ExecuteOptions_Gen4, stmt.(sqlparser.SelectStatement), reservedVars, ctx.VSchema)
	if err != nil {
		return nil, vterrors.Wrapf(err, "failed to plan the lookup query: [%s]", query)
	}

	vdxLookup := &engine.VindexLookup{
		Opcode:    eroute.Opcode,
		Vindex:    planableVindex,
		Keyspace:  eroute.Keyspace,
		Values:    eroute.Values,
		SendTo:    eroute,
		Arguments: args,
		Lookup:    lookupPrimitive.primitive,
	}

	eroute.RoutingParameters.Opcode = engine.ByDestination
	eroute.RoutingParameters.Values = nil
	eroute.RoutingParameters.Vindex = nil

	return vdxLookup, nil
}

// prepareTheAST does minor fixups of the SELECT struct before producing the query string
func prepareTheAST(sel sqlparser.SelectStatement) {
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		switch node := node.(type) {
		case *sqlparser.Select:
			if len(node.SelectExprs) == 0 {
				node.SelectExprs = []sqlparser.SelectExpr{
					&sqlparser.AliasedExpr{
						Expr: sqlparser.NewIntLiteral("1"),
					},
				}
			}
		case *sqlparser.ComparisonExpr:
			// 42 = colName -> colName = 42
			if node.Operator.IsCommutative() &&
				!sqlparser.IsColName(node.Left) &&
				sqlparser.IsColName(node.Right) {
				node.Left, node.Right = node.Right, node.Left
			}
		}
		return true, nil
	}, sel)
}
