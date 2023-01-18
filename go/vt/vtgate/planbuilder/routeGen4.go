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
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

var _ logicalPlan = (*routeGen4)(nil)

// routeGen4 is used to build a Route primitive.
// It's used to build one of the Select routes like
// SelectScatter, etc. Portions of the original Select AST
// are moved into this node, which will be used to build
// the final SQL for this route.
type routeGen4 struct {
	gen4Plan

	// Select is the AST for the query fragment that will be
	// executed by this route.
	Select sqlparser.SelectStatement

	// condition stores the AST condition that will be used
	// to resolve the ERoute Values field.
	condition sqlparser.Expr

	// eroute is the primitive being built.
	eroute *engine.Route

	// is the engine primitive we will return from the Primitive() method. Note that it could be different than eroute
	enginePrimitive engine.Primitive

	// tables keeps track of which tables this route is covering
	tables semantics.TableSet
}

// Primitive implements the logicalPlan interface
func (rb *routeGen4) Primitive() engine.Primitive {
	return rb.enginePrimitive
}

// SetLimit adds a LIMIT clause to the route.
func (rb *routeGen4) SetLimit(limit *sqlparser.Limit) {
	rb.Select.SetLimit(limit)
}

// WireupGen4 implements the logicalPlan interface
func (rb *routeGen4) WireupGen4(ctx *plancontext.PlanningContext) error {
	rb.prepareTheAST()

	// prepare the queries we will pass down
	rb.eroute.Query = sqlparser.String(rb.Select)
	buffer := sqlparser.NewTrackedBuffer(sqlparser.FormatImpossibleQuery)
	node := buffer.WriteNode(rb.Select)
	parsedQuery := node.ParsedQuery()
	rb.eroute.FieldQuery = parsedQuery.Query

	// if we have a planable vindex lookup, let's extract it into its own primitive
	planableVindex, ok := rb.eroute.RoutingParameters.Vindex.(vindexes.LookupPlanable)
	if !ok {
		rb.enginePrimitive = rb.eroute
		return nil
	}

	query, args := planableVindex.Query()
	stmt, reserved, err := sqlparser.Parse2(query)
	if err != nil {
		return err
	}
	reservedVars := sqlparser.NewReservedVars("vtg", reserved)

	lookupPrimitive, err := gen4SelectStmtPlanner(query, querypb.ExecuteOptions_Gen4, stmt.(sqlparser.SelectStatement), reservedVars, ctx.VSchema)
	if err != nil {
		return vterrors.Wrapf(err, "failed to plan the lookup query: [%s]", query)
	}

	rb.enginePrimitive = &engine.VindexLookup{
		Opcode:    rb.eroute.Opcode,
		Vindex:    planableVindex,
		Keyspace:  rb.eroute.Keyspace,
		Values:    rb.eroute.Values,
		SendTo:    rb.eroute,
		Arguments: args,
		Lookup:    lookupPrimitive.primitive,
	}

	rb.eroute.RoutingParameters.Opcode = engine.ByDestination
	rb.eroute.RoutingParameters.Values = nil
	rb.eroute.RoutingParameters.Vindex = nil

	return nil
}

// ContainsTables implements the logicalPlan interface
func (rb *routeGen4) ContainsTables() semantics.TableSet {
	return rb.tables
}

// OutputColumns implements the logicalPlan interface
func (rb *routeGen4) OutputColumns() []sqlparser.SelectExpr {
	return sqlparser.GetFirstSelect(rb.Select).SelectExprs
}

// prepareTheAST does minor fixups of the SELECT struct before producing the query string
func (rb *routeGen4) prepareTheAST() {
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
			b := node.Operator == sqlparser.EqualOp
			value := sqlparser.IsValue(node.Left)
			name := sqlparser.IsColName(node.Right)
			if b &&
				value &&
				name {
				node.Left, node.Right = node.Right, node.Left
			}
		}
		return true, nil
	}, rb.Select)
}

func (rb *routeGen4) isLocal(col *sqlparser.ColName) bool {
	return col.Metadata.(*column).Origin() == rb
}

// generateFieldQuery generates a query with an impossible where.
// This will be used on the RHS node to fetch field info if the LHS
// returns no result.
func (rb *routeGen4) generateFieldQuery(sel sqlparser.SelectStatement, jt *jointab) string {
	formatter := func(buf *sqlparser.TrackedBuffer, node sqlparser.SQLNode) {
		switch node := node.(type) {
		case *sqlparser.ColName:
			if !rb.isLocal(node) {
				_, joinVar := jt.Lookup(node)
				buf.WriteArg(":", joinVar)
				return
			}
		case sqlparser.TableName:
			if !sqlparser.SystemSchema(node.Qualifier.String()) {
				node.Name.Format(buf)
				return
			}
			node.Format(buf)
			return
		}
		sqlparser.FormatImpossibleQuery(buf, node)
	}

	buffer := sqlparser.NewTrackedBuffer(formatter)
	node := buffer.WriteNode(sel)
	query := node.ParsedQuery()
	return query.Query
}

// Rewrite implements the logicalPlan interface
func (rb *routeGen4) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 0 {
		return vterrors.VT13001("route: wrong number of inputs")
	}
	return nil
}

// Inputs implements the logicalPlan interface
func (rb *routeGen4) Inputs() []logicalPlan {
	return []logicalPlan{}
}

func (rb *routeGen4) isSingleShard() bool {
	return rb.eroute.Opcode.IsSingleShard()
}

func (rb *routeGen4) unionCanMerge(other *routeGen4, distinct bool) bool {
	if rb.eroute.Keyspace.Name != other.eroute.Keyspace.Name {
		return false
	}
	switch rb.eroute.Opcode {
	case engine.Unsharded, engine.Reference:
		return rb.eroute.Opcode == other.eroute.Opcode
	case engine.DBA:
		return other.eroute.Opcode == engine.DBA &&
			len(rb.eroute.SysTableTableSchema) == 0 &&
			len(rb.eroute.SysTableTableName) == 0 &&
			len(other.eroute.SysTableTableSchema) == 0 &&
			len(other.eroute.SysTableTableName) == 0
	case engine.EqualUnique:
		// Check if they target the same shard.
		if other.eroute.Opcode == engine.EqualUnique && rb.eroute.Vindex == other.eroute.Vindex && valEqual(rb.condition, other.condition) {
			return true
		}
	case engine.Scatter:
		return other.eroute.Opcode == engine.Scatter && !distinct
	case engine.Next:
		return false
	}
	return false
}

func (rb *routeGen4) updateRoute(opcode engine.Opcode, vindex vindexes.SingleColumn, condition sqlparser.Expr) {
	rb.eroute.Opcode = opcode
	rb.eroute.Vindex = vindex
	rb.condition = condition
}

// computeNotInPlan looks for null values to produce a SelectNone if found
func (rb *routeGen4) computeNotInPlan(right sqlparser.Expr) engine.Opcode {
	switch node := right.(type) {
	case sqlparser.ValTuple:
		for _, n := range node {
			if sqlparser.IsNull(n) {
				return engine.None
			}
		}
	}

	return engine.Scatter
}

// exprIsValue returns true if the expression can be treated as a value
// for the routeOption. External references are treated as value.
func (rb *routeGen4) exprIsValue(expr sqlparser.Expr) bool {
	if node, ok := expr.(*sqlparser.ColName); ok {
		return node.Metadata.(*column).Origin() != rb
	}
	return sqlparser.IsValue(expr)
}
