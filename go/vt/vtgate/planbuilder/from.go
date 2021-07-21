/*
Copyright 2019 The Vitess Authors.

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
	"errors"
	"fmt"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// This file has functions to analyze the FROM clause.

// processDMLTable analyzes the FROM clause for DMLs and returns a route.
func (pb *primitiveBuilder) processDMLTable(tableExprs sqlparser.TableExprs, reservedVars sqlparser.BindVars, where sqlparser.Expr) (*route, error) {
	if err := pb.processTableExprs(tableExprs, reservedVars, where); err != nil {
		return nil, err
	}
	rb, ok := pb.plan.(*route)
	if !ok {
		return nil, errors.New("unsupported: multi-shard or vindex write statement")
	}
	for _, sub := range rb.substitutions {
		*sub.oldExpr = *sub.newExpr
	}
	return rb, nil
}

// processTableExprs analyzes the FROM clause. It produces a logicalPlan
// with all the routes identified.
func (pb *primitiveBuilder) processTableExprs(tableExprs sqlparser.TableExprs, reservedVars sqlparser.BindVars, where sqlparser.Expr) error {
	if len(tableExprs) == 1 {
		return pb.processTableExpr(tableExprs[0], reservedVars, where)
	}

	if err := pb.processTableExpr(tableExprs[0], reservedVars, where); err != nil {
		return err
	}
	rpb := newPrimitiveBuilder(pb.vschema, pb.jt)
	if err := rpb.processTableExprs(tableExprs[1:], reservedVars, where); err != nil {
		return err
	}
	return pb.join(rpb, nil, reservedVars, where)
}

// processTableExpr produces a logicalPlan subtree for the given TableExpr.
func (pb *primitiveBuilder) processTableExpr(tableExpr sqlparser.TableExpr, reservedVars sqlparser.BindVars, where sqlparser.Expr) error {
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		return pb.processAliasedTable(tableExpr, reservedVars)
	case *sqlparser.ParenTableExpr:
		err := pb.processTableExprs(tableExpr.Exprs, reservedVars, where)
		// If it's a route, preserve the parenthesis so things
		// don't associate differently when more things are pushed
		// into it. FROM a, (b, c) should not become FROM a, b, c.
		if rb, ok := pb.plan.(*route); ok {
			sel, ok := rb.Select.(*sqlparser.Select)
			if !ok {
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected AST struct for query: %s", sqlparser.String(rb.Select))
			}

			sel.From = sqlparser.TableExprs{&sqlparser.ParenTableExpr{Exprs: sel.From}}
		}
		return err
	case *sqlparser.JoinTableExpr:
		return pb.processJoin(tableExpr, reservedVars, where)
	}
	return fmt.Errorf("BUG: unexpected table expression type: %T", tableExpr)
}

// processAliasedTable produces a logicalPlan subtree for the given AliasedTableExpr.
// If the expression is a subquery, then the primitive will create a table
// for it in the symtab. If the subquery is a route, then we build a route
// primitive with the subquery in the From clause, because a route is more
// versatile than a subquery. If a subquery becomes a route, then any result
// columns that represent underlying vindex columns are also exposed as
// vindex columns.
func (pb *primitiveBuilder) processAliasedTable(tableExpr *sqlparser.AliasedTableExpr, reservedVars sqlparser.BindVars) error {
	switch expr := tableExpr.Expr.(type) {
	case sqlparser.TableName:
		return pb.buildTablePrimitive(tableExpr, expr)
	case *sqlparser.DerivedTable:
		spb := newPrimitiveBuilder(pb.vschema, pb.jt)
		switch stmt := expr.Select.(type) {
		case *sqlparser.Select:
			if err := spb.processSelect(stmt, reservedVars, nil, ""); err != nil {
				return err
			}
		case *sqlparser.Union:
			if err := spb.processUnion(stmt, reservedVars, nil); err != nil {
				return err
			}
		default:
			return fmt.Errorf("BUG: unexpected SELECT type: %T", stmt)
		}

		subroute, ok := spb.plan.(*route)
		if !ok {
			var err error
			pb.plan, pb.st, err = newSubquery(tableExpr.As, spb.plan)
			if err != nil {
				return err
			}
			pb.plan.Reorder(0)
			return nil
		}

		// Since a route is more versatile than a subquery, we
		// build a route primitive that has the subquery in its
		// FROM clause. This allows for other constructs to be
		// later pushed into it.
		rb, st := newRoute(&sqlparser.Select{From: []sqlparser.TableExpr{tableExpr}})
		rb.substitutions = subroute.substitutions
		rb.condition = subroute.condition
		rb.eroute = subroute.eroute
		subroute.Redirect = rb

		// The subquery needs to be represented as a new logical table in the symtab.
		// The new route will inherit the routeOptions of the underlying subquery.
		// For this, we first build new vschema tables based on the columns returned
		// by the subquery, and re-expose possible vindexes. When added to the symtab,
		// a new set of column references will be generated against the new tables,
		// and those vindex maps will be returned. They have to replace the old vindex
		// maps of the inherited route options.
		vschemaTable := &vindexes.Table{
			Keyspace: subroute.eroute.Keyspace,
		}
		for _, rc := range subroute.ResultColumns() {
			if rc.column.vindex == nil {
				continue
			}
			// Check if a colvindex of the same name already exists.
			// Dups are not allowed in subqueries in this situation.
			for _, colVindex := range vschemaTable.ColumnVindexes {
				if colVindex.Columns[0].Equal(rc.alias) {
					return fmt.Errorf("duplicate column aliases: %v", rc.alias)
				}
			}
			vschemaTable.ColumnVindexes = append(vschemaTable.ColumnVindexes, &vindexes.ColumnVindex{
				Columns: []sqlparser.ColIdent{rc.alias},
				Vindex:  rc.column.vindex,
			})
		}
		if err := st.AddVSchemaTable(sqlparser.TableName{Name: tableExpr.As}, vschemaTable, rb); err != nil {
			return err
		}

		pb.plan, pb.st = rb, st
		return nil
	}
	return fmt.Errorf("BUG: unexpected table expression type: %T", tableExpr.Expr)
}

// buildTablePrimitive builds a primitive based on the table name.
func (pb *primitiveBuilder) buildTablePrimitive(tableExpr *sqlparser.AliasedTableExpr, tableName sqlparser.TableName) error {
	alias := tableName
	if !tableExpr.As.IsEmpty() {
		alias = sqlparser.TableName{Name: tableExpr.As}
	}
	sel := &sqlparser.Select{From: sqlparser.TableExprs([]sqlparser.TableExpr{tableExpr})}

	if sqlparser.SystemSchema(tableName.Qualifier.String()) {
		ks, err := pb.vschema.AnyKeyspace()
		if err != nil {
			return err
		}
		rb, st := newRoute(sel)
		rb.eroute = engine.NewSimpleRoute(engine.SelectDBA, ks)
		pb.plan, pb.st = rb, st
		// Add the table to symtab
		return st.AddTable(&table{
			alias:  alias,
			origin: rb,
		})
	}

	vschemaTable, vindex, _, destTableType, destTarget, err := pb.vschema.FindTableOrVindex(tableName)
	if err != nil {
		return err
	}
	if vindex != nil {
		single, ok := vindex.(vindexes.SingleColumn)
		if !ok {
			return fmt.Errorf("multi-column vindexes not supported")
		}
		pb.plan, pb.st = newVindexFunc(alias, single)
		return nil
	}

	rb, st := newRoute(sel)
	pb.plan, pb.st = rb, st
	if err := st.AddVSchemaTable(alias, vschemaTable, rb); err != nil {
		return err
	}

	sub := &tableSubstitution{
		oldExpr: tableExpr,
	}
	if tableExpr.As.IsEmpty() {
		if tableName.Name != vschemaTable.Name {
			// Table name does not match. Change and alias it to old name.
			sub.newExpr = &sqlparser.AliasedTableExpr{
				Expr: sqlparser.TableName{Name: vschemaTable.Name},
				As:   tableName.Name,
			}
		}
	} else {
		// Table is already aliased.
		if tableName.Name != vschemaTable.Name {
			// Table name does not match. Change it and reuse existing alias.
			sub.newExpr = &sqlparser.AliasedTableExpr{
				Expr: sqlparser.TableName{Name: vschemaTable.Name},
				As:   tableExpr.As,
			}
		}
	}
	if sub != nil && sub.newExpr != nil {
		rb.substitutions = []*tableSubstitution{sub}
	}

	var eroute *engine.Route
	switch {
	case vschemaTable.Type == vindexes.TypeSequence:
		eroute = engine.NewSimpleRoute(engine.SelectNext, vschemaTable.Keyspace)
	case vschemaTable.Type == vindexes.TypeReference:
		eroute = engine.NewSimpleRoute(engine.SelectReference, vschemaTable.Keyspace)
	case !vschemaTable.Keyspace.Sharded:
		eroute = engine.NewSimpleRoute(engine.SelectUnsharded, vschemaTable.Keyspace)
	case vschemaTable.Pinned == nil:
		eroute = engine.NewSimpleRoute(engine.SelectScatter, vschemaTable.Keyspace)
		eroute.TargetDestination = destTarget
		eroute.TargetTabletType = destTableType
	default:
		// Pinned tables have their keyspace ids already assigned.
		// Use the Binary vindex, which is the identity function
		// for keyspace id.
		eroute = engine.NewSimpleRoute(engine.SelectEqualUnique, vschemaTable.Keyspace)
		vindex, _ = vindexes.NewBinary("binary", nil)
		eroute.Vindex, _ = vindex.(vindexes.SingleColumn)
		eroute.Values = []sqltypes.PlanValue{{Value: sqltypes.MakeTrusted(sqltypes.VarBinary, vschemaTable.Pinned)}}
	}
	eroute.TableName = sqlparser.String(vschemaTable.Name)
	rb.eroute = eroute

	return nil
}

// processJoin produces a logicalPlan subtree for the given Join.
// If the left and right nodes can be part of the same route,
// then it's a route. Otherwise, it's a join.
func (pb *primitiveBuilder) processJoin(ajoin *sqlparser.JoinTableExpr, reservedVars sqlparser.BindVars, where sqlparser.Expr) error {
	switch ajoin.Join {
	case sqlparser.NormalJoinType, sqlparser.StraightJoinType, sqlparser.LeftJoinType:
	case sqlparser.RightJoinType:
		convertToLeftJoin(ajoin)
	default:
		return fmt.Errorf("unsupported: %s", ajoin.Join.ToString())
	}
	if err := pb.processTableExpr(ajoin.LeftExpr, reservedVars, where); err != nil {
		return err
	}
	rpb := newPrimitiveBuilder(pb.vschema, pb.jt)
	if err := rpb.processTableExpr(ajoin.RightExpr, reservedVars, where); err != nil {
		return err
	}
	return pb.join(rpb, ajoin, reservedVars, where)
}

// convertToLeftJoin converts a right join into a left join.
func convertToLeftJoin(ajoin *sqlparser.JoinTableExpr) {
	newRHS := ajoin.LeftExpr
	// If the LHS is a join, we have to parenthesize it.
	// Otherwise, it can be used as is.
	if _, ok := newRHS.(*sqlparser.JoinTableExpr); ok {
		newRHS = &sqlparser.ParenTableExpr{
			Exprs: sqlparser.TableExprs{newRHS},
		}
	}
	ajoin.LeftExpr, ajoin.RightExpr = ajoin.RightExpr, newRHS
	ajoin.Join = sqlparser.LeftJoinType
}

func (pb *primitiveBuilder) join(rpb *primitiveBuilder, ajoin *sqlparser.JoinTableExpr, reservedVars sqlparser.BindVars, where sqlparser.Expr) error {
	// Merge the symbol tables. In the case of a left join, we have to
	// ideally create new symbols that originate from the join primitive.
	// However, this is not worth it for now, because the Push functions
	// verify that only valid constructs are passed through in case of left join.
	err := pb.st.Merge(rpb.st)
	if err != nil {
		return err
	}

	lRoute, leftIsRoute := pb.plan.(*route)
	rRoute, rightIsRoute := rpb.plan.(*route)
	if !leftIsRoute || !rightIsRoute {
		return newJoin(pb, rpb, ajoin, reservedVars)
	}

	// Try merging the routes.
	if !lRoute.JoinCanMerge(pb, rRoute, ajoin, where) {
		return newJoin(pb, rpb, ajoin, reservedVars)
	}

	if lRoute.eroute.Opcode == engine.SelectReference {
		// Swap the conditions & eroutes, and then merge.
		lRoute.condition, rRoute.condition = rRoute.condition, lRoute.condition
		lRoute.eroute, rRoute.eroute = rRoute.eroute, lRoute.eroute
	}
	lRoute.substitutions = append(lRoute.substitutions, rRoute.substitutions...)
	rRoute.Redirect = lRoute

	// Merge the AST.
	sel, ok := lRoute.Select.(*sqlparser.Select)
	if !ok {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected AST struct for query: %s", sqlparser.String(lRoute.Select))
	}
	if ajoin == nil {
		rhsSel, ok := rRoute.Select.(*sqlparser.Select)
		if !ok {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected AST struct for query: %s", sqlparser.String(rRoute.Select))
		}
		sel.From = append(sel.From, rhsSel.From...)
	} else {
		sel.From = sqlparser.TableExprs{ajoin}
	}

	// Since the routes have merged, set st.singleRoute to point at
	// the merged route.
	pb.st.singleRoute = lRoute
	if ajoin == nil {
		return nil
	}
	pullouts, _, expr, err := pb.findOrigin(ajoin.Condition.On, reservedVars)
	if err != nil {
		return err
	}
	ajoin.Condition.On = expr
	pb.addPullouts(pullouts)
	for _, filter := range splitAndExpression(nil, ajoin.Condition.On) {
		lRoute.UpdatePlan(pb, filter)
	}
	return nil
}
