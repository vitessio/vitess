/*
Copyright 2017 Google Inc.

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

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// This file has functions to analyze the FROM clause.

// processTableExprs analyzes the FROM clause. It produces a builder
// with all the routes identified.
func (pb *planBuilder) processTableExprs(tableExprs sqlparser.TableExprs) error {
	if len(tableExprs) == 1 {
		return pb.processTableExpr(tableExprs[0])
	}

	if err := pb.processTableExpr(tableExprs[0]); err != nil {
		return err
	}
	rpb := newPlanBuilder(pb.vschema, pb.jt)
	if err := rpb.processTableExprs(tableExprs[1:]); err != nil {
		return err
	}
	return pb.join(rpb, nil)
}

// processTableExpr produces a builder subtree for the given TableExpr.
func (pb *planBuilder) processTableExpr(tableExpr sqlparser.TableExpr) error {
	switch tableExpr := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		return pb.processAliasedTable(tableExpr)
	case *sqlparser.ParenTableExpr:
		err := pb.processTableExprs(tableExpr.Exprs)
		// If it's a route, preserve the parenthesis so things
		// don't associate differently when more things are pushed
		// into it. FROM a, (b, c) should not become FROM a, b, c.
		if rb, ok := pb.bldr.(*route); ok {
			sel := rb.Select.(*sqlparser.Select)
			sel.From = sqlparser.TableExprs{&sqlparser.ParenTableExpr{Exprs: sel.From}}
		}
		return err
	case *sqlparser.JoinTableExpr:
		return pb.processJoin(tableExpr)
	}
	panic(fmt.Sprintf("BUG: unexpected table expression type: %T", tableExpr))
}

// processAliasedTable produces a builder subtree for the given AliasedTableExpr.
// If the expression is a subquery, then the primitive will create a table
// for it in the symtab. If the subquery is a route, then we build a route
// primitive with the subquery in the From clause, because a route is more
// versatile than a subquery. If a subquery becomes a route, then any result
// columns that represent underlying vindex columns are also exposed as
// vindex columns.
func (pb *planBuilder) processAliasedTable(tableExpr *sqlparser.AliasedTableExpr) error {
	switch expr := tableExpr.Expr.(type) {
	case sqlparser.TableName:
		return pb.buildTablePrimitive(tableExpr, expr)
	case *sqlparser.Subquery:
		spb := newPlanBuilder(pb.vschema, pb.jt)
		switch stmt := expr.Select.(type) {
		case *sqlparser.Select:
			if err := spb.processSelect(stmt, nil); err != nil {
				return err
			}
		case *sqlparser.Union:
			if err := spb.processUnion(stmt, nil); err != nil {
				return err
			}
		default:
			panic(fmt.Sprintf("BUG: unexpected SELECT type: %T", stmt))
		}

		subroute, ok := spb.bldr.(*route)
		if !ok {
			pb.bldr, pb.st = newSubquery(tableExpr.As, spb.bldr)
			return nil
		}

		// Since a route is more versatile than a subquery, we
		// build a route primitive that has the subquery in its
		// FROM clause. This allows for other constructs to be
		// later pushed into it.
		table := &vindexes.Table{
			Keyspace: subroute.ERoute.Keyspace,
		}
		for _, rc := range subroute.ResultColumns() {
			if rc.column.Vindex == nil {
				continue
			}
			// Check if a colvindex of the same name already exists.
			// Dups are not allowed in subqueries in this situation.
			for _, colVindex := range table.ColumnVindexes {
				if colVindex.Columns[0].Equal(rc.alias) {
					return fmt.Errorf("duplicate column aliases: %v", rc.alias)
				}
			}
			table.ColumnVindexes = append(table.ColumnVindexes, &vindexes.ColumnVindex{
				Columns: []sqlparser.ColIdent{rc.alias},
				Vindex:  rc.column.Vindex,
			})
		}
		rb, st := newRoute(
			&sqlparser.Select{From: sqlparser.TableExprs([]sqlparser.TableExpr{tableExpr})},
			subroute.ERoute,
			subroute.condition,
		)
		// AddVindexTable can never fail because symtab is empty.
		_ = st.AddVindexTable(sqlparser.TableName{Name: tableExpr.As}, table, rb)
		subroute.Redirect = rb
		pb.bldr, pb.st = rb, st
		return nil
	}
	panic(fmt.Sprintf("BUG: unexpected table expression type: %T", tableExpr.Expr))
}

// buildTablePrimitive builds a primitive based on the table name.
func (pb *planBuilder) buildTablePrimitive(tableExpr *sqlparser.AliasedTableExpr, tableName sqlparser.TableName) error {
	alias := tableName
	if !tableExpr.As.IsEmpty() {
		alias = sqlparser.TableName{Name: tableExpr.As}
	}
	sel := &sqlparser.Select{From: sqlparser.TableExprs([]sqlparser.TableExpr{tableExpr})}

	if systemTable(tableName.Qualifier.String()) {
		ks, err := pb.vschema.DefaultKeyspace()
		if err != nil {
			return err
		}
		rb, st := newRoute(sel, nil, nil)
		rb.ERoute = &engine.Route{
			Opcode:   engine.SelectDBA,
			Keyspace: ks,
		}
		pb.bldr, pb.st = rb, st
		return nil
	}

	table, vindex, _, _, destTarget, err := pb.vschema.FindTableOrVindex(tableName)
	if err != nil {
		return err
	}
	if vindex != nil {
		pb.bldr, pb.st = newVindexFunc(alias, vindex)
		return nil
	}

	rb, st := newRoute(sel, nil, nil)
	pb.bldr, pb.st = rb, st
	// AddVindexTable can never fail because symtab is empty.
	_ = st.AddVindexTable(alias, table, rb)

	if !table.Keyspace.Sharded {
		rb.ERoute = &engine.Route{
			Opcode:   engine.SelectUnsharded,
			Keyspace: table.Keyspace,
		}
		return nil
	}
	if table.Pinned == nil {
		rb.ERoute = &engine.Route{
			Opcode:            engine.SelectScatter,
			Keyspace:          table.Keyspace,
			TargetDestination: destTarget,
		}
		return nil
	}
	// Pinned tables have their keyspace ids already assigned.
	// Use the Binary vindex, which is the identity function
	// for keyspace id. Currently only dual tables are pinned.
	eRoute := &engine.Route{
		Opcode:   engine.SelectEqualUnique,
		Keyspace: table.Keyspace,
	}
	eRoute.Vindex, _ = vindexes.NewBinary("binary", nil)
	eRoute.Values = []sqltypes.PlanValue{{Value: sqltypes.MakeTrusted(sqltypes.VarBinary, table.Pinned)}}
	rb.ERoute = eRoute
	return nil
}

// processJoin produces a builder subtree for the given Join.
// If the left and right nodes can be part of the same route,
// then it's a route. Otherwise, it's a join.
func (pb *planBuilder) processJoin(ajoin *sqlparser.JoinTableExpr) error {
	switch ajoin.Join {
	case sqlparser.JoinStr, sqlparser.StraightJoinStr, sqlparser.LeftJoinStr:
	case sqlparser.RightJoinStr:
		convertToLeftJoin(ajoin)
	default:
		return fmt.Errorf("unsupported: %s", ajoin.Join)
	}
	if err := pb.processTableExpr(ajoin.LeftExpr); err != nil {
		return err
	}
	rpb := newPlanBuilder(pb.vschema, pb.jt)
	if err := rpb.processTableExpr(ajoin.RightExpr); err != nil {
		return err
	}
	return pb.join(rpb, ajoin)
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
	ajoin.Join = sqlparser.LeftJoinStr
}

func (pb *planBuilder) join(rpb *planBuilder, ajoin *sqlparser.JoinTableExpr) error {
	if ajoin != nil && ajoin.Condition.Using != nil {
		return errors.New("unsupported: join with USING(column_list) clause")
	}
	lRoute, leftIsRoute := pb.bldr.(*route)
	rRoute, rightIsRoute := rpb.bldr.(*route)
	if leftIsRoute && rightIsRoute {
		// If both are routes, they have an opportunity
		// to merge into one.
		if lRoute.ERoute.Opcode == engine.SelectNext || rRoute.ERoute.Opcode == engine.SelectNext {
			return errors.New("unsupported: sequence join with another table")
		}
		if lRoute.ERoute.Keyspace.Name != rRoute.ERoute.Keyspace.Name {
			goto nomerge
		}
		switch lRoute.ERoute.Opcode {
		case engine.SelectUnsharded:
			if rRoute.ERoute.Opcode == engine.SelectUnsharded {
				return pb.mergeRoutes(rpb, ajoin)
			}
			return errIntermixingUnsupported
		case engine.SelectDBA:
			if rRoute.ERoute.Opcode == engine.SelectDBA {
				return pb.mergeRoutes(rpb, ajoin)
			}
			return errIntermixingUnsupported
		}

		// Both route are sharded routes. For ',' joins (ajoin==nil), don't
		// analyze mergeability.
		if ajoin == nil {
			goto nomerge
		}

		// Both route are sharded routes. Analyze join condition for merging.
		for _, filter := range splitAndExpression(nil, ajoin.Condition.On) {
			if pb.isSameRoute(rpb, filter) {
				return pb.mergeRoutes(rpb, ajoin)
			}
		}

		// Both l & r routes point to the same shard.
		if lRoute.ERoute.Opcode == engine.SelectEqualUnique && rRoute.ERoute.Opcode == engine.SelectEqualUnique {
			if valEqual(lRoute.condition, rRoute.condition) {
				return pb.mergeRoutes(rpb, ajoin)
			}
		}
	}

nomerge:
	return newJoin(pb, rpb, ajoin)
}

// mergeRoutes merges the two routes. The ON clause is also analyzed to
// see if the primitive can be improved. The operation can fail if
// the expression contains a non-pushable subquery. ajoin can be nil
// if the join is on a ',' operator.
func (pb *planBuilder) mergeRoutes(rpb *planBuilder, ajoin *sqlparser.JoinTableExpr) error {
	lRoute := pb.bldr.(*route)
	rRoute := rpb.bldr.(*route)
	sel := lRoute.Select.(*sqlparser.Select)

	if ajoin == nil {
		rhsSel := rRoute.Select.(*sqlparser.Select)
		sel.From = append(sel.From, rhsSel.From...)
	} else {
		sel.From = sqlparser.TableExprs{ajoin}
		if ajoin.Join == sqlparser.LeftJoinStr {
			rpb.st.ClearVindexes()
		}
	}
	// Redirect before merging the symtabs. Merge will use Redirect
	// to check if rRoute matches lRoute.
	rRoute.Redirect = lRoute
	err := pb.st.Merge(rpb.st)
	if err != nil {
		return err
	}
	if ajoin == nil {
		return nil
	}
	for _, filter := range splitAndExpression(nil, ajoin.Condition.On) {
		// If VTGate evolves, this section should be rewritten
		// to use processExpr.
		_, err = pb.findOrigin(filter)
		if err != nil {
			return err
		}
		lRoute.UpdatePlan(pb, filter)
	}
	return nil
}

// isSameRoute returns true if the join constraint makes the routes
// mergeable by unique vindex. The constraint has to be an equality
// like a.id = b.id where both columns have the same unique vindex.
func (pb *planBuilder) isSameRoute(rpb *planBuilder, filter sqlparser.Expr) bool {
	lRoute := pb.bldr.(*route)
	rRoute := rpb.bldr.(*route)

	filter = skipParenthesis(filter)
	comparison, ok := filter.(*sqlparser.ComparisonExpr)
	if !ok {
		return false
	}
	if comparison.Operator != sqlparser.EqualStr {
		return false
	}
	left := comparison.Left
	right := comparison.Right
	lVindex := pb.st.Vindex(left, lRoute)
	if lVindex == nil {
		left, right = right, left
		lVindex = pb.st.Vindex(left, lRoute)
	}
	if lVindex == nil || !lVindex.IsUnique() {
		return false
	}
	rVindex := rpb.st.Vindex(right, rRoute)
	if rVindex == nil {
		return false
	}
	if rVindex != lVindex {
		return false
	}
	return true
}
