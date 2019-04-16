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
	"fmt"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

// This file has functions to analyze the FROM clause.

// processTableExprs analyzes the FROM clause. It produces a builder
// with all the routes identified.
func (pb *primitiveBuilder) processTableExprs(tableExprs sqlparser.TableExprs) error {
	if len(tableExprs) == 1 {
		return pb.processTableExpr(tableExprs[0])
	}

	if err := pb.processTableExpr(tableExprs[0]); err != nil {
		return err
	}
	rpb := newPrimitiveBuilder(pb.vschema, pb.jt)
	if err := rpb.processTableExprs(tableExprs[1:]); err != nil {
		return err
	}
	return pb.join(rpb, nil)
}

// processTableExpr produces a builder subtree for the given TableExpr.
func (pb *primitiveBuilder) processTableExpr(tableExpr sqlparser.TableExpr) error {
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
func (pb *primitiveBuilder) processAliasedTable(tableExpr *sqlparser.AliasedTableExpr) error {
	switch expr := tableExpr.Expr.(type) {
	case sqlparser.TableName:
		return pb.buildTablePrimitive(tableExpr, expr)
	case *sqlparser.Subquery:
		spb := newPrimitiveBuilder(pb.vschema, pb.jt)
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
			var err error
			pb.bldr, pb.st, err = newSubquery(tableExpr.As, spb.bldr)
			return err
		}

		// Since a route is more versatile than a subquery, we
		// build a route primitive that has the subquery in its
		// FROM clause. This allows for other constructs to be
		// later pushed into it.
		rb, st := newRoute(&sqlparser.Select{From: sqlparser.TableExprs([]sqlparser.TableExpr{tableExpr})})

		// The subquery needs to be represented as a new logical table in the symtab.
		// The new route will inherit the routeOptions of the underlying subquery.
		// For this, we first build new vschema tables based on the columns returned
		// by the subquery, and re-expose possible vindexes. When added to the symtab,
		// a new set of column references will be generated against the new tables,
		// and those vindex maps will be returned. They have to replace the old vindex
		// maps of the inherited route options.
		vschemaTables := make([]*vindexes.Table, 0, len(subroute.routeOptions))
		for _, ro := range subroute.routeOptions {
			vst := &vindexes.Table{
				Keyspace: ro.ERoute.Keyspace,
			}
			vschemaTables = append(vschemaTables, vst)
			for _, rc := range subroute.ResultColumns() {
				vindex, ok := ro.vindexes[rc.column]
				if !ok {
					continue
				}
				// Check if a colvindex of the same name already exists.
				// Dups are not allowed in subqueries in this situation.
				for _, colVindex := range vst.ColumnVindexes {
					if colVindex.Columns[0].Equal(rc.alias) {
						return fmt.Errorf("duplicate column aliases: %v", rc.alias)
					}
				}
				vst.ColumnVindexes = append(vst.ColumnVindexes, &vindexes.ColumnVindex{
					Columns: []sqlparser.ColIdent{rc.alias},
					Vindex:  vindex,
				})
			}
		}
		vindexMaps, err := st.AddVSchemaTable(sqlparser.TableName{Name: tableExpr.As}, vschemaTables, rb)
		if err != nil {
			return err
		}
		for i, ro := range subroute.routeOptions {
			ro.rb = rb
			ro.vindexes = vindexMaps[i]
		}
		rb.routeOptions = subroute.routeOptions
		subroute.Redirect = rb
		pb.bldr, pb.st = rb, st
		return nil
	}
	panic(fmt.Sprintf("BUG: unexpected table expression type: %T", tableExpr.Expr))
}

// buildTablePrimitive builds a primitive based on the table name.
func (pb *primitiveBuilder) buildTablePrimitive(tableExpr *sqlparser.AliasedTableExpr, tableName sqlparser.TableName) error {
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
		rb, st := newRoute(sel)
		rb.routeOptions = []*routeOption{{
			rb:     rb,
			ERoute: engine.NewSimpleRoute(engine.SelectDBA, ks),
		}}
		pb.bldr, pb.st = rb, st
		return nil
	}

	vschemaTables, vindex, _, destTableType, destTarget, err := pb.vschema.FindTablesOrVindex(tableName)
	if err != nil {
		return err
	}
	if vindex != nil {
		pb.bldr, pb.st = newVindexFunc(alias, vindex)
		return nil
	}

	rb, st := newRoute(sel)
	pb.bldr, pb.st = rb, st
	vindexMaps, err := st.AddVSchemaTable(alias, vschemaTables, rb)
	if err != nil {
		return err
	}
	for i, vst := range vschemaTables {
		var eroute *engine.Route
		switch {
		case !vst.Keyspace.Sharded:
			eroute = engine.NewSimpleRoute(engine.SelectUnsharded, vst.Keyspace)
		case vst.Pinned == nil:
			eroute = engine.NewSimpleRoute(engine.SelectScatter, vst.Keyspace)
			eroute.TargetDestination = destTarget
			eroute.TargetTabletType = destTableType
		default:
			// Pinned tables have their keyspace ids already assigned.
			// Use the Binary vindex, which is the identity function
			// for keyspace id. Currently only dual tables are pinned.
			eroute = engine.NewSimpleRoute(engine.SelectEqualUnique, vst.Keyspace)
			eroute.Vindex, _ = vindexes.NewBinary("binary", nil)
			eroute.Values = []sqltypes.PlanValue{{Value: sqltypes.MakeTrusted(sqltypes.VarBinary, vst.Pinned)}}
		}
		rb.routeOptions = append(rb.routeOptions, &routeOption{
			rb:           rb,
			vschemaTable: vst,
			vindexes:     vindexMaps[i],
			ERoute:       eroute,
		})
	}
	return nil
}

// processJoin produces a builder subtree for the given Join.
// If the left and right nodes can be part of the same route,
// then it's a route. Otherwise, it's a join.
func (pb *primitiveBuilder) processJoin(ajoin *sqlparser.JoinTableExpr) error {
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
	rpb := newPrimitiveBuilder(pb.vschema, pb.jt)
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

func (pb *primitiveBuilder) join(rpb *primitiveBuilder, ajoin *sqlparser.JoinTableExpr) error {
	// Merge the symbol tables. In the case of a left join, we have to
	// ideally create new symbols that originate from the join primitive.
	// However, this is not worth it for now, because the Push functions
	// verify that only valid constructs are passed through in case of left join.
	err := pb.st.Merge(rpb.st)
	if err != nil {
		return err
	}

	lRoute, leftIsRoute := pb.bldr.(*route)
	rRoute, rightIsRoute := rpb.bldr.(*route)
	if !leftIsRoute || !rightIsRoute {
		return newJoin(pb, rpb, ajoin)
	}

	// Try merging the routes.
	var mergedRouteOptions []*routeOption
outer:
	for _, lro := range lRoute.routeOptions {
		for _, rro := range rRoute.routeOptions {
			if lro.JoinCanMerge(pb, rro, ajoin) {
				lro.vschemaTable = nil
				lro.substitutions = append(lro.substitutions, rro.substitutions...)
				// Add RHS vindexes only if it's not a left join.
				if ajoin == nil || ajoin.Join != sqlparser.LeftJoinStr {
					for c, v := range rro.vindexes {
						lro.vindexes[c] = v
					}
				}
				mergedRouteOptions = append(mergedRouteOptions, lro)
				continue outer
			}
		}
	}
	if len(mergedRouteOptions) != 0 {
		return pb.mergeRoutes(rpb, mergedRouteOptions, ajoin)
	}

	return newJoin(pb, rpb, ajoin)
}

// mergeRoutes merges the two routes. The ON clause is also analyzed to
// see if the primitive can be improved. The operation can fail if
// the expression contains a non-pushable subquery. ajoin can be nil
// if the join is on a ',' operator.
func (pb *primitiveBuilder) mergeRoutes(rpb *primitiveBuilder, routeOptions []*routeOption, ajoin *sqlparser.JoinTableExpr) error {
	lRoute := pb.bldr.(*route)
	rRoute := rpb.bldr.(*route)
	sel := lRoute.Select.(*sqlparser.Select)

	if ajoin == nil {
		rhsSel := rRoute.Select.(*sqlparser.Select)
		sel.From = append(sel.From, rhsSel.From...)
	} else {
		sel.From = sqlparser.TableExprs{ajoin}
	}
	// Redirect before merging the symtabs. Merge will use Redirect
	// to check if rRoute matches lRoute.
	rRoute.Redirect = lRoute
	// Since the routes have merged, set st.singleRoute to point at
	// the merged route.
	pb.st.singleRoute = lRoute
	lRoute.routeOptions = routeOptions
	if ajoin == nil {
		return nil
	}
	pullouts, _, expr, err := pb.findOrigin(ajoin.Condition.On)
	if err != nil {
		return err
	}
	ajoin.Condition.On = expr
	pb.addPullouts(pullouts)
	for _, filter := range splitAndExpression(nil, ajoin.Condition.On) {
		lRoute.UpdatePlans(pb, filter)
	}
	return nil
}
