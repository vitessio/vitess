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

	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"

	"vitess.io/vitess/go/vt/vtgate/semantics"

	"vitess.io/vitess/go/vt/key"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func buildSelectPlan(query string) func(sqlparser.Statement, sqlparser.BindVars, ContextVSchema) (engine.Primitive, error) {
	return func(stmt sqlparser.Statement, reservedVars sqlparser.BindVars, vschema ContextVSchema) (engine.Primitive, error) {
		sel := stmt.(*sqlparser.Select)

		p, err := handleDualSelects(sel, vschema)
		if err != nil {
			return nil, err
		}
		if p != nil {
			return p, nil
		}

		getPlan := func(sel *sqlparser.Select) (logicalPlan, error) {
			pb := newPrimitiveBuilder(vschema, newJointab(reservedVars))
			if err := pb.processSelect(sel, reservedVars, nil, query); err != nil {
				return nil, err
			}
			if err := pb.plan.Wireup(pb.plan, pb.jt); err != nil {
				return nil, err
			}
			return pb.plan, nil
		}

		plan, err := getPlan(sel)
		if err != nil {
			return nil, err
		}

		if shouldRetryWithCNFRewriting(plan) {
			// by transforming the predicates to CNF, the planner will sometimes find better plans
			primitive := rewriteToCNFAndReplan(stmt, getPlan)
			if primitive != nil {
				return primitive, nil
			}
		}
		return plan.Primitive(), nil
	}
}

func rewriteToCNFAndReplan(stmt sqlparser.Statement, getPlan func(sel *sqlparser.Select) (logicalPlan, error)) engine.Primitive {
	rewritten := sqlparser.RewriteToCNF(stmt)
	sel2, isSelect := rewritten.(*sqlparser.Select)
	if isSelect {
		log.Infof("retrying plan after cnf: %s", sqlparser.String(sel2))
		plan2, err := getPlan(sel2)
		if err == nil && !shouldRetryWithCNFRewriting(plan2) {
			// we only use this new plan if it's better than the old one we got
			return plan2.Primitive()
		}
	}
	return nil
}

func shouldRetryWithCNFRewriting(plan logicalPlan) bool {
	routePlan, isRoute := plan.(*route)
	if !isRoute {
		return false
	}
	// if we have a I_S query, but have not found table_schema or table_name, let's try CNF
	return routePlan.eroute.Opcode == engine.SelectDBA &&
		routePlan.eroute.SysTableTableName == nil &&
		routePlan.eroute.SysTableTableSchema == nil

}

func pushProjection(expr *sqlparser.AliasedExpr, plan logicalPlan, semTable *semantics.SemTable) (firstOffset int, err error) {
	switch node := plan.(type) {
	case *route:
		sel := node.Select.(*sqlparser.Select)
		offset := len(sel.SelectExprs)
		sel.SelectExprs = append(sel.SelectExprs, expr)
		return offset, nil
	case *joinV4:
		lhsSolves := node.Left.ContainsTables()
		rhsSolves := node.Right.ContainsTables()
		deps := semTable.Dependencies(expr.Expr)
		switch {
		case deps.IsSolvedBy(lhsSolves):
			offset, err := pushProjection(expr, node.Left, semTable)
			if err != nil {
				return 0, err
			}
			node.Cols = append(node.Cols, -(offset + 1))
		case deps.IsSolvedBy(rhsSolves):
			offset, err := pushProjection(expr, node.Right, semTable)
			if err != nil {
				return 0, err
			}
			node.Cols = append(node.Cols, offset+1)
		default:
			return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unknown dependencies for %s", sqlparser.String(expr))
		}
		return len(node.Cols) - 1, nil
	default:
		return 0, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%T not yet supported", node)
	}
}

func pushPredicate(exprs []sqlparser.Expr, plan logicalPlan, semTable *semantics.SemTable) (err error) {
	if len(exprs) == 0 {
		return nil
	}
	switch node := plan.(type) {
	case *route:
		sel := node.Select.(*sqlparser.Select)
		finalExpr := reorderExpression(exprs[0], node.tables, semTable)
		for i, expr := range exprs {
			if i == 0 {
				continue
			}
			finalExpr = &sqlparser.AndExpr{
				Left:  finalExpr,
				Right: reorderExpression(expr, node.tables, semTable),
			}
		}
		if sel.Where != nil {
			finalExpr = &sqlparser.AndExpr{
				Left:  sel.Where.Expr,
				Right: finalExpr,
			}
		}
		sel.Where = &sqlparser.Where{
			Type: sqlparser.WhereClause,
			Expr: finalExpr,
		}
		return nil
	case *joinV4:
		var lhs, rhs []sqlparser.Expr
		lhsSolves := node.Left.ContainsTables()
		rhsSolves := node.Right.ContainsTables()
		for _, expr := range exprs {
			deps := semTable.Dependencies(expr)
			switch {
			case deps.IsSolvedBy(lhsSolves):
				lhs = append(lhs, expr)
			case deps.IsSolvedBy(rhsSolves):
				rhs = append(rhs, expr)
			default:
				return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unknown dependencies for %s", sqlparser.String(expr))
			}
		}
		err := pushPredicate(lhs, node.Left, semTable)
		if err != nil {
			return err
		}
		err = pushPredicate(rhs, node.Right, semTable)
		return err
	default:
		return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%T not yet supported", node)
	}
}

func reorderExpression(expr sqlparser.Expr, solves semantics.TableSet, semTable *semantics.SemTable) sqlparser.Expr {
	switch compExpr := expr.(type) {
	case *sqlparser.ComparisonExpr:
		if compExpr.Operator == sqlparser.EqualOp {
			if !dependsOnRoute(solves, compExpr.Left, semTable) && dependsOnRoute(solves, compExpr.Right, semTable) {
				compExpr.Left, compExpr.Right = compExpr.Right, compExpr.Left
			}
		}
	}
	return expr
}

func dependsOnRoute(solves semantics.TableSet, expr sqlparser.Expr, semTable *semantics.SemTable) bool {
	if node, ok := expr.(*sqlparser.ColName); ok {
		return semTable.Dependencies(node).IsSolvedBy(solves)
	}
	return !sqlparser.IsValue(expr)
}

var errSQLCalcFoundRows = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.CantUseOptionHere, "Incorrect usage/placement of 'SQL_CALC_FOUND_ROWS'")
var errInto = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.CantUseOptionHere, "Incorrect usage/placement of 'INTO'")

// processSelect builds a primitive tree for the given query or subquery.
// The tree built by this function has the following general structure:
//
// The leaf nodes can be a route, vindexFunc or subquery. In the symtab,
// the tables map has columns that point to these leaf nodes. A subquery
// itself contains a logicalPlan tree, but it's opaque and is made to look
// like a table for the analysis of the current tree.
//
// The leaf nodes are usually tied together by join nodes. While the join
// nodes are built, they have ON clauses. Those are analyzed and pushed
// down into the leaf nodes as the tree is formed. Join nodes are formed
// during analysis of the FROM clause.
//
// During the WHERE clause analysis, the target leaf node is identified
// for each part, and the PushFilter function is used to push the condition
// down. The same strategy is used for the other clauses.
//
// So, a typical plan would either be a simple leaf node, or may consist
// of leaf nodes tied together by join nodes.
//
// If a query has aggregates that cannot be pushed down, an aggregator
// primitive is built. The current orderedAggregate primitive can only
// be built on top of a route. The orderedAggregate expects the rows
// to be ordered as they are returned. This work is performed by the
// underlying route. This means that a compatible ORDER BY clause
// can also be handled by this combination of primitives. In this case,
// the tree would consist of an orderedAggregate whose input is a route.
//
// If a query has an ORDER BY, but the route is a scatter, then the
// ordering is pushed down into the route itself. This results in a simple
// route primitive.
//
// The LIMIT clause is the last construct of a query. If it cannot be
// pushed into a route, then a primitive is created on top of any
// of the above trees to make it discard unwanted rows.
func (pb *primitiveBuilder) processSelect(sel *sqlparser.Select, reservedVars sqlparser.BindVars, outer *symtab, query string) error {
	// Check and error if there is any locking function present in select expression.
	for _, expr := range sel.SelectExprs {
		if aExpr, ok := expr.(*sqlparser.AliasedExpr); ok && sqlparser.IsLockingFunc(aExpr.Expr) {
			return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%v allowed only with dual", sqlparser.String(aExpr))
		}
	}
	if sel.SQLCalcFoundRows {
		if outer != nil || query == "" {
			return errSQLCalcFoundRows
		}
		sel.SQLCalcFoundRows = false
		if sel.Limit != nil {
			plan, err := buildSQLCalcFoundRowsPlan(query, sel, reservedVars, outer, pb.vschema)
			if err != nil {
				return err
			}
			pb.plan = plan
			return nil
		}
	}

	// Into is not supported in subquery.
	if sel.Into != nil && (outer != nil || query == "") {
		return errInto
	}

	var where sqlparser.Expr
	if sel.Where != nil {
		where = sel.Where.Expr
	}
	if err := pb.processTableExprs(sel.From, reservedVars, where); err != nil {
		return err
	}

	if rb, ok := pb.plan.(*route); ok {
		// TODO(sougou): this can probably be improved.
		directives := sqlparser.ExtractCommentDirectives(sel.Comments)
		rb.eroute.QueryTimeout = queryTimeout(directives)
		if rb.eroute.TargetDestination != nil {
			return errors.New("unsupported: SELECT with a target destination")
		}

		if directives.IsSet(sqlparser.DirectiveScatterErrorsAsWarnings) {
			rb.eroute.ScatterErrorsAsWarnings = true
		}
	}

	// Set the outer symtab after processing of FROM clause.
	// This is because correlation is not allowed there.
	pb.st.Outer = outer
	if sel.Where != nil {
		if err := pb.pushFilter(sel.Where.Expr, sqlparser.WhereStr, reservedVars); err != nil {
			return err
		}
	}
	if err := pb.checkAggregates(sel); err != nil {
		return err
	}
	if err := pb.pushSelectExprs(sel, reservedVars); err != nil {
		return err
	}
	if sel.Having != nil {
		if err := pb.pushFilter(sel.Having.Expr, sqlparser.HavingStr, reservedVars); err != nil {
			return err
		}
	}
	if err := pb.pushOrderBy(sel.OrderBy); err != nil {
		return err
	}
	if err := pb.pushLimit(sel.Limit); err != nil {
		return err
	}

	return setMiscFunc(pb.plan, sel)
}

func setMiscFunc(in logicalPlan, sel *sqlparser.Select) error {
	_, err := visit(in, func(plan logicalPlan) (bool, logicalPlan, error) {
		switch node := plan.(type) {
		case *route:
			query, ok := node.Select.(*sqlparser.Select)
			if !ok {
				return false, nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unexpected AST struct for query: %T", node.Select)
			}
			query.Comments = sel.Comments
			query.Lock = sel.Lock
			if sel.Into != nil {
				if node.eroute.Opcode != engine.SelectUnsharded {
					return false, nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "INTO is not supported on sharded keyspace")
				}
				query.Into = sel.Into
			}
			return true, node, nil
		}
		return true, plan, nil
	})

	if err != nil {
		return err
	}
	return nil
}

func buildSQLCalcFoundRowsPlan(query string, sel *sqlparser.Select, reservedVars sqlparser.BindVars, outer *symtab, vschema ContextVSchema) (logicalPlan, error) {
	ljt := newJointab(reservedVars)
	frpb := newPrimitiveBuilder(vschema, ljt)
	err := frpb.processSelect(sel, reservedVars, outer, "")
	if err != nil {
		return nil, err
	}

	statement2, reservedVars2, err := sqlparser.Parse2(query)
	if err != nil {
		return nil, err
	}
	sel2 := statement2.(*sqlparser.Select)

	sel2.SQLCalcFoundRows = false
	sel2.OrderBy = nil
	sel2.Limit = nil

	countStartExpr := []sqlparser.SelectExpr{&sqlparser.AliasedExpr{
		Expr: &sqlparser.FuncExpr{
			Name:  sqlparser.NewColIdent("count"),
			Exprs: []sqlparser.SelectExpr{&sqlparser.StarExpr{}},
		},
	}}
	if sel2.GroupBy == nil && sel2.Having == nil {
		// if there is no grouping, we can use the same query and
		// just replace the SELECT sub-clause to have a single count(*)
		sel2.SelectExprs = countStartExpr
	} else {
		// when there is grouping, we have to move the original query into a derived table.
		//                       select id, sum(12) from user group by id =>
		// select count(*) from (select id, sum(12) from user group by id) t
		sel3 := &sqlparser.Select{
			SelectExprs: countStartExpr,
			From: []sqlparser.TableExpr{
				&sqlparser.AliasedTableExpr{
					Expr: &sqlparser.DerivedTable{Select: sel2},
					As:   sqlparser.NewTableIdent("t"),
				},
			},
		}
		sel2 = sel3
	}

	cjt := newJointab(reservedVars2)
	countpb := newPrimitiveBuilder(vschema, cjt)
	err = countpb.processSelect(sel2, reservedVars2, outer, "")
	if err != nil {
		return nil, err
	}
	return &sqlCalcFoundRows{LimitQuery: frpb.plan, CountQuery: countpb.plan, ljt: ljt, cjt: cjt}, nil
}

func handleDualSelects(sel *sqlparser.Select, vschema ContextVSchema) (engine.Primitive, error) {
	if !isOnlyDual(sel) {
		return nil, nil
	}

	exprs := make([]evalengine.Expr, len(sel.SelectExprs))
	cols := make([]string, len(sel.SelectExprs))
	for i, e := range sel.SelectExprs {
		expr, ok := e.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, nil
		}
		var err error
		if sqlparser.IsLockingFunc(expr.Expr) {
			// if we are using any locking functions, we bail out here and send the whole query to a single destination
			return buildLockingPrimitive(sel, vschema)

		}
		exprs[i], err = sqlparser.Convert(expr.Expr)
		if err != nil {
			return nil, nil
		}
		cols[i] = expr.As.String()
		if cols[i] == "" {
			cols[i] = sqlparser.String(expr.Expr)
		}
	}
	return &engine.Projection{
		Exprs: exprs,
		Cols:  cols,
		Input: &engine.SingleRow{},
	}, nil
}

func buildLockingPrimitive(sel *sqlparser.Select, vschema ContextVSchema) (engine.Primitive, error) {
	ks, err := vschema.FirstSortedKeyspace()
	if err != nil {
		return nil, err
	}
	return &engine.Lock{
		Keyspace:          ks,
		TargetDestination: key.DestinationKeyspaceID{0},
		Query:             sqlparser.String(sel),
	}, nil
}

func isOnlyDual(sel *sqlparser.Select) bool {
	if sel.Where != nil || sel.GroupBy != nil || sel.Having != nil || sel.Limit != nil || sel.OrderBy != nil {
		// we can only deal with queries without any other subclauses - just SELECT and FROM, nothing else is allowed
		return false
	}

	if len(sel.From) > 1 {
		return false
	}
	table, ok := sel.From[0].(*sqlparser.AliasedTableExpr)
	if !ok {
		return false
	}
	tableName, ok := table.Expr.(sqlparser.TableName)

	return ok && tableName.Name.String() == "dual" && tableName.Qualifier.IsEmpty()
}

// pushFilter identifies the target route for the specified bool expr,
// pushes it down, and updates the route info if the new constraint improves
// the primitive. This function can push to a WHERE or HAVING clause.
func (pb *primitiveBuilder) pushFilter(in sqlparser.Expr, whereType string, reservedVars sqlparser.BindVars) error {
	filters := splitAndExpression(nil, in)
	reorderBySubquery(filters)
	for _, filter := range filters {
		pullouts, origin, expr, err := pb.findOrigin(filter, reservedVars)
		if err != nil {
			return err
		}
		rut, isRoute := origin.(*route)
		if isRoute && rut.eroute.Opcode == engine.SelectDBA {
			err := pb.findSysInfoRoutingPredicates(expr, rut)
			if err != nil {
				return err
			}
		}
		// The returned expression may be complex. Resplit before pushing.
		for _, subexpr := range splitAndExpression(nil, expr) {
			pb.plan, err = planFilter(pb, pb.plan, subexpr, whereType, origin)
			if err != nil {
				return err
			}
		}
		pb.addPullouts(pullouts)
	}
	return nil
}

// reorderBySubquery reorders the filters by pushing subqueries
// to the end. This allows the non-subquery filters to be
// pushed first because they can potentially improve the routing
// plan, which can later allow a filter containing a subquery
// to successfully merge with the corresponding route.
func reorderBySubquery(filters []sqlparser.Expr) {
	max := len(filters)
	for i := 0; i < max; i++ {
		if !hasSubquery(filters[i]) {
			continue
		}
		saved := filters[i]
		for j := i; j < len(filters)-1; j++ {
			filters[j] = filters[j+1]
		}
		filters[len(filters)-1] = saved
		max--
	}
}

// addPullouts adds the pullout subqueries to the primitiveBuilder.
func (pb *primitiveBuilder) addPullouts(pullouts []*pulloutSubquery) {
	for _, pullout := range pullouts {
		pullout.setUnderlying(pb.plan)
		pb.plan = pullout
		pb.plan.Reorder(0)
	}
}

// pushSelectExprs identifies the target route for the
// select expressions and pushes them down.
func (pb *primitiveBuilder) pushSelectExprs(sel *sqlparser.Select, reservedVars sqlparser.BindVars) error {
	resultColumns, err := pb.pushSelectRoutes(sel.SelectExprs, reservedVars)
	if err != nil {
		return err
	}
	pb.st.SetResultColumns(resultColumns)
	return pb.pushGroupBy(sel)
}

// pushSelectRoutes is a convenience function that pushes all the select
// expressions and returns the list of resultColumns generated for it.
func (pb *primitiveBuilder) pushSelectRoutes(selectExprs sqlparser.SelectExprs, reservedVars sqlparser.BindVars) ([]*resultColumn, error) {
	resultColumns := make([]*resultColumn, 0, len(selectExprs))
	for _, node := range selectExprs {
		switch node := node.(type) {
		case *sqlparser.AliasedExpr:
			pullouts, origin, expr, err := pb.findOrigin(node.Expr, reservedVars)
			if err != nil {
				return nil, err
			}
			node.Expr = expr
			newBuilder, rc, _, err := planProjection(pb, pb.plan, node, origin)
			if err != nil {
				return nil, err
			}
			pb.plan = newBuilder
			resultColumns = append(resultColumns, rc)
			pb.addPullouts(pullouts)
		case *sqlparser.StarExpr:
			var expanded bool
			var err error
			resultColumns, expanded, err = pb.expandStar(resultColumns, node)
			if err != nil {
				return nil, err
			}
			if expanded {
				continue
			}
			// We'll allow select * for simple routes.
			rb, ok := pb.plan.(*route)
			if !ok {
				return nil, errors.New("unsupported: '*' expression in cross-shard query")
			}
			// Validate keyspace reference if any.
			if !node.TableName.IsEmpty() {
				if _, err := pb.st.FindTable(node.TableName); err != nil {
					return nil, err
				}
			}
			resultColumns = append(resultColumns, rb.PushAnonymous(node))
		case *sqlparser.Nextval:
			rb, ok := pb.plan.(*route)
			if !ok {
				// This code is unreachable because the parser doesn't allow joins for next val statements.
				return nil, errors.New("unsupported: SELECT NEXT query in cross-shard query")
			}
			if rb.eroute.Opcode != engine.SelectNext {
				return nil, errors.New("NEXT used on a non-sequence table")
			}
			rb.eroute.Opcode = engine.SelectNext
			resultColumns = append(resultColumns, rb.PushAnonymous(node))
		default:
			return nil, fmt.Errorf("BUG: unexpected select expression type: %T", node)
		}
	}
	return resultColumns, nil
}

// expandStar expands a StarExpr and pushes the expanded
// expressions down if the tables have authoritative column lists.
// If not, it returns false.
// This function breaks the abstraction a bit: it directly sets the
// the Metadata for newly created expressions. In all other cases,
// the Metadata is set through a symtab Find.
func (pb *primitiveBuilder) expandStar(inrcs []*resultColumn, expr *sqlparser.StarExpr) (outrcs []*resultColumn, expanded bool, err error) {
	tables := pb.st.AllTables()
	if tables == nil {
		// no table metadata available.
		return inrcs, false, nil
	}
	if expr.TableName.IsEmpty() {
		for _, t := range tables {
			// All tables must have authoritative column lists.
			if !t.isAuthoritative {
				return inrcs, false, nil
			}
		}
		singleTable := false
		if len(tables) == 1 {
			singleTable = true
		}
		for _, t := range tables {
			for _, col := range t.columnNames {
				var expr *sqlparser.AliasedExpr
				if singleTable {
					// If there's only one table, we use unqualified column names.
					expr = &sqlparser.AliasedExpr{
						Expr: &sqlparser.ColName{
							Metadata: t.columns[col.Lowered()],
							Name:     col,
						},
					}
				} else {
					// If a and b have id as their column, then
					// select * from a join b should result in
					// select a.id as id, b.id as id from a join b.
					expr = &sqlparser.AliasedExpr{
						Expr: &sqlparser.ColName{
							Metadata:  t.columns[col.Lowered()],
							Name:      col,
							Qualifier: t.alias,
						},
						As: col,
					}
				}
				newBuilder, rc, _, err := planProjection(pb, pb.plan, expr, t.Origin())
				if err != nil {
					// Unreachable because PushSelect won't fail on ColName.
					return inrcs, false, err
				}
				pb.plan = newBuilder
				inrcs = append(inrcs, rc)
			}
		}
		return inrcs, true, nil
	}

	// Expression qualified with table name.
	t, err := pb.st.FindTable(expr.TableName)
	if err != nil {
		return inrcs, false, err
	}
	if !t.isAuthoritative {
		return inrcs, false, nil
	}
	for _, col := range t.columnNames {
		expr := &sqlparser.AliasedExpr{
			Expr: &sqlparser.ColName{
				Metadata:  t.columns[col.Lowered()],
				Name:      col,
				Qualifier: expr.TableName,
			},
		}
		newBuilder, rc, _, err := planProjection(pb, pb.plan, expr, t.Origin())
		if err != nil {
			// Unreachable because PushSelect won't fail on ColName.
			return inrcs, false, err
		}
		pb.plan = newBuilder
		inrcs = append(inrcs, rc)
	}
	return inrcs, true, nil
}
