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
	"fmt"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/vt/key"

	"vitess.io/vitess/go/vt/vterrors"

	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

func buildSelectPlan(query string) stmtPlanner {
	return func(stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (*planResult, error) {
		sel := stmt.(*sqlparser.Select)
		if sel.With != nil {
			return nil, vterrors.VT12001("WITH expression in SELECT statement")
		}
		err := checkUnsupportedExpressions(sel)
		if err != nil {
			return nil, err
		}

		p, err := handleDualSelects(sel, vschema)
		if err != nil {
			return nil, err
		}
		if p != nil {
			return newPlanResult(p), nil
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

		if shouldRetryAfterPredicateRewriting(plan) {
			// by transforming the predicates to CNF, the planner will sometimes find better plans
			primitive := rewriteToCNFAndReplan(stmt, getPlan)
			if primitive != nil {
				return newPlanResult(primitive), nil
			}
		}
		primitive := plan.Primitive()
		if rb, ok := primitive.(*engine.Route); ok {
			// this is done because engine.Route doesn't handle the empty result well
			// if it doesn't find a shard to send the query to.
			// All other engine primitives can handle this, so we only need it when
			// Route is the last (and only) instruction before the user sees a result
			if isOnlyDual(sel) || (len(sel.GroupBy) == 0 && sel.SelectExprs.AllAggregation()) {
				rb.NoRoutesSpecialHandling = true
			}
		}

		return newPlanResult(primitive), nil
	}
}

// checkUnsupportedExpressions checks for unsupported expressions.
func checkUnsupportedExpressions(sel sqlparser.SQLNode) error {
	var unsupportedErr error
	sqlparser.Rewrite(sel, func(cursor *sqlparser.Cursor) bool {
		switch cursor.Node().(type) {
		case *sqlparser.AssignmentExpr:
			unsupportedErr = vterrors.VT12001("Assignment expression")
			return false
		default:
			return true
		}
	}, nil)
	if unsupportedErr != nil {
		return unsupportedErr
	}
	return nil
}

func rewriteToCNFAndReplan(stmt sqlparser.Statement, getPlan func(sel *sqlparser.Select) (logicalPlan, error)) engine.Primitive {
	rewritten := sqlparser.RewritePredicate(stmt)
	sel2, isSelect := rewritten.(*sqlparser.Select)
	if isSelect {
		log.Infof("retrying plan after cnf: %s", sqlparser.String(sel2))
		plan2, err := getPlan(sel2)
		if err == nil && !shouldRetryAfterPredicateRewriting(plan2) {
			// we only use this new plan if it's better than the old one we got
			return plan2.Primitive()
		}
	}
	return nil
}

func shouldRetryAfterPredicateRewriting(plan logicalPlan) bool {
	// if we have a I_S query, but have not found table_schema or table_name, let's try CNF
	var opcode engine.Opcode
	var sysTableTableName map[string]evalengine.Expr
	var sysTableTableSchema []evalengine.Expr

	switch routePlan := plan.(type) {
	case *routeGen4:
		opcode = routePlan.eroute.Opcode
		sysTableTableName = routePlan.eroute.SysTableTableName
		sysTableTableSchema = routePlan.eroute.SysTableTableSchema
	case *route:
		opcode = routePlan.eroute.Opcode
		sysTableTableName = routePlan.eroute.SysTableTableName
		sysTableTableSchema = routePlan.eroute.SysTableTableSchema
	default:
		return false
	}

	return opcode == engine.DBA &&
		len(sysTableTableName) == 0 &&
		len(sysTableTableSchema) == 0
}

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
func (pb *primitiveBuilder) processSelect(sel *sqlparser.Select, reservedVars *sqlparser.ReservedVars, outer *symtab, query string) error {
	// Check and error if there is any locking function present in select expression.
	for _, expr := range sel.SelectExprs {
		if aExpr, ok := expr.(*sqlparser.AliasedExpr); ok && sqlparser.IsLockingFunc(aExpr.Expr) {
			return vterrors.VT12001(fmt.Sprintf("%v is allowed only with dual", sqlparser.String(aExpr)))
		}
	}
	if sel.SQLCalcFoundRows {
		if outer != nil || query == "" {
			return vterrors.VT03008("SQL_CALC_FOUND_ROWS")
		}
		sel.SQLCalcFoundRows = false
		if sel.Limit != nil {
			plan, _, err := buildSQLCalcFoundRowsPlan(query, sel, reservedVars, pb.vschema, planSelectV3)
			if err != nil {
				return err
			}
			pb.plan = plan
			return nil
		}
	}

	// Into is not supported in subquery.
	if sel.Into != nil && (outer != nil || query == "") {
		return vterrors.VT03008("INTO")
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
		directives := sel.Comments.Directives()
		rb.eroute.QueryTimeout = queryTimeout(directives)
		if rb.eroute.TargetDestination != nil {
			return vterrors.VT12001("SELECT with a target destination")
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
			err := copyCommentsAndLocks(node.Select, sel, node.eroute.Opcode)
			if err != nil {
				return false, nil, err
			}
			return true, node, nil
		case *routeGen4:
			err := copyCommentsAndLocks(node.Select, sel, node.eroute.Opcode)
			if err != nil {
				return false, nil, err
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

func copyCommentsAndLocks(statement sqlparser.SelectStatement, sel *sqlparser.Select, opcode engine.Opcode) error {
	query := sqlparser.GetFirstSelect(statement)
	query.Comments = sel.Comments
	query.Lock = sel.Lock
	if sel.Into != nil {
		if opcode != engine.Unsharded {
			return vterrors.VT12001("INTO on sharded keyspace")
		}
		query.Into = sel.Into
	}
	return nil
}

func buildSQLCalcFoundRowsPlan(
	originalQuery string,
	sel *sqlparser.Select,
	reservedVars *sqlparser.ReservedVars,
	vschema plancontext.VSchema,
	planSelect func(reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, sel *sqlparser.Select) (*jointab, logicalPlan, []string, error),
) (logicalPlan, []string, error) {
	ljt, limitPlan, _, err := planSelect(reservedVars, vschema, sel)
	if err != nil {
		return nil, nil, err
	}

	statement2, reserved2, err := sqlparser.Parse2(originalQuery)
	if err != nil {
		return nil, nil, err
	}
	sel2 := statement2.(*sqlparser.Select)

	sel2.SQLCalcFoundRows = false
	sel2.OrderBy = nil
	sel2.Limit = nil

	countStartExpr := []sqlparser.SelectExpr{&sqlparser.AliasedExpr{
		Expr: &sqlparser.CountStar{},
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
					As:   sqlparser.NewIdentifierCS("t"),
				},
			},
		}
		sel2 = sel3
	}

	reservedVars2 := sqlparser.NewReservedVars("vtg", reserved2)

	cjt, countPlan, tablesUsed, err := planSelect(reservedVars2, vschema, sel2)
	if err != nil {
		return nil, nil, err
	}
	return &sqlCalcFoundRows{LimitQuery: limitPlan, CountQuery: countPlan, ljt: ljt, cjt: cjt}, tablesUsed, nil
}

func planSelectV3(reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, sel *sqlparser.Select) (*jointab, logicalPlan, []string, error) {
	ljt := newJointab(reservedVars)
	frpb := newPrimitiveBuilder(vschema, ljt)
	err := frpb.processSelect(sel, reservedVars, nil, "")
	return ljt, frpb.plan, nil, err
}

func handleDualSelects(sel *sqlparser.Select, vschema plancontext.VSchema) (engine.Primitive, error) {
	if !isOnlyDual(sel) {
		return nil, nil
	}

	exprs := make([]evalengine.Expr, len(sel.SelectExprs))
	cols := make([]string, len(sel.SelectExprs))
	var lockFunctions []*engine.LockFunc
	for i, e := range sel.SelectExprs {
		expr, ok := e.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, nil
		}
		var err error
		lFunc, isLFunc := expr.Expr.(*sqlparser.LockingFunc)
		if isLFunc {
			elem := &engine.LockFunc{Typ: expr.Expr.(*sqlparser.LockingFunc)}
			if lFunc.Name != nil {
				n, err := evalengine.Translate(lFunc.Name, nil)
				if err != nil {
					return nil, err
				}
				elem.Name = n
			}
			lockFunctions = append(lockFunctions, elem)
			continue
		}
		if len(lockFunctions) > 0 {
			return nil, vterrors.VT12001(fmt.Sprintf("LOCK function and other expression: [%s] in same select query", sqlparser.String(expr)))
		}
		exprs[i], err = evalengine.Translate(expr.Expr, &evalengine.Config{Collation: vschema.ConnCollation()})
		if err != nil {
			return nil, nil
		}
		cols[i] = expr.As.String()
		if cols[i] == "" {
			cols[i] = sqlparser.String(expr.Expr)
		}
	}
	if len(lockFunctions) > 0 {
		return buildLockingPrimitive(sel, vschema, lockFunctions)
	}
	return &engine.Projection{
		Exprs: exprs,
		Cols:  cols,
		Input: &engine.SingleRow{},
	}, nil
}

func buildLockingPrimitive(sel *sqlparser.Select, vschema plancontext.VSchema, lockFunctions []*engine.LockFunc) (engine.Primitive, error) {
	ks, err := vschema.FirstSortedKeyspace()
	if err != nil {
		return nil, err
	}
	buf := sqlparser.NewTrackedBuffer(sqlparser.FormatImpossibleQuery).WriteNode(sel)
	return &engine.Lock{
		Keyspace:          ks,
		TargetDestination: key.DestinationKeyspaceID{0},
		FieldQuery:        buf.String(),
		LockFunctions:     lockFunctions,
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
func (pb *primitiveBuilder) pushFilter(in sqlparser.Expr, whereType string, reservedVars *sqlparser.ReservedVars) error {
	filters := sqlparser.SplitAndExpression(nil, in)
	reorderBySubquery(filters)
	for _, filter := range filters {
		pullouts, origin, expr, err := pb.findOrigin(filter, reservedVars)
		if err != nil {
			return err
		}
		rut, isRoute := origin.(*route)
		if isRoute && rut.eroute.Opcode == engine.DBA {
			err := pb.findSysInfoRoutingPredicates(expr, rut, reservedVars)
			if err != nil {
				return err
			}
		}
		// The returned expression may be complex. Resplit before pushing.
		for _, subexpr := range sqlparser.SplitAndExpression(nil, expr) {
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
func (pb *primitiveBuilder) pushSelectExprs(sel *sqlparser.Select, reservedVars *sqlparser.ReservedVars) error {
	resultColumns, err := pb.pushSelectRoutes(sel.SelectExprs, reservedVars)
	if err != nil {
		return err
	}
	pb.st.SetResultColumns(resultColumns)
	return pb.pushGroupBy(sel)
}

// pushSelectRoutes is a convenience function that pushes all the select
// expressions and returns the list of resultColumns generated for it.
func (pb *primitiveBuilder) pushSelectRoutes(selectExprs sqlparser.SelectExprs, reservedVars *sqlparser.ReservedVars) ([]*resultColumn, error) {
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
				return nil, vterrors.VT12001("'*' expression in cross-shard query")
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
				return nil, vterrors.VT12001("SELECT NEXT query in cross-shard query")
			}
			if rb.eroute.Opcode != engine.Next {
				return nil, vterrors.VT03018()
			}
			rb.eroute.Opcode = engine.Next
			resultColumns = append(resultColumns, rb.PushAnonymous(node))
		default:
			return nil, vterrors.VT13001(fmt.Sprintf("unexpected SELECT expression type: %T", node))
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
