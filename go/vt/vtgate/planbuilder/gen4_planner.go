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
	"errors"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func gen4Planner(query string, plannerVersion querypb.ExecuteOptions_PlannerVersion) stmtPlanner {
	return func(stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (*planResult, error) {
		switch stmt := stmt.(type) {
		case sqlparser.SelectStatement:
			return gen4SelectStmtPlanner(query, plannerVersion, stmt, reservedVars, vschema)
		case *sqlparser.Update:
			return gen4UpdateStmtPlanner(plannerVersion, stmt, reservedVars, vschema)
		case *sqlparser.Delete:
			return gen4DeleteStmtPlanner(plannerVersion, stmt, reservedVars, vschema)
		default:
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%T not yet supported", stmt)
		}
	}
}

func gen4SelectStmtPlanner(
	query string,
	plannerVersion querypb.ExecuteOptions_PlannerVersion,
	stmt sqlparser.SelectStatement,
	reservedVars *sqlparser.ReservedVars,
	vschema plancontext.VSchema,
) (*planResult, error) {
	switch node := stmt.(type) {
	case *sqlparser.Select:
		if node.With != nil {
			return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: with expression in select statement")
		}
	case *sqlparser.Union:
		if node.With != nil {
			return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: with expression in union statement")
		}
	}

	sel, isSel := stmt.(*sqlparser.Select)
	if isSel {
		// handle dual table for processing at vtgate.
		p, err := handleDualSelects(sel, vschema)
		if err != nil {
			return nil, err
		}
		if p != nil {
			used := "dual"
			keyspace, ksErr := vschema.DefaultKeyspace()
			if ksErr == nil {
				// we are just getting the ks to log the correct table use.
				// no need to fail this if we can't find the default keyspace
				used = keyspace.Name + ".dual"
			}
			return newPlanResult(p, used), nil
		}

		if sel.SQLCalcFoundRows && sel.Limit != nil {
			return gen4planSQLCalcFoundRows(vschema, sel, query, reservedVars)
		}
		// if there was no limit, we can safely ignore the SQLCalcFoundRows directive
		sel.SQLCalcFoundRows = false
	}

	getPlan := func(selStatement sqlparser.SelectStatement) (logicalPlan, *semantics.SemTable, error) {
		return newBuildSelectPlan(selStatement, reservedVars, vschema, plannerVersion)
	}

	plan, st, err := getPlan(stmt)
	if err != nil {
		return nil, err
	}

	if shouldRetryAfterPredicateRewriting(plan) {
		// by transforming the predicates to CNF, the planner will sometimes find better plans
		primitive, st := gen4PredicateRewrite(stmt, getPlan)
		if primitive != nil {
			return newPlanResult(primitive, tablesFromSemantics(st)...), nil
		}
	}

	primitive := plan.Primitive()
	if !isSel {
		return newPlanResult(primitive, tablesFromSemantics(st)...), nil
	}

	// this is done because engine.Route doesn't handle the empty result well
	// if it doesn't find a shard to send the query to.
	// All other engine primitives can handle this, so we only need it when
	// Route is the last (and only) instruction before the user sees a result
	if isOnlyDual(sel) || (len(sel.GroupBy) == 0 && sel.SelectExprs.AllAggregation()) {
		switch prim := primitive.(type) {
		case *engine.Route:
			prim.NoRoutesSpecialHandling = true
		case *engine.VindexLookup:
			prim.SendTo.NoRoutesSpecialHandling = true
		}
	}
	return newPlanResult(primitive, tablesFromSemantics(st)...), nil
}

func gen4planSQLCalcFoundRows(vschema plancontext.VSchema, sel *sqlparser.Select, query string, reservedVars *sqlparser.ReservedVars) (*planResult, error) {
	ksName := ""
	if ks, _ := vschema.DefaultKeyspace(); ks != nil {
		ksName = ks.Name
	}
	semTable, err := semantics.Analyze(sel, ksName, vschema)
	if err != nil {
		return nil, err
	}
	// record any warning as planner warning.
	vschema.PlannerWarning(semTable.Warning)

	plan, err := buildSQLCalcFoundRowsPlan(query, sel, reservedVars, vschema, planSelectGen4)
	if err != nil {
		return nil, err
	}
	return newPlanResult(plan.Primitive(), tablesFromSemantics(semTable)...), nil
}

func planSelectGen4(reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, sel *sqlparser.Select) (*jointab, logicalPlan, error) {
	plan, _, err := newBuildSelectPlan(sel, reservedVars, vschema, 0)
	if err != nil {
		return nil, nil, err
	}
	return nil, plan, nil
}

func gen4PredicateRewrite(stmt sqlparser.Statement, getPlan func(selStatement sqlparser.SelectStatement) (logicalPlan, *semantics.SemTable, error)) (engine.Primitive, *semantics.SemTable) {
	rewritten, isSel := sqlparser.RewritePredicate(stmt).(sqlparser.SelectStatement)
	if !isSel {
		// Fail-safe code, should never happen
		return nil, nil
	}
	plan2, st, err := getPlan(rewritten)
	if err == nil && !shouldRetryAfterPredicateRewriting(plan2) {
		// we only use this new plan if it's better than the old one we got
		return plan2.Primitive(), st
	}
	return nil, nil
}

func newBuildSelectPlan(
	selStmt sqlparser.SelectStatement,
	reservedVars *sqlparser.ReservedVars,
	vschema plancontext.VSchema,
	version querypb.ExecuteOptions_PlannerVersion,
) (plan logicalPlan, semTable *semantics.SemTable, err error) {
	ksName := ""
	if ks, _ := vschema.DefaultKeyspace(); ks != nil {
		ksName = ks.Name
	}
	semTable, err = semantics.Analyze(selStmt, ksName, vschema)
	if err != nil {
		return nil, nil, err
	}
	// record any warning as planner warning.
	vschema.PlannerWarning(semTable.Warning)

	ctx := plancontext.NewPlanningContext(reservedVars, semTable, vschema, version)

	if ks, _ := semTable.SingleUnshardedKeyspace(); ks != nil {
		plan, err = unshardedShortcut(ctx, selStmt, ks)
		if err != nil {
			return nil, nil, err
		}
		plan, err = pushCommentDirectivesOnPlan(plan, selStmt)
		if err != nil {
			return nil, nil, err
		}
		return plan, semTable, err
	}

	// From this point on, we know it is not an unsharded query and return the NotUnshardedErr if there is any
	if semTable.NotUnshardedErr != nil {
		return nil, nil, semTable.NotUnshardedErr
	}

	err = queryRewrite(semTable, reservedVars, selStmt)
	if err != nil {
		return nil, nil, err
	}

	op, err := operators.PlanQuery(ctx, selStmt)
	if err != nil {
		return nil, nil, err
	}

	plan, err = transformToLogicalPlan(ctx, op, true)
	if err != nil {
		return nil, nil, err
	}

	plan = optimizePlan(plan)

	sel, isSel := selStmt.(*sqlparser.Select)
	if isSel {
		if err = setMiscFunc(plan, sel); err != nil {
			return nil, nil, err
		}
	}

	if err = plan.WireupGen4(ctx); err != nil {
		return nil, nil, err
	}

	plan, err = pushCommentDirectivesOnPlan(plan, selStmt)
	if err != nil {
		return nil, nil, err
	}

	return plan, semTable, nil
}

// optimizePlan removes unnecessary simpleProjections that have been created while planning
func optimizePlan(plan logicalPlan) logicalPlan {
	newPlan, _ := visit(plan, func(plan logicalPlan) (bool, logicalPlan, error) {
		this, ok := plan.(*simpleProjection)
		if !ok {
			return true, plan, nil
		}

		input, ok := this.input.(*simpleProjection)
		if !ok {
			return true, plan, nil
		}

		for i, col := range this.eSimpleProj.Cols {
			this.eSimpleProj.Cols[i] = input.eSimpleProj.Cols[col]
		}
		this.input = input.input
		return true, this, nil
	})
	return newPlan
}

func gen4UpdateStmtPlanner(
	version querypb.ExecuteOptions_PlannerVersion,
	updStmt *sqlparser.Update,
	reservedVars *sqlparser.ReservedVars,
	vschema plancontext.VSchema,
) (*planResult, error) {
	if updStmt.With != nil {
		return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: with expression in update statement")
	}

	ksName := ""
	if ks, _ := vschema.DefaultKeyspace(); ks != nil {
		ksName = ks.Name
	}
	semTable, err := semantics.Analyze(updStmt, ksName, vschema)
	if err != nil {
		return nil, err
	}
	// record any warning as planner warning.
	vschema.PlannerWarning(semTable.Warning)

	err = rewriteRoutedTables(updStmt, vschema)
	if err != nil {
		return nil, err
	}

	if ks, tables := semTable.SingleUnshardedKeyspace(); ks != nil {
		edml := engine.NewDML()
		edml.Keyspace = ks
		edml.Table = tables
		edml.Opcode = engine.Unsharded
		edml.Query = generateQuery(updStmt)
		upd := &engine.Update{DML: edml}
		return newPlanResult(upd, tablesFromSemantics(semTable)...), nil
	}

	if semTable.NotUnshardedErr != nil {
		return nil, semTable.NotUnshardedErr
	}

	err = queryRewrite(semTable, reservedVars, updStmt)
	if err != nil {
		return nil, err
	}

	ctx := plancontext.NewPlanningContext(reservedVars, semTable, vschema, version)

	op, err := operators.PlanQuery(ctx, updStmt)
	if err != nil {
		return nil, err
	}

	plan, err := transformToLogicalPlan(ctx, op, true)
	if err != nil {
		return nil, err
	}

	plan, err = pushCommentDirectivesOnPlan(plan, updStmt)
	if err != nil {
		return nil, err
	}

	setLockOnAllSelect(plan)

	if err := plan.WireupGen4(ctx); err != nil {
		return nil, err
	}

	return newPlanResult(plan.Primitive(), tablesFromSemantics(semTable)...), nil
}

func gen4DeleteStmtPlanner(
	version querypb.ExecuteOptions_PlannerVersion,
	deleteStmt *sqlparser.Delete,
	reservedVars *sqlparser.ReservedVars,
	vschema plancontext.VSchema,
) (*planResult, error) {
	if deleteStmt.With != nil {
		return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: with expression in delete statement")
	}

	var err error
	if len(deleteStmt.TableExprs) == 1 && len(deleteStmt.Targets) == 1 {
		deleteStmt, err = rewriteSingleTbl(deleteStmt)
		if err != nil {
			return nil, err
		}
	}

	ksName := ""
	if ks, _ := vschema.DefaultKeyspace(); ks != nil {
		ksName = ks.Name
	}
	semTable, err := semantics.Analyze(deleteStmt, ksName, vschema)
	if err != nil {
		return nil, err
	}

	// record any warning as planner warning.
	vschema.PlannerWarning(semTable.Warning)
	err = rewriteRoutedTables(deleteStmt, vschema)
	if err != nil {
		return nil, err
	}

	if ks, tables := semTable.SingleUnshardedKeyspace(); ks != nil {
		edml := engine.NewDML()
		edml.Keyspace = ks
		edml.Table = tables
		edml.Opcode = engine.Unsharded
		edml.Query = generateQuery(deleteStmt)
		del := &engine.Delete{DML: edml}
		return newPlanResult(del, tablesFromSemantics(semTable)...), nil
	}

	if err := checkIfDeleteSupported(deleteStmt, semTable); err != nil {
		return nil, err
	}

	err = queryRewrite(semTable, reservedVars, deleteStmt)
	if err != nil {
		return nil, err
	}

	ctx := plancontext.NewPlanningContext(reservedVars, semTable, vschema, version)
	op, err := operators.PlanQuery(ctx, deleteStmt)
	if err != nil {
		return nil, err
	}

	plan, err := transformToLogicalPlan(ctx, op, true)
	if err != nil {
		return nil, err
	}

	plan, err = pushCommentDirectivesOnPlan(plan, deleteStmt)
	if err != nil {
		return nil, err
	}

	setLockOnAllSelect(plan)

	if err := plan.WireupGen4(ctx); err != nil {
		return nil, err
	}

	return newPlanResult(plan.Primitive(), tablesFromSemantics(semTable)...), nil
}

func rewriteRoutedTables(stmt sqlparser.Statement, vschema plancontext.VSchema) (err error) {
	// Rewrite routed tables
	_ = sqlparser.Rewrite(stmt, func(cursor *sqlparser.Cursor) bool {
		aliasTbl, isAlias := cursor.Node().(*sqlparser.AliasedTableExpr)
		if !isAlias {
			return err == nil
		}
		tableName, ok := aliasTbl.Expr.(sqlparser.TableName)
		if !ok {
			return err == nil
		}
		var vschemaTable *vindexes.Table
		vschemaTable, _, _, _, _, err = vschema.FindTableOrVindex(tableName)
		if err != nil {
			return false
		}

		if vschemaTable.Name.String() != tableName.Name.String() {
			name := tableName.Name
			if aliasTbl.As.IsEmpty() {
				// if the user hasn't specified an alias, we'll insert one here so the old table name still works
				aliasTbl.As = sqlparser.NewIdentifierCS(name.String())
			}
			tableName.Name = sqlparser.NewIdentifierCS(vschemaTable.Name.String())
			aliasTbl.Expr = tableName
		}

		return err == nil
	}, nil)
	return
}

func setLockOnAllSelect(plan logicalPlan) {
	_, _ = visit(plan, func(plan logicalPlan) (bool, logicalPlan, error) {
		switch node := plan.(type) {
		case *routeGen4:
			node.Select.SetLock(sqlparser.ShareModeLock)
			return true, node, nil
		}
		return true, plan, nil
	})
}

func planLimit(limit *sqlparser.Limit, plan logicalPlan) (logicalPlan, error) {
	if limit == nil {
		return plan, nil
	}
	rb, ok := plan.(*routeGen4)
	if ok && rb.isSingleShard() {
		rb.SetLimit(limit)
		return plan, nil
	}

	lPlan, err := createLimit(plan, limit)
	if err != nil {
		return nil, err
	}

	// visit does not modify the plan.
	_, err = visit(lPlan, setUpperLimit)
	if err != nil {
		return nil, err
	}
	return lPlan, nil
}

func planHorizon(ctx *plancontext.PlanningContext, plan logicalPlan, in sqlparser.SelectStatement, truncateColumns bool) (logicalPlan, error) {
	switch node := in.(type) {
	case *sqlparser.Select:
		hp := horizonPlanning{
			sel: node,
		}

		replaceSubQuery(ctx, node)
		var err error
		plan, err = hp.planHorizon(ctx, plan, truncateColumns)
		if err != nil {
			return nil, err
		}
		plan, err = planLimit(node.Limit, plan)
		if err != nil {
			return nil, err
		}
	case *sqlparser.Union:
		var err error
		rb, isRoute := plan.(*routeGen4)
		if !isRoute && ctx.SemTable.NotSingleRouteErr != nil {
			return nil, ctx.SemTable.NotSingleRouteErr
		}
		if isRoute && rb.isSingleShard() {
			err = planSingleShardRoutePlan(node, rb)
		} else {
			plan, err = planOrderByOnUnion(ctx, plan, node)
		}
		if err != nil {
			return nil, err
		}

		plan, err = planLimit(node.Limit, plan)
		if err != nil {
			return nil, err
		}
	}
	return plan, nil

}

func planOrderByOnUnion(ctx *plancontext.PlanningContext, plan logicalPlan, union *sqlparser.Union) (logicalPlan, error) {
	qp, err := operators.CreateQPFromUnion(union)
	if err != nil {
		return nil, err
	}
	hp := horizonPlanning{
		qp: qp,
	}
	if len(qp.OrderExprs) > 0 {
		plan, err = hp.planOrderBy(ctx, qp.OrderExprs, plan)
		if err != nil {
			return nil, err
		}
	}
	return plan, nil
}

func pushCommentDirectivesOnPlan(plan logicalPlan, stmt sqlparser.Statement) (logicalPlan, error) {
	var directives *sqlparser.CommentDirectives
	cmt, ok := stmt.(sqlparser.Commented)
	if ok {
		directives = cmt.GetParsedComments().Directives()
		scatterAsWarns := directives.IsSet(sqlparser.DirectiveScatterErrorsAsWarnings)
		timeout := queryTimeout(directives)

		if scatterAsWarns || timeout > 0 {
			_, _ = visit(plan, func(logicalPlan logicalPlan) (bool, logicalPlan, error) {
				switch plan := logicalPlan.(type) {
				case *routeGen4:
					plan.eroute.ScatterErrorsAsWarnings = scatterAsWarns
					plan.eroute.QueryTimeout = timeout
				}
				return true, logicalPlan, nil
			})
		}
	}

	return plan, nil
}

// checkIfDeleteSupported checks if the delete query is supported or we must return an error.
func checkIfDeleteSupported(del *sqlparser.Delete, semTable *semantics.SemTable) error {
	if semTable.NotUnshardedErr != nil {
		return semTable.NotUnshardedErr
	}

	// Delete is only supported for a single TableExpr which is supposed to be an aliased expression
	multiShardErr := errors.New("unsupported: multi-shard or vindex write statement")
	if len(del.TableExprs) != 1 {
		return multiShardErr
	}
	aliasedTableExpr, isAliasedExpr := del.TableExprs[0].(*sqlparser.AliasedTableExpr)
	if !isAliasedExpr {
		return multiShardErr
	}

	if len(del.Targets) > 1 {
		return vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "multi-table delete statement in not supported in sharded database")
	}

	// Get the table information and the vindex table from it
	ti, err := semTable.TableInfoFor(semTable.TableSetFor(aliasedTableExpr))
	if err != nil {
		return err
	}
	isSharded := false
	vt := ti.GetVindexTable()
	if vt != nil && vt.Keyspace != nil {
		isSharded = vt.Keyspace.Sharded
	}

	err = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node.(type) {
		case *sqlparser.Subquery, *sqlparser.DerivedTable:
			// We have a subquery, so we must fail the planning.
			// If this subquery and the table expression were all belonging to the same unsharded keyspace,
			// we would have already created a plan for them before doing these checks.
			if isSharded {
				return false, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: subqueries in sharded DML")
			}
			return false, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: sharded subqueries in DML")
		}
		return true, nil
	}, del)
	if err != nil {
		return err
	}

	return nil
}
