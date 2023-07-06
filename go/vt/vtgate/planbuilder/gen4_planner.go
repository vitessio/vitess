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
	"fmt"

	querypb "vitess.io/vitess/go/vt/proto/query"
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
		case *sqlparser.Insert:
			return gen4InsertStmtPlanner(plannerVersion, stmt, reservedVars, vschema)
		default:
			return nil, vterrors.VT12001(fmt.Sprintf("%T", stmt))
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
			return nil, vterrors.VT12001("WITH expression in SELECT statement")
		}
	case *sqlparser.Union:
		if node.With != nil {
			return nil, vterrors.VT12001("WITH expression in UNION statement")
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

	getPlan := func(selStatement sqlparser.SelectStatement) (logicalPlan, *semantics.SemTable, []string, error) {
		return newBuildSelectPlan(selStatement, reservedVars, vschema, plannerVersion)
	}

	plan, _, tablesUsed, err := getPlan(stmt)
	if err != nil {
		return nil, err
	}

	if shouldRetryAfterPredicateRewriting(plan) {
		// by transforming the predicates to CNF, the planner will sometimes find better plans
		plan2, _, tablesUsed := gen4PredicateRewrite(stmt, getPlan)
		if plan2 != nil {
			return newPlanResult(plan2.Primitive(), tablesUsed...), nil
		}
	}

	primitive := plan.Primitive()
	if !isSel {
		return newPlanResult(primitive, tablesUsed...), nil
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
	return newPlanResult(primitive, tablesUsed...), nil
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

	plan, tablesUsed, err := buildSQLCalcFoundRowsPlan(query, sel, reservedVars, vschema, planSelectGen4)
	if err != nil {
		return nil, err
	}
	return newPlanResult(plan.Primitive(), tablesUsed...), nil
}

func planSelectGen4(reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, sel *sqlparser.Select) (*jointab, logicalPlan, []string, error) {
	plan, _, tablesUsed, err := newBuildSelectPlan(sel, reservedVars, vschema, 0)
	if err != nil {
		return nil, nil, nil, err
	}
	return nil, plan, tablesUsed, nil
}

func gen4PredicateRewrite(stmt sqlparser.Statement, getPlan func(selStatement sqlparser.SelectStatement) (logicalPlan, *semantics.SemTable, []string, error)) (logicalPlan, *semantics.SemTable, []string) {
	rewritten, isSel := sqlparser.RewritePredicate(stmt).(sqlparser.SelectStatement)
	if !isSel {
		// Fail-safe code, should never happen
		return nil, nil, nil
	}
	plan2, st, op, err := getPlan(rewritten)
	if err == nil && !shouldRetryAfterPredicateRewriting(plan2) {
		// we only use this new plan if it's better than the old one we got
		return plan2, st, op
	}
	return nil, nil, nil
}

func newBuildSelectPlan(
	selStmt sqlparser.SelectStatement,
	reservedVars *sqlparser.ReservedVars,
	vschema plancontext.VSchema,
	version querypb.ExecuteOptions_PlannerVersion,
) (plan logicalPlan, semTable *semantics.SemTable, tablesUsed []string, err error) {
	ksName := ""
	if ks, _ := vschema.DefaultKeyspace(); ks != nil {
		ksName = ks.Name
	}
	semTable, err = semantics.Analyze(selStmt, ksName, vschema)
	if err != nil {
		return nil, nil, nil, err
	}
	// record any warning as planner warning.
	vschema.PlannerWarning(semTable.Warning)

	ctx := plancontext.NewPlanningContext(reservedVars, semTable, vschema, version)

	if ks, _ := semTable.SingleUnshardedKeyspace(); ks != nil {
		plan, tablesUsed, err = selectUnshardedShortcut(ctx, selStmt, ks)
		if err != nil {
			return nil, nil, nil, err
		}
		plan = pushCommentDirectivesOnPlan(plan, selStmt)
		return plan, semTable, tablesUsed, err
	}

	// From this point on, we know it is not an unsharded query and return the NotUnshardedErr if there is any
	if semTable.NotUnshardedErr != nil {
		return nil, nil, nil, semTable.NotUnshardedErr
	}

	err = queryRewrite(semTable, reservedVars, selStmt)
	if err != nil {
		return nil, nil, nil, err
	}

	op, err := operators.PlanQuery(ctx, selStmt)
	if err != nil {
		return nil, nil, nil, err
	}

	plan, err = transformToLogicalPlan(ctx, op, true)
	if err != nil {
		return nil, nil, nil, err
	}

	optimizePlan(plan)

	sel, isSel := selStmt.(*sqlparser.Select)
	if isSel {
		if err = setMiscFunc(plan, sel); err != nil {
			return nil, nil, nil, err
		}
	}

	if err = plan.WireupGen4(ctx); err != nil {
		return nil, nil, nil, err
	}

	plan = pushCommentDirectivesOnPlan(plan, selStmt)

	return plan, semTable, operators.TablesUsed(op), nil
}

// optimizePlan removes unnecessary simpleProjections that have been created while planning
func optimizePlan(plan logicalPlan) {
	for _, lp := range plan.Inputs() {
		optimizePlan(lp)
	}

	this, ok := plan.(*simpleProjection)
	if !ok {
		return
	}

	input, ok := this.input.(*simpleProjection)
	if !ok {
		return
	}

	for i, col := range this.eSimpleProj.Cols {
		this.eSimpleProj.Cols[i] = input.eSimpleProj.Cols[col]
	}
	this.input = input.input
}

func gen4UpdateStmtPlanner(
	version querypb.ExecuteOptions_PlannerVersion,
	updStmt *sqlparser.Update,
	reservedVars *sqlparser.ReservedVars,
	vschema plancontext.VSchema,
) (*planResult, error) {
	if updStmt.With != nil {
		return nil, vterrors.VT12001("WITH expression in UPDATE statement")
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
		plan := updateUnshardedShortcut(updStmt, ks, tables)
		plan = pushCommentDirectivesOnPlan(plan, updStmt)
		return newPlanResult(plan.Primitive(), operators.QualifiedTables(ks, tables)...), nil
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

	plan = pushCommentDirectivesOnPlan(plan, updStmt)

	setLockOnAllSelect(plan)

	if err := plan.WireupGen4(ctx); err != nil {
		return nil, err
	}

	return newPlanResult(plan.Primitive(), operators.TablesUsed(op)...), nil
}

func updateUnshardedShortcut(stmt *sqlparser.Update, ks *vindexes.Keyspace, tables []*vindexes.Table) logicalPlan {
	edml := engine.NewDML()
	edml.Keyspace = ks
	edml.Table = tables
	edml.Opcode = engine.Unsharded
	edml.Query = generateQuery(stmt)
	return &primitiveWrapper{prim: &engine.Update{DML: edml}}
}

func gen4DeleteStmtPlanner(
	version querypb.ExecuteOptions_PlannerVersion,
	deleteStmt *sqlparser.Delete,
	reservedVars *sqlparser.ReservedVars,
	vschema plancontext.VSchema,
) (*planResult, error) {
	if deleteStmt.With != nil {
		return nil, vterrors.VT12001("WITH expression in DELETE statement")
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
		plan := deleteUnshardedShortcut(deleteStmt, ks, tables)
		plan = pushCommentDirectivesOnPlan(plan, deleteStmt)
		return newPlanResult(plan.Primitive(), operators.QualifiedTables(ks, tables)...), nil
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

	plan = pushCommentDirectivesOnPlan(plan, deleteStmt)

	setLockOnAllSelect(plan)

	if err := plan.WireupGen4(ctx); err != nil {
		return nil, err
	}

	return newPlanResult(plan.Primitive(), operators.TablesUsed(op)...), nil
}

func deleteUnshardedShortcut(stmt *sqlparser.Delete, ks *vindexes.Keyspace, tables []*vindexes.Table) logicalPlan {
	edml := engine.NewDML()
	edml.Keyspace = ks
	edml.Table = tables
	edml.Opcode = engine.Unsharded
	edml.Query = generateQuery(stmt)
	return &primitiveWrapper{prim: &engine.Delete{DML: edml}}
}

func gen4InsertStmtPlanner(version querypb.ExecuteOptions_PlannerVersion, insStmt *sqlparser.Insert, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (*planResult, error) {
	ksName := ""
	if ks, _ := vschema.DefaultKeyspace(); ks != nil {
		ksName = ks.Name
	}
	semTable, err := semantics.Analyze(insStmt, ksName, vschema)
	if err != nil {
		return nil, err
	}
	// record any warning as planner warning.
	vschema.PlannerWarning(semTable.Warning)

	err = rewriteRoutedTables(insStmt, vschema)
	if err != nil {
		return nil, err
	}
	// remove any alias added from routing table.
	// insert query does not support table alias.
	insStmt.Table.As = sqlparser.NewIdentifierCS("")

	// Check single unsharded. Even if the table is for single unsharded but sequence table is used.
	// We cannot shortcut here as sequence column needs additional planning.
	ks, tables := semTable.SingleUnshardedKeyspace()
	if ks != nil && tables[0].AutoIncrement == nil {
		plan := insertUnshardedShortcut(insStmt, ks, tables)
		plan = pushCommentDirectivesOnPlan(plan, insStmt)
		return newPlanResult(plan.Primitive(), operators.QualifiedTables(ks, tables)...), nil
	}

	tblInfo, err := semTable.TableInfoFor(semTable.TableSetFor(insStmt.Table))
	if err != nil {
		return nil, err
	}
	if tblInfo.GetVindexTable().Keyspace.Sharded && semTable.NotUnshardedErr != nil {
		return nil, semTable.NotUnshardedErr
	}

	err = queryRewrite(semTable, reservedVars, insStmt)
	if err != nil {
		return nil, err
	}

	ctx := plancontext.NewPlanningContext(reservedVars, semTable, vschema, version)

	op, err := operators.PlanQuery(ctx, insStmt)
	if err != nil {
		return nil, err
	}

	plan, err := transformToLogicalPlan(ctx, op, true)
	if err != nil {
		return nil, err
	}

	plan = pushCommentDirectivesOnPlan(plan, insStmt)

	setLockOnAllSelect(plan)

	if err := plan.WireupGen4(ctx); err != nil {
		return nil, err
	}

	return newPlanResult(plan.Primitive(), operators.TablesUsed(op)...), nil
}

func insertUnshardedShortcut(stmt *sqlparser.Insert, ks *vindexes.Keyspace, tables []*vindexes.Table) logicalPlan {
	eIns := &engine.Insert{}
	eIns.Keyspace = ks
	eIns.Table = tables[0]
	eIns.Opcode = engine.InsertUnsharded
	eIns.Query = generateQuery(stmt)
	return &insert{eInsert: eIns}
}

func rewriteRoutedTables(stmt sqlparser.Statement, vschema plancontext.VSchema) error {
	// Rewrite routed tables
	return sqlparser.Walk(func(node sqlparser.SQLNode) (bool, error) {
		aliasTbl, isAlias := node.(*sqlparser.AliasedTableExpr)
		if !isAlias {
			return true, nil
		}
		tableName, ok := aliasTbl.Expr.(sqlparser.TableName)
		if !ok {
			return true, nil
		}
		vschemaTable, vindexTbl, _, _, _, err := vschema.FindTableOrVindex(tableName)
		if err != nil {
			return false, err
		}
		if vindexTbl != nil {
			// vindex cannot be present in a dml statement.
			return false, vterrors.VT09014()
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

		return true, nil
	}, stmt)
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
			err = planSingleRoutePlan(node, rb)
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
	qp, err := operators.CreateQPFromSelectStatement(ctx, union)
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

func pushCommentDirectivesOnPlan(plan logicalPlan, stmt sqlparser.Statement) logicalPlan {
	var directives *sqlparser.CommentDirectives
	cmt, ok := stmt.(sqlparser.Commented)
	if ok {
		directives = cmt.GetParsedComments().Directives()
		scatterAsWarns := directives.IsSet(sqlparser.DirectiveScatterErrorsAsWarnings)
		timeout := queryTimeout(directives)
		multiShardAutoCommit := directives.IsSet(sqlparser.DirectiveMultiShardAutocommit)

		if scatterAsWarns || timeout > 0 || multiShardAutoCommit {
			_, _ = visit(plan, func(logicalPlan logicalPlan) (bool, logicalPlan, error) {
				switch plan := logicalPlan.(type) {
				case *routeGen4:
					plan.eroute.ScatterErrorsAsWarnings = scatterAsWarns
					plan.eroute.QueryTimeout = timeout
				case *primitiveWrapper:
					setDirective(plan.prim, multiShardAutoCommit, timeout)
				case *insert:
					setDirective(plan.eInsert, multiShardAutoCommit, timeout)
				}
				return true, logicalPlan, nil
			})
		}
	}

	return plan
}

func setDirective(prim engine.Primitive, msac bool, timeout int) {
	switch edml := prim.(type) {
	case *engine.Insert:
		edml.MultiShardAutocommit = msac
		edml.QueryTimeout = timeout
	case *engine.Update:
		edml.MultiShardAutocommit = msac
		edml.QueryTimeout = timeout
	case *engine.Delete:
		edml.MultiShardAutocommit = msac
		edml.QueryTimeout = timeout
	}
}

// checkIfDeleteSupported checks if the delete query is supported or we must return an error.
func checkIfDeleteSupported(del *sqlparser.Delete, semTable *semantics.SemTable) error {
	if semTable.NotUnshardedErr != nil {
		return semTable.NotUnshardedErr
	}

	// Delete is only supported for a single TableExpr which is supposed to be an aliased expression
	multiShardErr := vterrors.VT12001("multi-shard or vindex write statement")
	if len(del.TableExprs) != 1 {
		return multiShardErr
	}
	_, isAliasedExpr := del.TableExprs[0].(*sqlparser.AliasedTableExpr)
	if !isAliasedExpr {
		return multiShardErr
	}

	if len(del.Targets) > 1 {
		return vterrors.VT12001("multi-table DELETE statement in a sharded keyspace")
	}

	err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node.(type) {
		case *sqlparser.Subquery, *sqlparser.DerivedTable:
			// We have a subquery, so we must fail the planning.
			// If this subquery and the table expression were all belonging to the same unsharded keyspace,
			// we would have already created a plan for them before doing these checks.
			return false, vterrors.VT12001("subqueries in DML")
		}
		return true, nil
	}, del)
	if err != nil {
		return err
	}

	return nil
}
