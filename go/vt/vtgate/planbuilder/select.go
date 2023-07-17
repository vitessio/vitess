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

	"vitess.io/vitess/go/vt/key"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

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

	plan, tablesUsed, err := buildSQLCalcFoundRowsPlan(query, sel, reservedVars, vschema)
	if err != nil {
		return nil, err
	}
	return newPlanResult(plan.Primitive(), tablesUsed...), nil
}

func buildSQLCalcFoundRowsPlan(
	originalQuery string,
	sel *sqlparser.Select,
	reservedVars *sqlparser.ReservedVars,
	vschema plancontext.VSchema,
) (logicalPlan, []string, error) {
	limitPlan, _, err := planSelectGen4(reservedVars, vschema, sel)
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

	countPlan, tablesUsed, err := planSelectGen4(reservedVars2, vschema, sel2)
	if err != nil {
		return nil, nil, err
	}
	return &sqlCalcFoundRows{LimitQuery: limitPlan, CountQuery: countPlan}, tablesUsed, nil
}

func planSelectGen4(reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, sel *sqlparser.Select) (logicalPlan, []string, error) {
	plan, _, tablesUsed, err := newBuildSelectPlan(sel, reservedVars, vschema, 0)
	if err != nil {
		return nil, nil, err
	}
	return plan, tablesUsed, nil
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

	if err = plan.Wireup(ctx); err != nil {
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

func planLimit(limit *sqlparser.Limit, plan logicalPlan) (logicalPlan, error) {
	if limit == nil {
		return plan, nil
	}
	rb, ok := plan.(*route)
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
		rb, isRoute := plan.(*route)
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

func shouldRetryAfterPredicateRewriting(plan logicalPlan) bool {
	// if we have a I_S query, but have not found table_schema or table_name, let's try CNF
	var opcode engine.Opcode
	var sysTableTableName map[string]evalengine.Expr
	var sysTableTableSchema []evalengine.Expr

	switch routePlan := plan.(type) {
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

func setMiscFunc(in logicalPlan, sel *sqlparser.Select) error {
	_, err := visit(in, func(plan logicalPlan) (bool, logicalPlan, error) {
		switch node := plan.(type) {
		case *route:
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
