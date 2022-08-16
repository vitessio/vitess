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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/physical"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

var _ selectPlanner = gen4Planner("apa", 0)

func gen4Planner(query string, plannerVersion querypb.ExecuteOptions_PlannerVersion) selectPlanner {
	return func(stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema) (engine.Primitive, error) {
		selStatement, ok := stmt.(sqlparser.SelectStatement)
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%T not yet supported", stmt)
		}
		switch node := selStatement.(type) {
		case *sqlparser.Select:
			if node.With != nil {
				return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: with expression in select statement")
			}
		case *sqlparser.Union:
			if node.With != nil {
				return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: with expression in union statement")
			}
		}

		sel, isSel := selStatement.(*sqlparser.Select)
		if isSel {
			// handle dual table for processing at vtgate.
			p, err := handleDualSelects(sel, vschema)
			if err != nil || p != nil {
				return p, err
			}

			if sel.SQLCalcFoundRows && sel.Limit != nil {
				return gen4planSQLCalcFoundRows(vschema, sel, query, reservedVars)
			}
			// if there was no limit, we can safely ignore the SQLCalcFoundRows directive
			sel.SQLCalcFoundRows = false
		}

		getPlan := func(selStatement sqlparser.SelectStatement) (logicalPlan, error) {
			return newBuildSelectPlan(selStatement, reservedVars, vschema, plannerVersion)
		}

		plan, err := getPlan(selStatement)
		if err != nil {
			return nil, err
		}

		if shouldRetryWithCNFRewriting(plan) {
			// by transforming the predicates to CNF, the planner will sometimes find better plans
			primitive := gen4CNFRewrite(stmt, getPlan)
			if primitive != nil {
				return primitive, nil
			}
		}

		primitive := plan.Primitive()
		if rb, ok := primitive.(*engine.Route); ok && isSel {
			// this is done because engine.Route doesn't handle the empty result well
			// if it doesn't find a shard to send the query to.
			// All other engine primitives can handle this, so we only need it when
			// Route is the last (and only) instruction before the user sees a result
			if isOnlyDual(sel) || (len(sel.GroupBy) == 0 && sel.SelectExprs.AllAggregation()) {
				rb.NoRoutesSpecialHandling = true
			}
		}

		return primitive, nil
	}
}

func gen4planSQLCalcFoundRows(vschema plancontext.VSchema, sel *sqlparser.Select, query string, reservedVars *sqlparser.ReservedVars) (engine.Primitive, error) {
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
	err = plan.WireupGen4(semTable)
	if err != nil {
		return nil, err
	}
	return plan.Primitive(), nil
}

func planSelectGen4(reservedVars *sqlparser.ReservedVars, vschema plancontext.VSchema, sel *sqlparser.Select) (*jointab, logicalPlan, error) {
	plan, err := newBuildSelectPlan(sel, reservedVars, vschema, 0)
	if err != nil {
		return nil, nil, err
	}
	return nil, plan, nil
}

func gen4CNFRewrite(stmt sqlparser.Statement, getPlan func(selStatement sqlparser.SelectStatement) (logicalPlan, error)) engine.Primitive {
	rewritten, isSel := sqlparser.RewriteToCNF(stmt).(sqlparser.SelectStatement)
	if !isSel {
		// Fail-safe code, should never happen
		return nil
	}
	plan2, err := getPlan(rewritten)
	if err == nil && !shouldRetryWithCNFRewriting(plan2) {
		// we only use this new plan if it's better than the old one we got
		return plan2.Primitive()
	}
	return nil
}

func newBuildSelectPlan(
	selStmt sqlparser.SelectStatement,
	reservedVars *sqlparser.ReservedVars,
	vschema plancontext.VSchema,
	version querypb.ExecuteOptions_PlannerVersion,
) (logicalPlan, error) {
	ksName := ""
	if ks, _ := vschema.DefaultKeyspace(); ks != nil {
		ksName = ks.Name
	}
	semTable, err := semantics.Analyze(selStmt, ksName, vschema)
	if err != nil {
		return nil, err
	}
	// record any warning as planner warning.
	vschema.PlannerWarning(semTable.Warning)

	if ks := semTable.SingleUnshardedKeyspace(); ks != nil {
		return unshardedShortcut(selStmt, ks, semTable)
	}

	err = queryRewrite(semTable, reservedVars, selStmt)
	if err != nil {
		return nil, err
	}

	ctx := plancontext.NewPlanningContext(reservedVars, semTable, vschema, version)
	logical, err := abstract.CreateOperatorFromAST(selStmt, semTable)
	if err != nil {
		return nil, err
	}
	err = logical.CheckValid()
	if err != nil {
		return nil, err
	}

	physOp, err := physical.CreatePhysicalOperator(ctx, logical)
	if err != nil {
		return nil, err
	}

	plan, err := transformToLogicalPlan(ctx, physOp)
	if err != nil {
		return nil, err
	}

	plan, err = planHorizon(ctx, plan, selStmt)
	if err != nil {
		return nil, err
	}

	sel, isSel := selStmt.(*sqlparser.Select)
	if isSel {
		if err := setMiscFunc(plan, sel); err != nil {
			return nil, err
		}
	}

	if err := plan.WireupGen4(semTable); err != nil {
		return nil, err
	}

	plan, err = pushCommentDirectivesOnPlan(plan, selStmt)
	if err != nil {
		return nil, err
	}

	return plan, nil
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

func planHorizon(ctx *plancontext.PlanningContext, plan logicalPlan, in sqlparser.SelectStatement) (logicalPlan, error) {
	switch node := in.(type) {
	case *sqlparser.Select:
		hp := horizonPlanning{
			sel: node,
		}

		replaceSubQuery(ctx, node)
		var err error
		plan, err = hp.planHorizon(ctx, plan)
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
	qp, err := abstract.CreateQPFromUnion(union, ctx.SemTable)
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

func pushCommentDirectivesOnPlan(plan logicalPlan, stmt sqlparser.SelectStatement) (logicalPlan, error) {
	directives := sqlparser.ExtractCommentDirectives(sqlparser.GetFirstSelect(stmt).Comments)
	scatterAsWarns := false
	if directives.IsSet(sqlparser.DirectiveScatterErrorsAsWarnings) {
		scatterAsWarns = true
	}
	queryTimeout := queryTimeout(directives)
	if scatterAsWarns || queryTimeout > 0 {
		_, _ = visit(plan, func(logicalPlan logicalPlan) (bool, logicalPlan, error) {
			switch plan := logicalPlan.(type) {
			case *routeGen4:
				plan.eroute.ScatterErrorsAsWarnings = scatterAsWarns
				plan.eroute.QueryTimeout = queryTimeout
			}
			return true, logicalPlan, nil
		})
	}

	return plan, nil
}
