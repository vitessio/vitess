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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

var _ selectPlanner = gen4Planner

func gen4Planner(query string) func(sqlparser.Statement, *sqlparser.ReservedVars, ContextVSchema) (engine.Primitive, error) {
	return func(stmt sqlparser.Statement, reservedVars *sqlparser.ReservedVars, vschema ContextVSchema) (engine.Primitive, error) {
		selStatement, ok := stmt.(sqlparser.SelectStatement)
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "%T not yet supported", stmt)
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
			return newBuildSelectPlan(selStatement, reservedVars, vschema)
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
		return plan.Primitive(), nil
	}
}

func gen4planSQLCalcFoundRows(vschema ContextVSchema, sel *sqlparser.Select, query string, reservedVars *sqlparser.ReservedVars) (engine.Primitive, error) {
	ksName := ""
	if ks, _ := vschema.DefaultKeyspace(); ks != nil {
		ksName = ks.Name
	}
	semTable, err := semantics.Analyze(sel, ksName, vschema)
	if err != nil {
		return nil, err
	}
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

func planSelectGen4(reservedVars *sqlparser.ReservedVars, vschema ContextVSchema, sel *sqlparser.Select) (*jointab, logicalPlan, error) {
	plan, err := newBuildSelectPlan(sel, reservedVars, vschema)
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

func newBuildSelectPlan(selStmt sqlparser.SelectStatement, reservedVars *sqlparser.ReservedVars, vschema ContextVSchema) (logicalPlan, error) {
	ksName := ""
	if ks, _ := vschema.DefaultKeyspace(); ks != nil {
		ksName = ks.Name
	}
	semTable, err := semantics.Analyze(selStmt, ksName, vschema)
	if err != nil {
		return nil, err
	}

	err = queryRewrite(semTable, reservedVars, selStmt)
	if err != nil {
		return nil, err
	}

	ctx := newPlanningContext(reservedVars, semTable, vschema)
	opTree, err := abstract.CreateOperatorFromAST(selStmt, semTable)
	if err != nil {
		return nil, err
	}
	err = opTree.CheckValid()
	if err != nil {
		return nil, err
	}

	tree, err := optimizeQuery(ctx, opTree)
	if err != nil {
		return nil, err
	}

	plan, err := transformToLogicalPlan(ctx, tree)
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

	directives := sqlparser.ExtractCommentDirectives(sqlparser.GetFirstSelect(selStmt).Comments)
	if directives.IsSet(sqlparser.DirectiveScatterErrorsAsWarnings) {
		_, _ = visit(plan, func(logicalPlan logicalPlan) (bool, logicalPlan, error) {
			switch plan := logicalPlan.(type) {
			case *route:
				plan.eroute.ScatterErrorsAsWarnings = true
			}
			return true, logicalPlan, nil
		})
	}

	return plan, nil
}

func newPlanningContext(reservedVars *sqlparser.ReservedVars, semTable *semantics.SemTable, vschema ContextVSchema) *planningContext {
	ctx := &planningContext{
		reservedVars:          reservedVars,
		semTable:              semTable,
		vschema:               vschema,
		argToReplaceBySelect:  map[string]*sqlparser.Select{},
		exprToReplaceBySqExpr: map[sqlparser.Expr]sqlparser.Expr{},
	}
	return ctx
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

func planHorizon(ctx *planningContext, plan logicalPlan, in sqlparser.SelectStatement) (logicalPlan, error) {
	switch node := in.(type) {
	case *sqlparser.Select:
		hp := horizonPlanning{
			sel: node,
		}

		replaceSubQuery(ctx.exprToReplaceBySqExpr, node)
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
		rb, isRoute := plan.(*route)
		if !isRoute && ctx.semTable.ProjectionErr != nil {
			return nil, ctx.semTable.ProjectionErr
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

func planOrderByOnUnion(ctx *planningContext, plan logicalPlan, union *sqlparser.Union) (logicalPlan, error) {
	qp, err := abstract.CreateQPFromUnion(union, ctx.semTable)
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
