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
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func planHorizon(sel *sqlparser.Select, plan logicalPlan, semTable *semantics.SemTable) (logicalPlan, error) {
	rb, ok := plan.(*route)
	if ok && rb.isSingleShard() {
		ast := rb.Select.(*sqlparser.Select)
		ast.Distinct = sel.Distinct
		ast.GroupBy = sel.GroupBy
		ast.OrderBy = sel.OrderBy
		ast.SelectExprs = sel.SelectExprs
		ast.Comments = sel.Comments
		return plan, nil
	}

	// TODO real horizon planning to be done
	if sel.GroupBy != nil {
		return nil, semantics.Gen4NotSupportedF("GROUP BY")
	}
	qp, err := createQPFromSelect(sel, semTable)
	if err != nil {
		return nil, err
	}
	for _, e := range qp.selectExprs {
		if _, err := pushProjection(e, plan, semTable); err != nil {
			return nil, err
		}
	}

	if len(qp.aggrExprs) > 0 {
		plan, err = planAggregations(qp, plan, semTable)
		if err != nil {
			return nil, err
		}
	} else if sel.Distinct {
		pushDistinct(plan)

		// the user asked for distinct results, and the QP is not inherently distinct
		isDistinct, err := qp.isDistinct(semTable)
		if err != nil {
			return nil, err
		}
		if !isDistinct {
			distinctPlan, err := planDistinct(plan)

			if err != nil {
				return nil, err
			}
			plan = distinctPlan
		}
	}

	if len(sel.OrderBy) > 0 {
		plan, err = planOrderBy(qp, plan, semTable)
		if err != nil {
			return nil, err
		}
	}

	return plan, nil
}

func pushProjection(expr *sqlparser.AliasedExpr, plan logicalPlan, semTable *semantics.SemTable) (int, error) {
	switch plan := plan.(type) {
	case *route:
		sel := plan.Select.(*sqlparser.Select)
		offset := len(sel.SelectExprs)
		sel.SelectExprs = append(sel.SelectExprs, expr)
		return offset, nil
	case *joinV4:
		lhsSolves := plan.Left.ContainsTables()
		rhsSolves := plan.Right.ContainsTables()
		deps := semTable.Dependencies(expr.Expr)
		switch {
		case deps.IsSolvedBy(lhsSolves):
			offset, err := pushProjection(expr, plan.Left, semTable)
			if err != nil {
				return 0, err
			}
			plan.Cols = append(plan.Cols, -(offset + 1))
		case deps.IsSolvedBy(rhsSolves):
			offset, err := pushProjection(expr, plan.Right, semTable)
			if err != nil {
				return 0, err
			}
			plan.Cols = append(plan.Cols, offset+1)
		default:
			return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "unknown dependencies for %s", sqlparser.String(expr))
		}
		return len(plan.Cols) - 1, nil
	default:
		return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] pushProjection: should not happen")
	}
}

func pushDistinct(plan logicalPlan) {
	switch plan := plan.(type) {
	case *route:
		sel := plan.Select.(*sqlparser.Select)
		sel.Distinct = true
	case *joinV4:
		pushDistinct(plan.Left)
		pushDistinct(plan.Right)
	default:
		panic(vterrors.Errorf(vtrpcpb.Code_INTERNAL, "[BUG] pushProjection: should not happen"))
	}
}
