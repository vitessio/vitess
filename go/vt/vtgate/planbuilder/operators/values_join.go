/*
Copyright 2025 The Vitess Authors.

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

package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	ValuesJoin struct {
		binaryOperator

		bindVarName string

		noColumns
	}
)

var _ Operator = (*ValuesJoin)(nil)
var _ JoinOp = (*ValuesJoin)(nil)

func (vj *ValuesJoin) GetLHS() Operator {
	return vj.LHS
}

func (vj *ValuesJoin) GetRHS() Operator {
	return vj.RHS
}

func (vj *ValuesJoin) SetLHS(operator Operator) {
	vj.LHS = operator
}

func (vj *ValuesJoin) SetRHS(operator Operator) {
	vj.RHS = operator
}

func (vj *ValuesJoin) MakeInner() {
	// no-op for values-join
}

func (vj *ValuesJoin) IsInner() bool {
	return true
}

func (vj *ValuesJoin) AddJoinPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) {
	if expr == nil {
		return
	}
	lID := TableID(vj.LHS)
	lhsCols := breakValuesJoinExpressionInLHS(ctx, expr, lID)
	vj.RHS = vj.RHS.AddPredicate(ctx, expr)

	columns := ctx.ValuesJoinColumns[vj.bindVarName]

outer:
	for _, lhsCol := range lhsCols {
		for _, ci := range columns {
			if ci.Equal(lhsCol.Name) {
				// already there, no need to add it again
				continue outer
			}
		}
		columns = append(columns, lhsCol.Name)
	}

	ctx.ValuesJoinColumns[vj.bindVarName] = columns
}

func (vj *ValuesJoin) Clone(inputs []Operator) Operator {
	clone := *vj
	clone.LHS = inputs[0]
	clone.RHS = inputs[1]
	return &clone
}

func (vj *ValuesJoin) ShortDescription() string {
	return ""
}

func (vj *ValuesJoin) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return vj.RHS.GetOrdering(ctx)
}

func (vj *ValuesJoin) planOffsets(ctx *plancontext.PlanningContext) Operator {
	panic("implement me")
}
