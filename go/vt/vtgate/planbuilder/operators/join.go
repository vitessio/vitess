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

package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// Join represents a join. If we have a predicate, this is an inner join. If no predicate exists, it is a cross join
type Join struct {
	LHS, RHS  ops.Operator
	Predicate sqlparser.Expr
	LeftJoin  bool

	noColumns
}

var _ ops.Operator = (*Join)(nil)

// Clone implements the Operator interface
func (j *Join) Clone(inputs []ops.Operator) ops.Operator {
	clone := *j
	clone.LHS = inputs[0]
	clone.RHS = inputs[1]
	return &Join{
		LHS:       inputs[0],
		RHS:       inputs[1],
		Predicate: j.Predicate,
		LeftJoin:  j.LeftJoin,
	}
}

// Inputs implements the Operator interface
func (j *Join) Inputs() []ops.Operator {
	return []ops.Operator{j.LHS, j.RHS}
}

// SetInputs implements the Operator interface
func (j *Join) SetInputs(ops []ops.Operator) {
	j.LHS, j.RHS = ops[0], ops[1]
}

func (j *Join) Compact(ctx *plancontext.PlanningContext) (ops.Operator, rewrite.TreeIdentity, error) {
	if j.LeftJoin {
		// we can't merge outer joins into a single QG
		return j, rewrite.SameTree, nil
	}

	lqg, lok := j.LHS.(*QueryGraph)
	rqg, rok := j.RHS.(*QueryGraph)
	if !lok || !rok {
		return j, rewrite.SameTree, nil
	}

	newOp := &QueryGraph{
		Tables:     append(lqg.Tables, rqg.Tables...),
		innerJoins: append(lqg.innerJoins, rqg.innerJoins...),
		NoDeps:     ctx.SemTable.AndExpressions(lqg.NoDeps, rqg.NoDeps),
	}
	if j.Predicate != nil {
		err := newOp.collectPredicate(ctx, j.Predicate)
		if err != nil {
			return nil, rewrite.SameTree, err
		}
	}
	return newOp, rewrite.NewTree, nil
}

func createOuterJoin(tableExpr *sqlparser.JoinTableExpr, lhs, rhs ops.Operator) (ops.Operator, error) {
	if tableExpr.Join == sqlparser.RightJoinType {
		lhs, rhs = rhs, lhs
	}
	predicate := tableExpr.Condition.On
	sqlparser.RemoveKeyspaceFromColName(predicate)
	return &Join{LHS: lhs, RHS: rhs, LeftJoin: true, Predicate: predicate}, nil
}

func createJoin(ctx *plancontext.PlanningContext, LHS, RHS ops.Operator) ops.Operator {
	lqg, lok := LHS.(*QueryGraph)
	rqg, rok := RHS.(*QueryGraph)
	if lok && rok {
		op := &QueryGraph{
			Tables:     append(lqg.Tables, rqg.Tables...),
			innerJoins: append(lqg.innerJoins, rqg.innerJoins...),
			NoDeps:     ctx.SemTable.AndExpressions(lqg.NoDeps, rqg.NoDeps),
		}
		return op
	}
	return &Join{LHS: LHS, RHS: RHS}
}

func createInnerJoin(ctx *plancontext.PlanningContext, tableExpr *sqlparser.JoinTableExpr, lhs, rhs ops.Operator) (ops.Operator, error) {
	op := createJoin(ctx, lhs, rhs)
	pred := tableExpr.Condition.On
	if pred != nil {
		var err error
		sqlparser.RemoveKeyspaceFromColName(pred)
		op, err = op.AddPredicate(ctx, pred)
		if err != nil {
			return nil, err
		}
	}
	return op, nil
}

func (j *Join) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	return AddPredicate(j, ctx, expr, false, newFilter)
}

var _ JoinOp = (*Join)(nil)

func (j *Join) GetLHS() ops.Operator {
	return j.LHS
}

func (j *Join) GetRHS() ops.Operator {
	return j.RHS
}

func (j *Join) SetLHS(operator ops.Operator) {
	j.LHS = operator
}

func (j *Join) SetRHS(operator ops.Operator) {
	j.RHS = operator
}

func (j *Join) MakeInner() {
	j.LeftJoin = false
}

func (j *Join) IsInner() bool {
	return !j.LeftJoin
}

func (j *Join) AddJoinPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) error {
	j.Predicate = ctx.SemTable.AndExpressions(j.Predicate, expr)
	return nil
}
