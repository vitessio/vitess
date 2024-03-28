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
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// Join represents a join. If we have a predicate, this is an inner join. If no predicate exists, it is a cross join
type Join struct {
	LHS, RHS  Operator
	Predicate sqlparser.Expr
	// JoinType is permitted to store only 3 of the possible values
	// NormalJoinType, StraightJoinType and LeftJoinType.
	JoinType sqlparser.JoinType

	noColumns
}

var _ Operator = (*Join)(nil)

// Clone implements the Operator interface
func (j *Join) Clone(inputs []Operator) Operator {
	clone := *j
	clone.LHS = inputs[0]
	clone.RHS = inputs[1]
	return &Join{
		LHS:       inputs[0],
		RHS:       inputs[1],
		Predicate: j.Predicate,
		JoinType:  j.JoinType,
	}
}

func (j *Join) GetOrdering(*plancontext.PlanningContext) []OrderBy {
	return nil
}

// Inputs implements the Operator interface
func (j *Join) Inputs() []Operator {
	return []Operator{j.LHS, j.RHS}
}

// SetInputs implements the Operator interface
func (j *Join) SetInputs(ops []Operator) {
	j.LHS, j.RHS = ops[0], ops[1]
}

func (j *Join) Compact(ctx *plancontext.PlanningContext) (Operator, *ApplyResult) {
	if !j.JoinType.IsCommutative() {
		// if we can't move tables around, we can't merge these inputs
		return j, NoRewrite
	}

	lqg, lok := j.LHS.(*QueryGraph)
	rqg, rok := j.RHS.(*QueryGraph)
	if !lok || !rok {
		return j, NoRewrite
	}

	newOp := &QueryGraph{
		Tables:     append(lqg.Tables, rqg.Tables...),
		innerJoins: append(lqg.innerJoins, rqg.innerJoins...),
		NoDeps:     ctx.SemTable.AndExpressions(lqg.NoDeps, rqg.NoDeps),
	}
	if j.Predicate != nil {
		newOp.collectPredicate(ctx, j.Predicate)
	}
	return newOp, Rewrote("merge querygraphs into a single one")
}

func createStraightJoin(ctx *plancontext.PlanningContext, join *sqlparser.JoinTableExpr, lhs, rhs Operator) Operator {
	// for inner joins we can treat the predicates as filters on top of the join
	joinOp := &Join{LHS: lhs, RHS: rhs, JoinType: join.Join}

	return addJoinPredicates(ctx, join.Condition.On, joinOp)
}

func createLeftOuterJoin(ctx *plancontext.PlanningContext, join *sqlparser.JoinTableExpr, lhs, rhs Operator) Operator {
	// first we switch sides, so we always deal with left outer joins
	switch join.Join {
	case sqlparser.RightJoinType:
		lhs, rhs = rhs, lhs
		join.Join = sqlparser.LeftJoinType
	case sqlparser.NaturalRightJoinType:
		lhs, rhs = rhs, lhs
		join.Join = sqlparser.NaturalLeftJoinType
	}

	joinOp := &Join{LHS: lhs, RHS: rhs, JoinType: join.Join}

	// for outer joins we have to be careful with the predicates we use
	var op Operator
	subq, _ := getSubQuery(join.Condition.On)
	if subq != nil {
		panic(vterrors.VT12001("subquery in outer join predicate"))
	}
	predicate := join.Condition.On
	sqlparser.RemoveKeyspaceInCol(predicate)
	joinOp.Predicate = predicate
	op = joinOp

	return op
}

func createInnerJoin(ctx *plancontext.PlanningContext, tableExpr *sqlparser.JoinTableExpr, lhs, rhs Operator) Operator {
	op := createJoin(ctx, lhs, rhs)
	return addJoinPredicates(ctx, tableExpr.Condition.On, op)
}

func addJoinPredicates(
	ctx *plancontext.PlanningContext,
	joinPredicate sqlparser.Expr,
	op Operator,
) Operator {
	sqc := &SubQueryBuilder{}
	outerID := TableID(op)
	sqlparser.RemoveKeyspaceInCol(joinPredicate)
	exprs := sqlparser.SplitAndExpression(nil, joinPredicate)
	for _, pred := range exprs {
		subq := sqc.handleSubquery(ctx, pred, outerID)
		if subq != nil {
			continue
		}
		op = op.AddPredicate(ctx, pred)
	}
	return sqc.getRootOperator(op, nil)
}

func createJoin(ctx *plancontext.PlanningContext, LHS, RHS Operator) Operator {
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

func (j *Join) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	return AddPredicate(ctx, j, expr, false, newFilterSinglePredicate)
}

var _ JoinOp = (*Join)(nil)

func (j *Join) GetLHS() Operator {
	return j.LHS
}

func (j *Join) GetRHS() Operator {
	return j.RHS
}

func (j *Join) SetLHS(operator Operator) {
	j.LHS = operator
}

func (j *Join) SetRHS(operator Operator) {
	j.RHS = operator
}

func (j *Join) MakeInner() {
	if j.IsInner() {
		return
	}
	j.JoinType = sqlparser.NormalJoinType
}

func (j *Join) IsInner() bool {
	return j.JoinType.IsInner()
}

func (j *Join) AddJoinPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) {
	j.Predicate = ctx.SemTable.AndExpressions(j.Predicate, expr)
}

func (j *Join) ShortDescription() string {
	return sqlparser.String(j.Predicate)
}
