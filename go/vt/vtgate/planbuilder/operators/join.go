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
	LeftJoin  bool

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
		LeftJoin:  j.LeftJoin,
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
	if j.LeftJoin {
		// we can't merge outer joins into a single QG
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

func createOuterJoin(tableExpr *sqlparser.JoinTableExpr, lhs, rhs Operator) Operator {
	if tableExpr.Join == sqlparser.RightJoinType {
		lhs, rhs = rhs, lhs
	}
	subq, _ := getSubQuery(tableExpr.Condition.On)
	if subq != nil {
		panic(vterrors.VT12001("subquery in outer join predicate"))
	}
	predicate := tableExpr.Condition.On
	sqlparser.RemoveKeyspace(predicate)
	return &Join{LHS: lhs, RHS: rhs, LeftJoin: true, Predicate: predicate}
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

func createInnerJoin(ctx *plancontext.PlanningContext, tableExpr *sqlparser.JoinTableExpr, lhs, rhs Operator) Operator {
	op := createJoin(ctx, lhs, rhs)
	sqc := &SubQueryBuilder{}
	outerID := TableID(op)
	joinPredicate := tableExpr.Condition.On
	sqlparser.RemoveKeyspace(joinPredicate)
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

func (j *Join) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	return AddPredicate(ctx, j, expr, false, newFilter)
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
	j.LeftJoin = false
}

func (j *Join) IsInner() bool {
	return !j.LeftJoin
}

func (j *Join) AddJoinPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) {
	j.Predicate = ctx.SemTable.AndExpressions(j.Predicate, expr)
}

func (j *Join) ShortDescription() string {
	return sqlparser.String(j.Predicate)
}
