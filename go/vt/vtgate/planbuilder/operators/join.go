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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// Join represents a join. If we have a predicate, this is an inner join. If no predicate exists, it is a cross join
type Join struct {
	LHS, RHS  Operator
	Predicate sqlparser.Expr
	LeftJoin  bool

	noColumns
}

var _ Operator = (*Join)(nil)

// clone implements the Operator interface
func (j *Join) clone(inputs []Operator) Operator {
	checkSize(inputs, 2)
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

// inputs implements the Operator interface
func (j *Join) inputs() []Operator {
	return []Operator{j.LHS, j.RHS}
}

func createOuterJoin(tableExpr *sqlparser.JoinTableExpr, lhs, rhs Operator) (Operator, error) {
	if tableExpr.Join == sqlparser.RightJoinType {
		lhs, rhs = rhs, lhs
	}
	return &Join{LHS: lhs, RHS: rhs, LeftJoin: true, Predicate: sqlparser.RemoveKeyspaceFromColName(tableExpr.Condition.On)}, nil
}

func createJoin(LHS, RHS Operator) Operator {
	lqg, lok := LHS.(*QueryGraph)
	rqg, rok := RHS.(*QueryGraph)
	if lok && rok {
		op := &QueryGraph{
			Tables:     append(lqg.Tables, rqg.Tables...),
			innerJoins: append(lqg.innerJoins, rqg.innerJoins...),
			NoDeps:     sqlparser.AndExpressions(lqg.NoDeps, rqg.NoDeps),
		}
		return op
	}
	return &Join{LHS: LHS, RHS: RHS}
}

func createInnerJoin(ctx *plancontext.PlanningContext, tableExpr *sqlparser.JoinTableExpr, lhs, rhs Operator) (Operator, error) {
	op := createJoin(lhs, rhs)
	if tableExpr.Condition.On != nil {
		var err error
		predicate := sqlparser.RemoveKeyspaceFromColName(tableExpr.Condition.On)
		op, err = op.AddPredicate(ctx, predicate)
		if err != nil {
			return nil, err
		}
	}
	return op, nil
}

func (j *Join) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (Operator, error) {
	return addPredicate(j, ctx, expr, false)
}

var _ joinOperator = (*Join)(nil)

func (j *Join) tableID() semantics.TableSet {
	return TableID(j)
}

func (j *Join) getLHS() Operator {
	return j.LHS
}

func (j *Join) getRHS() Operator {
	return j.RHS
}

func (j *Join) setLHS(operator Operator) {
	j.LHS = operator
}

func (j *Join) setRHS(operator Operator) {
	j.RHS = operator
}

func (j *Join) makeInner() {
	j.LeftJoin = false
}

func (j *Join) isInner() bool {
	return !j.LeftJoin
}

func (j *Join) addJoinPredicate(_ *plancontext.PlanningContext, expr sqlparser.Expr) error {
	j.Predicate = sqlparser.AndExpressions(j.Predicate, expr)
	return nil
}
