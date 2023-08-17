/*
Copyright 2023 The Vitess Authors.

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
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// UncorrelatedSubQuery is a subquery that can be executed independently of the outer query,
// so we pull it out and execute the outer query first, and feed the result to the
// 'outer query through a bindvar
type UncorrelatedSubQuery struct {
	Original sqlparser.Expr
	Opcode   opcode.PulloutOpcode

	Subquery ops.Operator
	Outer    ops.Operator

	SubqueryResult string
	HasValues      string
}

func (s *UncorrelatedSubQuery) Inner() ops.Operator {
	return s.Subquery
}

func (s *UncorrelatedSubQuery) OriginalExpression() sqlparser.Expr {
	return s.Original
}

func (s *UncorrelatedSubQuery) OuterExpressionsNeeded() []*sqlparser.ColName {
	return nil
}

func (s *UncorrelatedSubQuery) SetOuter(op ops.Operator) {
	s.Outer = op
}

// Clone implements the Operator interface
func (s *UncorrelatedSubQuery) Clone(inputs []ops.Operator) ops.Operator {
	klone := *s
	klone.Subquery = inputs[0]
	if len(inputs) == 2 {
		klone.Outer = inputs[1]
	}
	return &klone
}

func (s *UncorrelatedSubQuery) GetOrdering() ([]ops.OrderBy, error) {
	return s.Outer.GetOrdering()
}

// Inputs implements the Operator interface
func (s *UncorrelatedSubQuery) Inputs() []ops.Operator {
	if s.Outer == nil {
		return []ops.Operator{s.Subquery}
	}
	return []ops.Operator{s.Subquery, s.Outer}
}

// SetInputs implements the Operator interface
func (s *UncorrelatedSubQuery) SetInputs(inputs []ops.Operator) {
	s.Subquery = inputs[0]
	if len(inputs) == 2 {
		s.Outer = inputs[1]
	}
}

func (s *UncorrelatedSubQuery) ShortDescription() string {
	return ""
}

func (s *UncorrelatedSubQuery) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	//TODO implement me
	panic("implement me")
}

func (s *UncorrelatedSubQuery) AddColumns(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy []bool, exprs []*sqlparser.AliasedExpr) ([]int, error) {
	//TODO implement me
	panic("implement me")
}

func (s *UncorrelatedSubQuery) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) (int, error) {
	if s.Outer == nil {
		return 0, vterrors.VT13001("rhs has not been set")
	}
	return s.Outer.FindCol(ctx, expr, underRoute)
}

func (s *UncorrelatedSubQuery) GetColumns(ctx *plancontext.PlanningContext) ([]*sqlparser.AliasedExpr, error) {
	//TODO implement me
	panic("implement me")
}

func (s *UncorrelatedSubQuery) GetSelectExprs(ctx *plancontext.PlanningContext) (sqlparser.SelectExprs, error) {
	//TODO implement me
	panic("implement me")
}
