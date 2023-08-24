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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	// SubQueryContainer stores the information about a query and it's subqueries.
	// The inner subqueries can be executed in any order, so we store them like this so we can see more opportunities
	// for merging
	SubQueryContainer struct {
		Outer ops.Operator
		Inner []SubQuery
	}

	SubQuery interface {
		ops.Operator

		Inner() ops.Operator

		OriginalExpression() sqlparser.Expr // tbl.id = (SELECT foo from user LIMIT 1)
		SetOriginal(sqlparser.Expr)
		OuterExpressionsNeeded() []*sqlparser.ColName
		SetOuter(operator ops.Operator)
		GetJoinPredicates() []sqlparser.Expr
		ReplaceJoinPredicates(predicates sqlparser.Exprs)
	}
)

var _ ops.Operator = (*SubQueryContainer)(nil)

// Clone implements the Operator interface
func (s *SubQueryContainer) Clone(inputs []ops.Operator) ops.Operator {
	result := &SubQueryContainer{
		Outer: inputs[0],
	}
	for idx := range s.Inner {
		inner, ok := inputs[idx+1].(SubQuery)
		if !ok {
			panic("got bad input")
		}
		result.Inner = append(result.Inner, inner)
	}
	return result
}

func (s *SubQueryContainer) GetOrdering() ([]ops.OrderBy, error) {
	return s.Outer.GetOrdering()
}

// Inputs implements the Operator interface
func (s *SubQueryContainer) Inputs() []ops.Operator {
	operators := []ops.Operator{s.Outer}
	for _, inner := range s.Inner {
		operators = append(operators, inner)
	}
	return operators
}

// SetInputs implements the Operator interface
func (s *SubQueryContainer) SetInputs(ops []ops.Operator) {
	s.Outer = ops[0]
}

func (s *SubQueryContainer) ShortDescription() string {
	return ""
}

func (sq *SubQueryContainer) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	newSrc, err := sq.Outer.AddPredicate(ctx, expr)
	sq.Outer = newSrc
	return sq, err
}

func (sq *SubQueryContainer) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, exprs *sqlparser.AliasedExpr) (int, error) {
	return sq.Outer.AddColumn(ctx, reuseExisting, addToGroupBy, exprs)
}

func (sq *SubQueryContainer) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) (int, error) {
	return sq.Outer.FindCol(ctx, expr, underRoute)
}

func (sq *SubQueryContainer) GetColumns(ctx *plancontext.PlanningContext) ([]*sqlparser.AliasedExpr, error) {
	return sq.Outer.GetColumns(ctx)
}

func (sq *SubQueryContainer) GetSelectExprs(ctx *plancontext.PlanningContext) (sqlparser.SelectExprs, error) {
	return sq.Outer.GetSelectExprs(ctx)
}
