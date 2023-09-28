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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	// SubQueryContainer stores the information about a query and it's subqueries.
	// The inner subqueries can be executed in any order, so we store them like this so we can see more opportunities
	// for merging
	SubQueryContainer struct {
		Outer ops.Operator
		Inner []*SubQuery
	}
)

var _ ops.Operator = (*SubQueryContainer)(nil)

// Clone implements the Operator interface
func (sqc *SubQueryContainer) Clone(inputs []ops.Operator) ops.Operator {
	result := &SubQueryContainer{
		Outer: inputs[0],
	}
	for idx := range sqc.Inner {
		inner, ok := inputs[idx+1].(*SubQuery)
		if !ok {
			panic("got bad input")
		}
		result.Inner = append(result.Inner, inner)
	}
	return result
}

func (sqc *SubQueryContainer) GetOrdering() ([]ops.OrderBy, error) {
	return sqc.Outer.GetOrdering()
}

// Inputs implements the Operator interface
func (sqc *SubQueryContainer) Inputs() []ops.Operator {
	operators := []ops.Operator{sqc.Outer}
	for _, inner := range sqc.Inner {
		operators = append(operators, inner)
	}
	return operators
}

// SetInputs implements the Operator interface
func (sqc *SubQueryContainer) SetInputs(ops []ops.Operator) {
	sqc.Outer = ops[0]
}

func (sqc *SubQueryContainer) ShortDescription() string {
	return ""
}

func (sqc *SubQueryContainer) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	newSrc, err := sqc.Outer.AddPredicate(ctx, expr)
	sqc.Outer = newSrc
	return sqc, err
}

func (sqc *SubQueryContainer) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, exprs *sqlparser.AliasedExpr) (int, error) {
	return sqc.Outer.AddColumn(ctx, reuseExisting, addToGroupBy, exprs)
}

func (sqc *SubQueryContainer) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) (int, error) {
	return sqc.Outer.FindCol(ctx, expr, underRoute)
}

func (sqc *SubQueryContainer) GetColumns(ctx *plancontext.PlanningContext) ([]*sqlparser.AliasedExpr, error) {
	return sqc.Outer.GetColumns(ctx)
}

func (sqc *SubQueryContainer) GetSelectExprs(ctx *plancontext.PlanningContext) (sqlparser.SelectExprs, error) {
	return sqc.Outer.GetSelectExprs(ctx)
}
