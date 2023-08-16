/*
Copyright 2022 The Vitess Authors.

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
	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	// SemiJoin is a correlated subquery that is used for filtering rows from the outer query.
	// It is a join between the outer query and the subquery, where the subquery is the RHS.
	// We are only interested in the existence of rows in the RHS, so we only need to know if
	SemiJoin struct {
		LHS ops.Operator // outer
		RHS ops.Operator // inner

		// JoinCols are the columns from the LHS used for the join.
		// These are the same columns pushed on the LHS that are now used in the Vars field
		JoinVars map[string]*sqlparser.ColName

		// arguments that need to be copied from the outer to inner
		// this field is filled in at offset planning time
		JoinVarOffsets map[string]int

		// Original is the original expression, including comparison operator or EXISTS expression
		Original sqlparser.Expr

		// inside and outside are the columns from the LHS and RHS respectively that are used in the semi join
		// only if the expressions are pure/bare/simple ColName:s, otherwise they are not added to these lists
		// for the predicate: tbl.id IN (SELECT bar(foo) from user WHERE tbl.id = user.id)
		// for the predicate: EXISTS (select 1 from user where tbl.ud = bar(foo) AND tbl.id = user.id limit)
		// We would store `tbl.id` in JoinVars, but nothing on the inside, since the expression
		// `foo(tbl.id)` is not a bare column
		comparisonColumns [][2]*sqlparser.ColName

		_sq *sqlparser.Subquery // (SELECT foo from user LIMIT 1)

		// if we are unable to
		rhsPredicate sqlparser.Expr
	}

	// UncorrelatedSubQuery is a subquery that can be executed indendently of the outer query,
	// so we pull it out and execute before the outer query, and feed the result into a bindvar
	// that is fed to the outer query
	UncorrelatedSubQuery struct {
		Outer, Inner ops.Operator
		Extracted    *sqlparser.ExtractedSubquery

		noColumns
		noPredicates
	}
)

func (sj *SemiJoin) SetOuter(operator ops.Operator) {
	sj.LHS = operator
}

func (sj *SemiJoin) OuterExpressionsNeeded() []*sqlparser.ColName {
	return maps.Values(sj.JoinVars)
}

var _ SubQuery = (*SemiJoin)(nil)

func (sj *SemiJoin) Inner() ops.Operator {
	return sj.RHS
}

func (sj *SemiJoin) OriginalExpression() sqlparser.Expr {
	return sj.Original
}

func (sj *SemiJoin) sq() *sqlparser.Subquery {
	return sj._sq
}

// Clone implements the Operator interface
func (s *UncorrelatedSubQuery) Clone(inputs []ops.Operator) ops.Operator {
	result := &UncorrelatedSubQuery{
		Outer:     inputs[0],
		Inner:     inputs[1],
		Extracted: s.Extracted,
	}
	return result
}

func (s *UncorrelatedSubQuery) GetOrdering() ([]ops.OrderBy, error) {
	return s.Outer.GetOrdering()
}

// Inputs implements the Operator interface
func (s *UncorrelatedSubQuery) Inputs() []ops.Operator {
	return []ops.Operator{s.Outer, s.Inner}
}

// SetInputs implements the Operator interface
func (s *UncorrelatedSubQuery) SetInputs(ops []ops.Operator) {
	s.Outer, s.Inner = ops[0], ops[1]
}

func (s *UncorrelatedSubQuery) ShortDescription() string {
	return ""
}

// Clone implements the Operator interface
func (sj *SemiJoin) Clone(inputs []ops.Operator) ops.Operator {
	klone := *sj
	switch len(inputs) {
	case 1:
		klone.RHS = inputs[0]
	case 2:
		klone.LHS = inputs[0]
		klone.RHS = inputs[1]
	default:
		panic("wrong number of inputs")
	}
	klone.JoinVars = maps.Clone(sj.JoinVars)
	klone.JoinVarOffsets = maps.Clone(sj.JoinVarOffsets)
	return &klone
}

func (sj *SemiJoin) GetOrdering() ([]ops.OrderBy, error) {
	return nil, nil
}

// Inputs implements the Operator interface
func (sj *SemiJoin) Inputs() []ops.Operator {
	if sj.LHS == nil {
		return []ops.Operator{sj.RHS}
	}

	return []ops.Operator{sj.LHS, sj.RHS}
}

// SetInputs implements the Operator interface
func (sj *SemiJoin) SetInputs(inputs []ops.Operator) {
	switch len(inputs) {
	case 1:
		sj.RHS = inputs[0]
	case 2:
		sj.LHS = inputs[0]
		sj.RHS = inputs[1]
	default:
		panic("wrong number of inputs")
	}
}

func (sj *SemiJoin) ShortDescription() string {
	return ""
}

func (sj *SemiJoin) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	//TODO implement me
	panic("implement me")
}

func (sj *SemiJoin) AddColumns(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy []bool, exprs []*sqlparser.AliasedExpr) ([]int, error) {
	return sj.LHS.AddColumns(ctx, reuseExisting, addToGroupBy, exprs)
}

func (sj *SemiJoin) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) (int, error) {
	return sj.LHS.FindCol(ctx, expr, underRoute)
}

func (sj *SemiJoin) GetColumns(ctx *plancontext.PlanningContext) ([]*sqlparser.AliasedExpr, error) {
	//TODO implement me
	panic("implement me")
}

func (sj *SemiJoin) GetSelectExprs(ctx *plancontext.PlanningContext) (sqlparser.SelectExprs, error) {
	//TODO implement me
	panic("implement me")
}
