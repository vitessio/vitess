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
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	// SubQueryLogical stores the information about subquery
	SubQueryLogical struct {
		Outer ops.Operator
		Inner []*SubQueryInner
	}

	// SubQueryInner stores the subquery information for a select statement
	SubQueryInner struct {
		// Inner is the Operator inside the parenthesis of the subquery.
		// i.e: select (select 1 union select 1), the Inner here would be
		// of type Concatenate since we have a Union.
		Inner ops.Operator

		OpCode         opcode.PulloutOpcode
		comparisonType sqlparser.ComparisonExprOperator

		// The comments below are for the following query:
		// WHERE tbl.id = (SELECT foo from user LIMIT 1)
		Original    sqlparser.Expr      // tbl.id = (SELECT foo from user LIMIT 1)
		outside     sqlparser.Expr      // tbl.id
		inside      sqlparser.Expr      // user.foo
		alternative sqlparser.Expr      // tbl.id = :arg
		sq          *sqlparser.Subquery // (SELECT foo from user LIMIT 1)

		noColumns
		noPredicates
	}
)

var _ ops.Operator = (*SubQueryLogical)(nil)
var _ ops.Operator = (*SubQueryInner)(nil)

// Clone implements the Operator interface
func (s *SubQueryInner) Clone(inputs []ops.Operator) ops.Operator {
	klone := *s
	klone.Inner = inputs[0]
	return &klone
}

func (s *SubQueryInner) GetOrdering() ([]ops.OrderBy, error) {
	return s.Inner.GetOrdering()
}

// Inputs implements the Operator interface
func (s *SubQueryInner) Inputs() []ops.Operator {
	return []ops.Operator{s.Inner}
}

// SetInputs implements the Operator interface
func (s *SubQueryInner) SetInputs(ops []ops.Operator) {
	s.Inner = ops[0]
}

// ShortDescription implements the Operator interface
func (s *SubQueryInner) ShortDescription() string {
	return ""
}

// Clone implements the Operator interface
func (s *SubQueryLogical) Clone(inputs []ops.Operator) ops.Operator {
	result := &SubQueryLogical{
		Outer: inputs[0],
	}
	for idx := range s.Inner {
		inner, ok := inputs[idx+1].(*SubQueryInner)
		if !ok {
			panic("got bad input")
		}
		result.Inner = append(result.Inner, inner)
	}
	return result
}

func (s *SubQueryLogical) GetOrdering() ([]ops.OrderBy, error) {
	return s.Outer.GetOrdering()
}

// Inputs implements the Operator interface
func (s *SubQueryLogical) Inputs() []ops.Operator {
	operators := []ops.Operator{s.Outer}
	for _, inner := range s.Inner {
		operators = append(operators, inner)
	}
	return operators
}

// SetInputs implements the Operator interface
func (s *SubQueryLogical) SetInputs(ops []ops.Operator) {
	s.Outer = ops[0]
}

func (s *SubQueryLogical) ShortDescription() string {
	return ""
}

func (sq *SubQueryLogical) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	newSrc, err := sq.Outer.AddPredicate(ctx, expr)
	sq.Outer = newSrc
	return sq, err
}

func (sq *SubQueryLogical) AddColumns(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy []bool, exprs []*sqlparser.AliasedExpr) ([]int, error) {
	return sq.Outer.AddColumns(ctx, reuseExisting, addToGroupBy, exprs)
}

func (sq *SubQueryLogical) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) (int, error) {
	return sq.Outer.FindCol(ctx, expr, underRoute)
}

func (sq *SubQueryLogical) GetColumns(ctx *plancontext.PlanningContext) ([]*sqlparser.AliasedExpr, error) {
	return sq.Outer.GetColumns(ctx)
}

func (sq *SubQueryLogical) GetSelectExprs(ctx *plancontext.PlanningContext) (sqlparser.SelectExprs, error) {
	return sq.Outer.GetSelectExprs(ctx)
}
