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
	// SubQuery stores the information about subquery
	SubQuery struct {
		Outer ops.Operator
		Inner []*SubQueryInner

		noColumns
		noPredicates
	}

	// SubQueryInner stores the subquery information for a select statement
	SubQueryInner struct {
		// Inner is the Operator inside the parenthesis of the subquery.
		// i.e: select (select 1 union select 1), the Inner here would be
		// of type Concatenate since we have a Union.
		Inner ops.Operator

		// ExtractedSubquery contains all information we need about this subquery
		ExtractedSubquery *sqlparser.ExtractedSubquery

		noColumns
		noPredicates
	}
)

var _ ops.Operator = (*SubQuery)(nil)
var _ ops.Operator = (*SubQueryInner)(nil)

// Clone implements the Operator interface
func (s *SubQueryInner) Clone(inputs []ops.Operator) ops.Operator {
	return &SubQueryInner{
		Inner:             inputs[0],
		ExtractedSubquery: s.ExtractedSubquery,
	}
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

// Clone implements the Operator interface
func (s *SubQuery) Clone(inputs []ops.Operator) ops.Operator {
	result := &SubQuery{
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

func (s *SubQuery) GetOrdering() ([]ops.OrderBy, error) {
	return s.Outer.GetOrdering()
}

// Inputs implements the Operator interface
func (s *SubQuery) Inputs() []ops.Operator {
	operators := []ops.Operator{s.Outer}
	for _, inner := range s.Inner {
		operators = append(operators, inner)
	}
	return operators
}

// SetInputs implements the Operator interface
func (s *SubQuery) SetInputs(ops []ops.Operator) {
	s.Outer = ops[0]
}

func createSubqueryFromStatement(ctx *plancontext.PlanningContext, stmt sqlparser.Statement) (*SubQuery, error) {
	if len(ctx.SemTable.SubqueryMap[stmt]) == 0 {
		return nil, nil
	}
	subq := &SubQuery{}
	for _, sq := range ctx.SemTable.SubqueryMap[stmt] {
		opInner, err := createLogicalOperatorFromAST(ctx, sq.Subquery.Select)
		if err != nil {
			return nil, err
		}
		if horizon, ok := opInner.(*Horizon); ok {
			opInner = horizon.Source
		}

		subq.Inner = append(subq.Inner, &SubQueryInner{
			ExtractedSubquery: sq,
			Inner:             opInner,
		})
	}
	return subq, nil
}

func (s *SubQuery) Description() ops.OpDescription {
	return ops.OpDescription{
		OperatorType: "SubQuery",
	}
}

func (s *SubQueryInner) Description() ops.OpDescription {
	return ops.OpDescription{
		OperatorType: "SubQueryInner",
	}
}

func (s *SubQuery) ShortDescription() string {
	return ""
}

func (s *SubQueryInner) ShortDescription() string {
	return ""
}
