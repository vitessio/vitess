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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	// SubQuery stores the information about subquery
	SubQuery struct {
		Outer Operator
		Inner []*SubQueryInner

		noColumns
	}

	// SubQueryInner stores the subquery information for a select statement
	SubQueryInner struct {
		// Inner is the Operator inside the parenthesis of the subquery.
		// i.e: select (select 1 union select 1), the Inner here would be
		// of type Concatenate since we have a Union.
		Inner Operator

		// ExtractedSubquery contains all information we need about this subquery
		ExtractedSubquery *sqlparser.ExtractedSubquery

		noColumns
	}
)

var _ Operator = (*SubQuery)(nil)
var _ Operator = (*SubQueryInner)(nil)

// Clone implements the Operator interface
func (s *SubQueryInner) Clone(inputs []Operator) Operator {
	checkSize(inputs, 1)
	return &SubQueryInner{
		Inner:             inputs[0],
		ExtractedSubquery: s.ExtractedSubquery,
	}
}

// Inputs implements the Operator interface
func (s *SubQueryInner) Inputs() []Operator {
	return []Operator{s.Inner}
}

// Clone implements the Operator interface
func (s *SubQuery) Clone(inputs []Operator) Operator {
	checkSize(inputs, len(s.Inner)+1)
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

// Inputs implements the Operator interface
func (s *SubQuery) Inputs() []Operator {
	operators := []Operator{s.Outer}
	for _, inner := range s.Inner {
		operators = append(operators, inner)
	}
	return operators
}

func createSubqueryFromStatement(ctx *plancontext.PlanningContext, stmt sqlparser.Statement) (*SubQuery, error) {
	if len(ctx.SemTable.SubqueryMap[stmt]) == 0 {
		return nil, nil
	}
	subq := &SubQuery{}
	for _, sq := range ctx.SemTable.SubqueryMap[stmt] {
		opInner, err := CreateLogicalOperatorFromAST(ctx, sq.Subquery.Select)
		if err != nil {
			return nil, err
		}
		subq.Inner = append(subq.Inner, &SubQueryInner{
			ExtractedSubquery: sq,
			Inner:             opInner,
		})
	}
	return subq, nil
}

func (s *SubQuery) AddPredicate(*plancontext.PlanningContext, sqlparser.Expr) (Operator, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "we cannot push predicates into %T", s)
}

func (s *SubQueryInner) AddPredicate(*plancontext.PlanningContext, sqlparser.Expr) (Operator, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "we cannot push predicates into %T", s)
}
