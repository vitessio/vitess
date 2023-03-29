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
	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type SimpleProjection struct {
	Source     ops.Operator
	Columns    []int
	ASTColumns []*sqlparser.AliasedExpr
}

var _ ops.PhysicalOperator = (*SimpleProjection)(nil)

func newSimpleProjection(src ops.Operator) *SimpleProjection {
	return &SimpleProjection{
		Source: src,
	}
}

func (s *SimpleProjection) IPhysical() {}

func (s *SimpleProjection) Clone(inputs []ops.Operator) ops.Operator {
	return &SimpleProjection{
		Source:     inputs[0],
		Columns:    slices.Clone(s.Columns),
		ASTColumns: slices.Clone(s.ASTColumns),
	}
}

func (s *SimpleProjection) Inputs() []ops.Operator {
	return []ops.Operator{s.Source}
}

func (s *SimpleProjection) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	// TODO implement me
	panic("implement me")
}

func (s *SimpleProjection) AddColumn(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr, reuseCol bool) (ops.Operator, int, error) {
	// TODO implement me
	panic("implement me")
}
