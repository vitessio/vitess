/*
Copyright 2024 The Vitess Authors.

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
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	PercentBasedMirror struct {
		Percent  float32
		Operator Operator
		Target   Operator
	}
)

var _ Operator = (*PercentBasedMirror)(nil)

func NewPercentBasedMirror(percent float32, operator, target Operator) *PercentBasedMirror {
	return &PercentBasedMirror{
		percent,
		operator,
		target,
	}
}

// Clone will return a copy of this operator, protected so changed to the original will not impact the clone
func (m *PercentBasedMirror) Clone(inputs []Operator) Operator {
	cloneMirror := *m
	cloneMirror.SetInputs(inputs)
	return &cloneMirror
}

// Inputs returns the inputs for this operator
func (m *PercentBasedMirror) Inputs() []Operator {
	return []Operator{
		m.Operator,
		m.Target,
	}
}

// SetInputs changes the inputs for this op
func (m *PercentBasedMirror) SetInputs(inputs []Operator) {
	if len(inputs) < 2 {
		panic(vterrors.VT13001("unexpected number of inputs for PercentBasedMirror operator"))
	}
	m.Operator = inputs[0]
	m.Target = inputs[1]
}

// AddPredicate is used to push predicates. It pushed it as far down as is possible in the tree.
// If we encounter a join and the predicate depends on both sides of the join, the predicate will be split into two parts,
// where data is fetched from the LHS of the join to be used in the evaluation on the RHS
// TODO: we should remove this and replace it with rewriters
func (m *PercentBasedMirror) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	panic(vterrors.VT13001("not supported"))
}

func (m *PercentBasedMirror) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, expr *sqlparser.AliasedExpr) int {
	panic(vterrors.VT13001("not supported"))
}

func (m *PercentBasedMirror) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	return m.Operator.FindCol(ctx, expr, underRoute)
}

func (m *PercentBasedMirror) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return m.Operator.GetColumns(ctx)
}

func (m *PercentBasedMirror) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
	return m.Operator.GetSelectExprs(ctx)
}

func (m *PercentBasedMirror) ShortDescription() string {
	return fmt.Sprintf("PercentBasedMirror (%.02f%%)", m.Percent)
}

func (m *PercentBasedMirror) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return m.Operator.GetOrdering(ctx)
}

// AddWSColumn implements Operator.
func (m *PercentBasedMirror) AddWSColumn(ctx *plancontext.PlanningContext, offset int, underRoute bool) int {
	panic(vterrors.VT13001("not supported"))
}
