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
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	Mirror struct {
		Operator Operator
		Targets  []MirrorTarget
	}

	MirrorTarget interface {
		Operator
		cloneAsMirrorTarget([]Operator) MirrorTarget
		addPredicateAsMirrorTarget(*plancontext.PlanningContext, sqlparser.Expr) MirrorTarget
	}

	PercentMirrorTarget struct {
		Percent  float32
		Operator Operator
	}
)

var (
	_ Operator     = (*Mirror)(nil)
	_ MirrorTarget = (*PercentMirrorTarget)(nil)
)

// Clone will return a copy of this operator, protected so changed to the original will not impact the clone
func (m *Mirror) Clone(inputs []Operator) Operator {
	if len(inputs) == 0 {
		panic("invalid number of inputs")
	}

	newOp := *m
	newOp.Operator = inputs[0]

	mirrorTargets := make([]MirrorTarget, len(inputs)-1)
	for i, target := range inputs[1:] {
		mirrorTarget, ok := target.(MirrorTarget)
		if !ok {
			panic("invalid input type")
		}
		mirrorTargets[i] = mirrorTarget
	}

	newOp.Targets = mirrorTargets

	return &newOp
}

// Inputs returns the inputs for this operator
func (m *Mirror) Inputs() []Operator {
	inputs := make([]Operator, 1+len(m.Targets))
	inputs[0] = m.Operator
	for i, target := range m.Targets {
		inputs[i+1] = target
	}
	return inputs
}

// SetInputs changes the inputs for this op
func (m *Mirror) SetInputs(inputs []Operator) {
	if len(inputs) == 0 {
		panic("too few inputs")
	}
	m.Operator = inputs[0]

	targetOps := inputs[1:]
	targets := make([]MirrorTarget, len(targetOps))
	for i := 0; i < len(targetOps); i++ {
		mirrorTarget, ok := targetOps[i].(MirrorTarget)
		if !ok {
			panic("invalid input type")
		}
		targets[i] = mirrorTarget
	}

	m.Targets = targets
}

// AddPredicate is used to push predicates. It pushed it as far down as is possible in the tree.
// If we encounter a join and the predicate depends on both sides of the join, the predicate will be split into two parts,
// where data is fetched from the LHS of the join to be used in the evaluation on the RHS
// TODO: we should remove this and replace it with rewriters
func (m *Mirror) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	m.Operator = m.Operator.AddPredicate(ctx, expr)
	for _, target := range m.Targets {
		target.AddPredicate(ctx, expr)
	}
	return m
}

func (m *Mirror) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, expr *sqlparser.AliasedExpr) int {
	r := m.Operator.AddColumn(ctx, reuseExisting, addToGroupBy, expr)
	for _, target := range m.Targets {
		target.AddColumn(ctx, reuseExisting, addToGroupBy, expr)
	}
	return r
}

func (m *Mirror) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	return m.Operator.FindCol(ctx, expr, underRoute)
}

func (m *Mirror) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return m.Operator.GetColumns(ctx)
}

func (m *Mirror) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
	return m.Operator.GetSelectExprs(ctx)
}

func (m *Mirror) ShortDescription() string {
	return ""
}

func (m *Mirror) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return m.Operator.GetOrdering(ctx)
}

// Clone will return a copy of this operator, protected so changed to the original will not impact the clone
func (p *PercentMirrorTarget) Clone(inputs []Operator) Operator {
	return p.cloneAsMirrorTarget(inputs)
}

func (p *PercentMirrorTarget) cloneAsMirrorTarget(inputs []Operator) MirrorTarget {
	if len(inputs) != 1 {
		panic("invalid number of inputs")
	}

	return &PercentMirrorTarget{
		Operator: inputs[0],
		Percent:  p.Percent,
	}
}

// Inputs returns the inputs for this operator
func (p *PercentMirrorTarget) Inputs() []Operator {
	return []Operator{p.Operator}
}

// SetInputs changes the inputs for this op
func (p *PercentMirrorTarget) SetInputs(inputs []Operator) {
	if len(inputs) != 1 {
		panic("too few or too many inputs")
	}
	p.Operator = inputs[0]
}

// AddPredicate is used to push predicates. It pushed it as far down as is possible in the tree.
// If we encounter a join and the predicate depends on both sides of the join, the predicate will be split into two parts,
// where data is fetched from the LHS of the join to be used in the evaluation on the RHS
// TODO: we should remove this and replace it with rewriters
func (p *PercentMirrorTarget) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) Operator {
	return p.addPredicateAsMirrorTarget(ctx, expr)
}

func (p *PercentMirrorTarget) addPredicateAsMirrorTarget(ctx *plancontext.PlanningContext, expr sqlparser.Expr) MirrorTarget {
	p.Operator = p.Operator.AddPredicate(ctx, expr)
	return p
}

func (p *PercentMirrorTarget) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, expr *sqlparser.AliasedExpr) int {
	return p.Operator.AddColumn(ctx, reuseExisting, addToGroupBy, expr)
}

func (p *PercentMirrorTarget) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) int {
	return p.Operator.FindCol(ctx, expr, underRoute)
}

func (p *PercentMirrorTarget) GetColumns(ctx *plancontext.PlanningContext) []*sqlparser.AliasedExpr {
	return p.Operator.GetColumns(ctx)
}

func (p *PercentMirrorTarget) GetSelectExprs(ctx *plancontext.PlanningContext) sqlparser.SelectExprs {
	return p.Operator.GetSelectExprs(ctx)
}

func (p *PercentMirrorTarget) ShortDescription() string {
	return fmt.Sprintf("Percent:%f", p.Percent)
}

func (p *PercentMirrorTarget) GetOrdering(ctx *plancontext.PlanningContext) []OrderBy {
	return p.Operator.GetOrdering(ctx)
}
