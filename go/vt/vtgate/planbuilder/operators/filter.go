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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type Filter struct {
	Source     ops.Operator
	Predicates []sqlparser.Expr
}

var _ ops.PhysicalOperator = (*Filter)(nil)

func newFilter(op ops.Operator, expr sqlparser.Expr) ops.Operator {
	return &Filter{
		Source: op, Predicates: []sqlparser.Expr{expr},
	}
}

// IPhysical implements the PhysicalOperator interface
func (f *Filter) IPhysical() {}

// Clone implements the Operator interface
func (f *Filter) Clone(inputs []ops.Operator) ops.Operator {
	predicatesClone := make([]sqlparser.Expr, len(f.Predicates))
	copy(predicatesClone, f.Predicates)
	return &Filter{
		Source:     inputs[0],
		Predicates: predicatesClone,
	}
}

// Inputs implements the Operator interface
func (f *Filter) Inputs() []ops.Operator {
	return []ops.Operator{f.Source}
}

// UnsolvedPredicates implements the unresolved interface
func (f *Filter) UnsolvedPredicates(st *semantics.SemTable) []sqlparser.Expr {
	var result []sqlparser.Expr
	id := TableID(f)
	for _, p := range f.Predicates {
		deps := st.RecursiveDeps(p)
		if !deps.IsSolvedBy(id) {
			result = append(result, p)
		}
	}
	return result
}

func (f *Filter) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	newSrc, err := f.Source.AddPredicate(ctx, expr)
	if err != nil {
		return nil, err
	}
	f.Source = newSrc
	return f, nil
}

func (f *Filter) AddColumn(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (int, error) {
	return f.Source.AddColumn(ctx, expr)
}

func (f *Filter) Compact(*plancontext.PlanningContext) (ops.Operator, rewrite.TreeIdentity, error) {
	if len(f.Predicates) == 0 {
		return f.Source, rewrite.NewTree, nil
	}

	other, isFilter := f.Source.(*Filter)
	if !isFilter {
		return f, rewrite.SameTree, nil
	}
	f.Source = other.Source
	f.Predicates = append(f.Predicates, other.Predicates...)
	return f, rewrite.NewTree, nil
}
