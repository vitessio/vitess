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
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type Filter struct {
	Source     Operator
	Predicates []sqlparser.Expr
}

var _ PhysicalOperator = (*Filter)(nil)

// IPhysical implements the PhysicalOperator interface
func (f *Filter) IPhysical() {}

// Clone implements the Operator interface
func (f *Filter) Clone(inputs []Operator) Operator {
	checkSize(inputs, 1)
	predicatesClone := make([]sqlparser.Expr, len(f.Predicates))
	copy(predicatesClone, f.Predicates)
	return &Filter{
		Source:     inputs[0],
		Predicates: predicatesClone,
	}
}

// Inputs implements the Operator interface
func (f *Filter) Inputs() []Operator {
	return []Operator{f.Source}
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

func newFilter(op Operator, expr ...sqlparser.Expr) Operator {
	return &Filter{
		Source: op, Predicates: expr,
	}
}
