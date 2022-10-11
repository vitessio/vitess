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

package physical

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type Filter struct {
	Source     abstract.PhysicalOperator
	Predicates []sqlparser.Expr
}

var _ abstract.PhysicalOperator = (*Filter)(nil)

// IPhysical implements the PhysicalOperator interface
func (f *Filter) IPhysical() {}

// TableID implements the PhysicalOperator interface
func (f *Filter) TableID() semantics.TableSet {
	return f.Source.TableID()
}

// UnsolvedPredicates implements the PhysicalOperator interface
func (f *Filter) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	panic("implement me")
}

// CheckValid implements the PhysicalOperator interface
func (f *Filter) CheckValid() error {
	return f.Source.CheckValid()
}

// Compact implements the PhysicalOperator interface
func (f *Filter) Compact(semTable *semantics.SemTable) (abstract.Operator, error) {
	return f, nil
}

// Cost implements the PhysicalOperator interface
func (f *Filter) Cost() int {
	return f.Source.Cost()
}

// Clone implements the PhysicalOperator interface
func (f *Filter) Clone() abstract.PhysicalOperator {
	predicatesClone := make([]sqlparser.Expr, len(f.Predicates))
	copy(predicatesClone, f.Predicates)
	return &Filter{
		Source:     f.Source.Clone(),
		Predicates: predicatesClone,
	}
}
