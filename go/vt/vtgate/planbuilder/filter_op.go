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

package planbuilder

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type filterOp struct {
	source     abstract.PhysicalOperator
	predicates []sqlparser.Expr
}

var _ abstract.PhysicalOperator = (*filterOp)(nil)

// IPhysical implements the PhysicalOperator interface
func (f *filterOp) IPhysical() {}

// TableID implements the PhysicalOperator interface
func (f *filterOp) TableID() semantics.TableSet {
	return f.source.TableID()
}

// PushPredicate implements the PhysicalOperator interface
func (f *filterOp) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) error {
	panic("unimplemented")
}

// UnsolvedPredicates implements the PhysicalOperator interface
func (f *filterOp) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	panic("implement me")
}

// CheckValid implements the PhysicalOperator interface
func (f *filterOp) CheckValid() error {
	return f.source.CheckValid()
}

// Compact implements the PhysicalOperator interface
func (f *filterOp) Compact(semTable *semantics.SemTable) (abstract.Operator, error) {
	return f, nil
}

// Cost implements the PhysicalOperator interface
func (f *filterOp) Cost() int {
	return f.source.Cost()
}

// Clone implements the PhysicalOperator interface
func (f *filterOp) Clone() abstract.PhysicalOperator {
	var predicatesClone []sqlparser.Expr
	for _, predicate := range f.predicates {
		predicatesClone = append(predicatesClone, sqlparser.CloneExpr(predicate))
	}
	return &filterOp{
		source:     f.source.Clone(),
		predicates: predicatesClone,
	}
}
