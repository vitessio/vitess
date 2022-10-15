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
)

type PhysFilter struct {
	Source     PhysicalOperator
	Predicates []sqlparser.Expr
}

var _ PhysicalOperator = (*PhysFilter)(nil)

// IPhysical implements the PhysicalOperator interface
func (f *PhysFilter) IPhysical() {}

// Cost implements the PhysicalOperator interface
func (f *PhysFilter) Cost() int {
	return f.Source.Cost()
}

// Clone implements the PhysicalOperator interface
func (f *PhysFilter) Clone() PhysicalOperator {
	predicatesClone := make([]sqlparser.Expr, len(f.Predicates))
	copy(predicatesClone, f.Predicates)
	return &PhysFilter{
		Source:     f.Source.Clone(),
		Predicates: predicatesClone,
	}
}
