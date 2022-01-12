/*
Copyright 2022 The Vitess Authors.

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

type Union struct {
	Sources     []abstract.PhysicalOperator
	SelectStmts []*sqlparser.Select
	Distinct    bool
}

var _ abstract.PhysicalOperator = (*Union)(nil)

func (u *Union) TableID() semantics.TableSet {
	ts := semantics.EmptyTableSet()
	for _, source := range u.Sources {
		ts.MergeInPlace(source.TableID())
	}
	return ts
}

func (u *Union) UnsolvedPredicates(*semantics.SemTable) []sqlparser.Expr {
	panic("implement me")
}

func (u *Union) CheckValid() error {
	return nil
}

func (u *Union) IPhysical() {}

func (u *Union) Cost() int {
	cost := 0
	for _, source := range u.Sources {
		cost += source.Cost()
	}
	return cost
}

func (u *Union) Clone() abstract.PhysicalOperator {
	newOp := &Union{Distinct: u.Distinct}
	newOp.Sources = make([]abstract.PhysicalOperator, 0, len(u.Sources))
	for _, source := range u.Sources {
		newOp.Sources = append(newOp.Sources, source.Clone())
	}
	return newOp
}
