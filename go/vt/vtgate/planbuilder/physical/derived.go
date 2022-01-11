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

type Derived struct {
	Source abstract.PhysicalOperator

	Query         sqlparser.SelectStatement
	Alias         string
	ColumnAliases sqlparser.Columns

	// Columns needed to feed other plans
	Columns       []*sqlparser.ColName
	ColumnsOffset []int
}

var _ abstract.PhysicalOperator = (*Derived)(nil)

// TableID implements the PhysicalOperator interface
func (d *Derived) TableID() semantics.TableSet {
	return d.Source.TableID()
}

// UnsolvedPredicates implements the PhysicalOperator interface
func (d *Derived) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	return d.Source.UnsolvedPredicates(semTable)
}

// CheckValid implements the PhysicalOperator interface
func (d *Derived) CheckValid() error {
	return d.Source.CheckValid()
}

// IPhysical implements the PhysicalOperator interface
func (d *Derived) IPhysical() {}

// Cost implements the PhysicalOperator interface
func (d *Derived) Cost() int {
	return d.Source.Cost()
}

// Clone implements the PhysicalOperator interface
func (d *Derived) Clone() abstract.PhysicalOperator {
	clone := *d
	clone.Source = d.Source.Clone()
	clone.ColumnAliases = sqlparser.CloneColumns(d.ColumnAliases)
	clone.Columns = make([]*sqlparser.ColName, 0, len(d.Columns))
	for _, x := range d.Columns {
		clone.Columns = append(clone.Columns, sqlparser.CloneRefOfColName(x))
	}
	clone.ColumnsOffset = make([]int, 0, len(d.ColumnsOffset))
	copy(clone.ColumnsOffset, d.ColumnsOffset)
	return &clone
}
