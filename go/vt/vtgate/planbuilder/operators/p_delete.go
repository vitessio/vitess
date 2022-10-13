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

package operators

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type PhysDelete struct {
	QTable           *QueryTable
	VTable           *vindexes.Table
	OwnedVindexQuery string
	AST              *sqlparser.Delete
}

var _ PhysicalOperator = (*PhysDelete)(nil)
var _ IntroducesTable = (*PhysDelete)(nil)

// TableID implements the PhysicalOperator interface
func (d *PhysDelete) TableID() semantics.TableSet {
	return d.QTable.ID
}

// UnsolvedPredicates implements the PhysicalOperator interface
func (d *PhysDelete) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	return nil
}

// CheckValid implements the PhysicalOperator interface
func (d *PhysDelete) CheckValid() error {
	return nil
}

// IPhysical implements the PhysicalOperator interface
func (d *PhysDelete) IPhysical() {}

// Cost implements the PhysicalOperator interface
func (d *PhysDelete) Cost() int {
	return 1
}

// Clone implements the PhysicalOperator interface
func (d *PhysDelete) Clone() PhysicalOperator {
	return &PhysDelete{
		QTable:           d.QTable,
		VTable:           d.VTable,
		OwnedVindexQuery: d.OwnedVindexQuery,
		AST:              d.AST,
	}
}

// GetQTable implements the IntroducesTable interface
func (d *PhysDelete) GetQTable() *QueryTable {
	return d.QTable
}

// GetVTable implements the IntroducesTable interface
func (d *PhysDelete) GetVTable() *vindexes.Table {
	return d.VTable
}
