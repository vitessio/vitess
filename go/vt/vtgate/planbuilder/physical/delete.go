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
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type Delete struct {
	QTable           *abstract.QueryTable
	VTable           *vindexes.Table
	OwnedVindexQuery string
	AST              *sqlparser.Delete
}

var _ abstract.PhysicalOperator = (*Delete)(nil)
var _ abstract.IntroducesTable = (*Delete)(nil)

// TableID implements the PhysicalOperator interface
func (d *Delete) TableID() semantics.TableSet {
	return d.QTable.ID
}

// UnsolvedPredicates implements the PhysicalOperator interface
func (d *Delete) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	return nil
}

// CheckValid implements the PhysicalOperator interface
func (d *Delete) CheckValid() error {
	return nil
}

// IPhysical implements the PhysicalOperator interface
func (d *Delete) IPhysical() {}

// Cost implements the PhysicalOperator interface
func (d *Delete) Cost() int {
	return 1
}

// Clone implements the PhysicalOperator interface
func (d *Delete) Clone() abstract.PhysicalOperator {
	return &Delete{
		QTable:           d.QTable,
		VTable:           d.VTable,
		OwnedVindexQuery: d.OwnedVindexQuery,
		AST:              d.AST,
	}
}

// GetQTable implements the IntroducesTable interface
func (d *Delete) GetQTable() *abstract.QueryTable {
	return d.QTable
}

// GetVTable implements the IntroducesTable interface
func (d *Delete) GetVTable() *vindexes.Table {
	return d.VTable
}
