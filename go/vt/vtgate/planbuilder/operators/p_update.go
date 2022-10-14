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
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type PhysUpdate struct {
	QTable              *QueryTable
	VTable              *vindexes.Table
	Assignments         map[string]sqlparser.Expr
	ChangedVindexValues map[string]*engine.VindexValues
	OwnedVindexQuery    string
	AST                 *sqlparser.Update
}

var _ PhysicalOperator = (*PhysUpdate)(nil)
var _ IntroducesTable = (*PhysUpdate)(nil)

// Introduces implements the PhysicalOperator interface
func (u *PhysUpdate) Introduces() semantics.TableSet {
	return u.QTable.ID
}

// CheckValid implements the PhysicalOperator interface
func (u *PhysUpdate) CheckValid() error {
	return nil
}

// IPhysical implements the PhysicalOperator interface
func (u *PhysUpdate) IPhysical() {}

// Cost implements the PhysicalOperator interface
func (u *PhysUpdate) Cost() int {
	return 1
}

// Clone implements the PhysicalOperator interface
func (u *PhysUpdate) Clone() PhysicalOperator {
	return &PhysUpdate{
		QTable:              u.QTable,
		VTable:              u.VTable,
		Assignments:         u.Assignments,
		ChangedVindexValues: u.ChangedVindexValues,
		OwnedVindexQuery:    u.OwnedVindexQuery,
		AST:                 u.AST,
	}
}

// GetQTable implements the IntroducesTable interface
func (u *PhysUpdate) GetQTable() *QueryTable {
	return u.QTable
}

// GetVTable implements the IntroducesTable interface
func (u *PhysUpdate) GetVTable() *vindexes.Table {
	return u.VTable
}
