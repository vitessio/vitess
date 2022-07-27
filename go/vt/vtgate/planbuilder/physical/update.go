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
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type Update struct {
	QTable              *abstract.QueryTable
	VTable              *vindexes.Table
	Assignments         map[string]sqlparser.Expr
	ChangedVindexValues map[string]*engine.VindexValues
	OwnedVindexQuery    string
	AST                 *sqlparser.Update
}

var _ abstract.PhysicalOperator = (*Update)(nil)
var _ abstract.IntroducesTable = (*Update)(nil)

// TableID implements the PhysicalOperator interface
func (u *Update) TableID() semantics.TableSet {
	return u.QTable.ID
}

// UnsolvedPredicates implements the PhysicalOperator interface
func (u *Update) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	return nil
}

// CheckValid implements the PhysicalOperator interface
func (u *Update) CheckValid() error {
	return nil
}

// IPhysical implements the PhysicalOperator interface
func (u *Update) IPhysical() {}

// Cost implements the PhysicalOperator interface
func (u *Update) Cost() int {
	return 1
}

// Clone implements the PhysicalOperator interface
func (u *Update) Clone() abstract.PhysicalOperator {
	return &Update{
		QTable:              u.QTable,
		VTable:              u.VTable,
		Assignments:         u.Assignments,
		ChangedVindexValues: u.ChangedVindexValues,
		OwnedVindexQuery:    u.OwnedVindexQuery,
		AST:                 u.AST,
	}
}

// GetQTable implements the IntroducesTable interface
func (u *Update) GetQTable() *abstract.QueryTable {
	return u.QTable
}

// GetVTable implements the IntroducesTable interface
func (u *Update) GetVTable() *vindexes.Table {
	return u.VTable
}
