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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type Update struct {
	QTable              *QueryTable
	VTable              *vindexes.Table
	Assignments         map[string]sqlparser.Expr
	ChangedVindexValues map[string]*engine.VindexValues
	OwnedVindexQuery    string
	AST                 *sqlparser.Update

	noInputs
	noColumns
	noPredicates
}

// Introduces implements the PhysicalOperator interface
func (u *Update) Introduces() semantics.TableSet {
	return u.QTable.ID
}

// Clone implements the Operator interface
func (u *Update) Clone(inputs []ops.Operator) ops.Operator {
	return &Update{
		QTable:              u.QTable,
		VTable:              u.VTable,
		Assignments:         u.Assignments,
		ChangedVindexValues: u.ChangedVindexValues,
		OwnedVindexQuery:    u.OwnedVindexQuery,
		AST:                 u.AST,
	}
}

func (u *Update) GetOrdering() ([]ops.OrderBy, error) {
	return nil, nil
}

func (u *Update) TablesUsed() []string {
	if u.VTable != nil {
		return SingleQualifiedIdentifier(u.VTable.Keyspace, u.VTable.Name)
	}
	return nil
}

func (u *Update) Description() ops.OpDescription {
	return ops.OpDescription{
		OperatorType: "Update",
	}
}

func (u *Update) ShortDescription() string {
	return u.VTable.String()
}
