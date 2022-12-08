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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type Delete struct {
	QTable           *QueryTable
	VTable           *vindexes.Table
	OwnedVindexQuery string
	AST              *sqlparser.Delete

	noInputs
	noColumns
	noPredicates
}

var _ ops.PhysicalOperator = (*Delete)(nil)

// Introduces implements the PhysicalOperator interface
func (d *Delete) Introduces() semantics.TableSet {
	return d.QTable.ID
}

// IPhysical implements the PhysicalOperator interface
func (d *Delete) IPhysical() {}

// Clone implements the Operator interface
func (d *Delete) Clone(inputs []ops.Operator) ops.Operator {
	return &Delete{
		QTable:           d.QTable,
		VTable:           d.VTable,
		OwnedVindexQuery: d.OwnedVindexQuery,
		AST:              d.AST,
	}
}

func (d *Delete) TablesUsed() []string {
	if d.VTable != nil {
		return SingleQualifiedIdentifier(d.VTable.Keyspace, d.VTable.Name)
	}
	return nil
}
