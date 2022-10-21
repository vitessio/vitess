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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type Table struct {
	QTable  *QueryTable
	VTable  *vindexes.Table
	Columns []*sqlparser.ColName

	noInputs
}

var _ PhysicalOperator = (*Table)(nil)

// IPhysical implements the PhysicalOperator interface
func (to *Table) IPhysical() {}

// Clone implements the Operator interface
func (to *Table) Clone(inputs []Operator) Operator {
	checkSize(inputs, 0)
	var columns []*sqlparser.ColName
	for _, name := range to.Columns {
		columns = append(columns, sqlparser.CloneRefOfColName(name))
	}
	return &Table{
		QTable:  to.QTable,
		VTable:  to.VTable,
		Columns: columns,
	}
}

// Introduces implements the PhysicalOperator interface
func (to *Table) Introduces() semantics.TableSet {
	return to.QTable.ID
}

// PushPredicate implements the PhysicalOperator interface
func (to *Table) PushPredicate(expr sqlparser.Expr, semTable *semantics.SemTable) error {
	return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "we should not push Predicates into a Table. It is meant to be immutable")
}
