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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type Delete struct {
	QTable           *QueryTable
	VTable           *vindexes.Table
	OwnedVindexQuery string
	AST              *sqlparser.Delete

	noInputs
}

func (d *Delete) AddColumn(*plancontext.PlanningContext, sqlparser.Expr) (int, error) {
	return 0, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "tried to push output column to delete")
}

var _ PhysicalOperator = (*Delete)(nil)

// Introduces implements the PhysicalOperator interface
func (d *Delete) Introduces() semantics.TableSet {
	return d.QTable.ID
}

// IPhysical implements the PhysicalOperator interface
func (d *Delete) IPhysical() {}

// Clone implements the Operator interface
func (d *Delete) Clone(inputs []Operator) Operator {
	checkSize(inputs, 0)
	return &Delete{
		QTable:           d.QTable,
		VTable:           d.VTable,
		OwnedVindexQuery: d.OwnedVindexQuery,
		AST:              d.AST,
	}
}

func (d *Delete) AddPredicate(*plancontext.PlanningContext, sqlparser.Expr) (Operator, error) {
	return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "we cannot push columns into %T", d)
}
