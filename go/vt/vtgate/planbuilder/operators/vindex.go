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
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type (
	Vindex struct {
		OpCode  engine.VindexOpcode
		Table   VindexTable
		Vindex  vindexes.Vindex
		Solved  semantics.TableSet
		Columns []*sqlparser.ColName
		Value   sqlparser.Expr

		noInputs
	}

	// VindexTable contains information about the vindex table we want to query
	VindexTable struct {
		TableID    semantics.TableSet
		Alias      *sqlparser.AliasedTableExpr
		Table      sqlparser.TableName
		Predicates []sqlparser.Expr
		VTable     *vindexes.Table
	}
)

const vindexUnsupported = "unsupported: where clause for vindex function must be of the form id = <val> or id in(<val>,...)"

// Introduces implements the Operator interface
func (v *Vindex) Introduces() semantics.TableSet {
	return v.Solved
}

// IPhysical implements the PhysicalOperator interface
func (v *Vindex) IPhysical() {}

// Clone implements the Operator interface
func (v *Vindex) Clone(inputs []Operator) Operator {
	checkSize(inputs, 0)
	clone := *v
	return &clone
}

var _ PhysicalOperator = (*Vindex)(nil)

func (v *Vindex) PushOutputColumns(columns []*sqlparser.ColName) ([]int, error) {
	idxs := make([]int, len(columns))
outer:
	for i, newCol := range columns {
		for j, existingCol := range v.Columns {
			if sqlparser.EqualsExpr(newCol, existingCol) {
				idxs[i] = j
				continue outer
			}
		}
		idxs[i] = len(v.Columns)
		v.Columns = append(v.Columns, newCol)
	}
	return idxs, nil
}

// CheckValid implements the Operator interface
func (v *Vindex) CheckValid() error {
	if len(v.Table.Predicates) == 0 {
		return vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: where clause for vindex function must be of the form id = <val> or id in(<val>,...) (where clause missing)")
	}

	return nil
}
