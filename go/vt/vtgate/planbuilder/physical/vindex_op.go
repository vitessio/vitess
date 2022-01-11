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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/context"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type VindexOp struct {
	OpCode  engine.VindexOpcode
	Table   abstract.VindexTable
	Vindex  vindexes.Vindex
	Solved  semantics.TableSet
	Columns []*sqlparser.ColName
	Value   sqlparser.Expr
}

// TableID implements the Operator interface
func (v *VindexOp) TableID() semantics.TableSet {
	return v.Solved
}

// UnsolvedPredicates implements the Operator interface
func (v *VindexOp) UnsolvedPredicates(*semantics.SemTable) []sqlparser.Expr {
	return nil
}

// CheckValid implements the Operator interface
func (v *VindexOp) CheckValid() error {
	return nil
}

// IPhysical implements the PhysicalOperator interface
func (v *VindexOp) IPhysical() {}

// Cost implements the PhysicalOperator interface
func (v *VindexOp) Cost() int {
	return int(engine.SelectEqualUnique)
}

// Clone implements the PhysicalOperator interface
func (v *VindexOp) Clone() abstract.PhysicalOperator {
	clone := *v
	return &clone
}

var _ abstract.PhysicalOperator = (*VindexOp)(nil)

func (v *VindexOp) PushOutputColumns(columns []*sqlparser.ColName) ([]int, error) {
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

func optimizeVindexOp(ctx *context.PlanningContext, op *abstract.Vindex) (abstract.PhysicalOperator, error) {
	solves := ctx.SemTable.TableSetFor(op.Table.Alias)
	return &VindexOp{
		OpCode: op.OpCode,
		Table:  op.Table,
		Vindex: op.Vindex,
		Solved: solves,
		Value:  op.Value,
	}, nil
}
