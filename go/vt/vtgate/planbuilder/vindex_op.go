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

package planbuilder

import (
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type vindexOp struct {
	opCode  engine.VindexOpcode
	table   vindexTable
	vindex  vindexes.Vindex
	solved  semantics.TableSet
	columns []*sqlparser.ColName
	value   sqlparser.Expr
}

// TableID implements the Operator interface
func (v *vindexOp) TableID() semantics.TableSet {
	return v.solved
}

// UnsolvedPredicates implements the Operator interface
func (v *vindexOp) UnsolvedPredicates(semTable *semantics.SemTable) []sqlparser.Expr {
	return nil
}

// CheckValid implements the Operator interface
func (v *vindexOp) CheckValid() error {
	return nil
}

// IPhysical implements the PhysicalOperator interface
func (v *vindexOp) IPhysical() {}

// Cost implements the PhysicalOperator interface
func (v *vindexOp) Cost() int {
	return int(engine.SelectEqualUnique)
}

// Clone implements the PhysicalOperator interface
func (v *vindexOp) Clone() abstract.PhysicalOperator {
	clone := *v
	return &clone
}

var _ abstract.PhysicalOperator = (*vindexOp)(nil)

func optimizeVindexOp(ctx *planningContext, op *abstract.Vindex) (abstract.PhysicalOperator, error) {
	solves := ctx.semTable.TableSetFor(op.Table.Alias)
	return &vindexOp{
		opCode: op.OpCode,
		table:  vindexTable{table: op.Table},
		vindex: op.Vindex,
		solved: solves,
		value:  op.Value,
	}, nil
}

func (v *vindexOp) pushOutputColumns(columns []*sqlparser.ColName) ([]int, error) {
	idxs := make([]int, len(columns))
outer:
	for i, newCol := range columns {
		for j, existingCol := range v.columns {
			if sqlparser.EqualsExpr(newCol, existingCol) {
				idxs[i] = j
				continue outer
			}
		}
		idxs[i] = len(v.columns)
		v.columns = append(v.columns, newCol)
	}
	return idxs, nil
}
