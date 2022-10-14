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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type PhysVindex struct {
	OpCode  engine.VindexOpcode
	Table   VindexTable
	Vindex  vindexes.Vindex
	Solved  semantics.TableSet
	Columns []*sqlparser.ColName
	Value   sqlparser.Expr
}

// TableID implements the Operator interface
func (v *PhysVindex) TableID() semantics.TableSet {
	return v.Solved
}

// UnsolvedPredicates implements the Operator interface
func (v *PhysVindex) UnsolvedPredicates(*semantics.SemTable) []sqlparser.Expr {
	return nil
}

// CheckValid implements the Operator interface
func (v *PhysVindex) CheckValid() error {
	return nil
}

// IPhysical implements the PhysicalOperator interface
func (v *PhysVindex) IPhysical() {}

// Cost implements the PhysicalOperator interface
func (v *PhysVindex) Cost() int {
	return int(engine.EqualUnique)
}

// Clone implements the PhysicalOperator interface
func (v *PhysVindex) Clone() PhysicalOperator {
	clone := *v
	return &clone
}

var _ PhysicalOperator = (*PhysVindex)(nil)

func (v *PhysVindex) PushOutputColumns(columns []*sqlparser.ColName) ([]int, error) {
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

func optimizeVindex(ctx *plancontext.PlanningContext, op *Vindex) (PhysicalOperator, error) {
	solves := ctx.SemTable.TableSetFor(op.Table.Alias)
	return &PhysVindex{
		OpCode: op.OpCode,
		Table:  op.Table,
		Vindex: op.Vindex,
		Solved: solves,
		Value:  op.Value,
	}, nil
}
