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
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type Vindex struct {
	OpCode  engine.VindexOpcode
	Table   abstract.VindexTable
	Vindex  vindexes.Vindex
	Solved  semantics.TableSet
	Columns []*sqlparser.ColName
	Value   sqlparser.Expr
}

// TableID implements the Operator interface
func (v *Vindex) TableID() semantics.TableSet {
	return v.Solved
}

// UnsolvedPredicates implements the Operator interface
func (v *Vindex) UnsolvedPredicates(*semantics.SemTable) []sqlparser.Expr {
	return nil
}

// CheckValid implements the Operator interface
func (v *Vindex) CheckValid() error {
	return nil
}

// IPhysical implements the PhysicalOperator interface
func (v *Vindex) IPhysical() {}

// Cost implements the PhysicalOperator interface
func (v *Vindex) Cost() int {
	return int(engine.EqualUnique)
}

// Clone implements the PhysicalOperator interface
func (v *Vindex) Clone() abstract.PhysicalOperator {
	clone := *v
	return &clone
}

var _ abstract.PhysicalOperator = (*Vindex)(nil)

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

func optimizeVindex(ctx *plancontext.PlanningContext, op *abstract.Vindex) (abstract.PhysicalOperator, error) {
	solves := ctx.SemTable.TableSetFor(op.Table.Alias)
	return &Vindex{
		OpCode: op.OpCode,
		Table:  op.Table,
		Vindex: op.Vindex,
		Solved: solves,
		Value:  op.Value,
	}, nil
}
