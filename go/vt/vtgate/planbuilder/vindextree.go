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

package planbuilder

import (
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

type vindexTree struct {
	opCode  engine.VindexOpcode
	table   vindexTable
	vindex  vindexes.Vindex
	solved  semantics.TableSet
	columns []*sqlparser.ColName
	value   sqltypes.PlanValue
}

var _ queryTree = (*vindexTree)(nil)

func (v *vindexTree) tableID() semantics.TableSet {
	return v.solved
}

func (v *vindexTree) clone() queryTree {
	clone := *v
	return &clone
}

func (v *vindexTree) cost() int {
	return int(engine.SelectEqualUnique)
}

func (v *vindexTree) pushOutputColumns(col []*sqlparser.ColName, _ *semantics.SemTable) ([]int, error) {
	idxs := make([]int, len(col))
outer:
	for i, newCol := range col {
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
