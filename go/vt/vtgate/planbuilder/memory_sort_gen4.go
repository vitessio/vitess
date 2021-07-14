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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

var _ logicalPlan = (*memorySortGen4)(nil)

type memorySortGen4 struct {
	orderBy             []engine.OrderbyParams
	input               logicalPlan
	truncateColumnCount int
}

func (m *memorySortGen4) Order() int {
	panic("implement me")
}

func (m *memorySortGen4) ResultColumns() []*resultColumn {
	panic("implement me")
}

func (m *memorySortGen4) Reorder(i int) {
	panic("implement me")
}

func (m *memorySortGen4) Wireup(lp logicalPlan, jt *jointab) error {
	panic("implement me")
}

func (m *memorySortGen4) WireupGen4(semTable *semantics.SemTable) error {
	return m.input.WireupGen4(semTable)
}

func (m *memorySortGen4) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	panic("implement me")
}

func (m *memorySortGen4) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colNumber int) {
	panic("implement me")
}

func (m *memorySortGen4) SupplyWeightString(colNumber int) (weightcolNumber int, err error) {
	panic("implement me")
}

func (m *memorySortGen4) Primitive() engine.Primitive {
	return &engine.MemorySort{
		UpperLimit:          sqltypes.PlanValue{},
		OrderBy:             m.orderBy,
		Input:               m.input.Primitive(),
		TruncateColumnCount: m.truncateColumnCount,
	}
}

func (m *memorySortGen4) Inputs() []logicalPlan {
	return []logicalPlan{m.input}
}

func (m *memorySortGen4) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 1 {
		return vterrors.New(vtrpcpb.Code_INTERNAL, "[BUG]: expected only 1 input")
	}
	m.input = inputs[0]
	return nil
}

func (m *memorySortGen4) ContainsTables() semantics.TableSet {
	return m.input.ContainsTables()
}
