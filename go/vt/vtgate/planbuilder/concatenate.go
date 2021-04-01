/*
Copyright 2020 The Vitess Authors.

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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type concatenate struct {
	lhs, rhs logicalPlan
	order    int
}

var _ logicalPlan = (*concatenate)(nil)

func (c *concatenate) Order() int {
	return c.order
}

func (c *concatenate) ResultColumns() []*resultColumn {
	return c.lhs.ResultColumns()
}

func (c *concatenate) Reorder(order int) {
	c.lhs.Reorder(order)
	c.rhs.Reorder(c.lhs.Order())
	c.order = c.rhs.Order() + 1
}

func (c *concatenate) Wireup(plan logicalPlan, jt *jointab) error {
	// TODO systay should we do something different here?
	err := c.lhs.Wireup(plan, jt)
	if err != nil {
		return err
	}
	return c.rhs.Wireup(plan, jt)
}

func (c *concatenate) WireupV4(semTable *semantics.SemTable) error {
	err := c.lhs.WireupV4(semTable)
	if err != nil {
		return err
	}
	return c.rhs.WireupV4(semTable)
}

func (c *concatenate) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	panic("implement me")
}

func (c *concatenate) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colNumber int) {
	panic("implement me")
}

func (c *concatenate) SupplyWeightString(colNumber int) (weightcolNumber int, err error) {
	panic("implement me")
}

func (c *concatenate) Primitive() engine.Primitive {
	lhs := c.lhs.Primitive()
	rhs := c.rhs.Primitive()

	return &engine.Concatenate{
		Sources: []engine.Primitive{lhs, rhs},
	}
}

// Rewrite implements the logicalPlan interface
func (c *concatenate) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 2 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "concatenate: wrong number of inputs")
	}
	c.lhs = inputs[0]
	c.rhs = inputs[1]
	return nil
}

func (c *concatenate) ContainsTables() semantics.TableSet {
	return c.lhs.ContainsTables().Merge(c.rhs.ContainsTables())
}

// Inputs implements the logicalPlan interface
func (c *concatenate) Inputs() []logicalPlan {
	return []logicalPlan{c.lhs, c.rhs}
}
