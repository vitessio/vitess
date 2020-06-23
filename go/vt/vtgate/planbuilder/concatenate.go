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
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

type concatenate struct {
	lhs, rhs builder
	order    int
}

var _ builder = (*concatenate)(nil)

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

func (c *concatenate) First() builder {
	panic("implement me")
}

func (c *concatenate) SetUpperLimit(count *sqlparser.SQLVal) {
	// not doing anything by design
}

func (c *concatenate) PushMisc(sel *sqlparser.Select) {
	c.lhs.PushMisc(sel)
	c.rhs.PushMisc(sel)
}

func (c *concatenate) Wireup(bldr builder, jt *jointab) error {
	// TODO systay should we do something different here?
	err := c.lhs.Wireup(bldr, jt)
	if err != nil {
		return err
	}
	return c.rhs.Wireup(bldr, jt)
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

func (c *concatenate) PushFilter(pb *primitiveBuilder, filter sqlparser.Expr, whereType string, origin builder) error {
	return unreachable("Filter")
}

func (c *concatenate) PushSelect(pb *primitiveBuilder, expr *sqlparser.AliasedExpr, origin builder) (rc *resultColumn, colNumber int, err error) {
	return nil, 0, unreachable("Select")
}

func (c *concatenate) MakeDistinct() error {
	return vterrors.New(vtrpc.Code_UNIMPLEMENTED, "only union-all is supported for this operator")
}

func (c *concatenate) PushGroupBy(by sqlparser.GroupBy) error {
	return unreachable("GroupBy")
}

func (c *concatenate) PushOrderBy(by sqlparser.OrderBy) (builder, error) {
	if by == nil {
		return c, nil
	}
	return nil, unreachable("OrderBy")
}

func (c *concatenate) Primitive() engine.Primitive {
	lhs := c.lhs.Primitive()
	rhs := c.rhs.Primitive()

	return &engine.Concatenate{
		Sources: []engine.Primitive{lhs, rhs},
	}
}

// PushLock satisfies the builder interface.
func (c *concatenate) PushLock(lock string) error {
	err := c.lhs.PushLock(lock)
	if err != nil {
		return err
	}
	return c.rhs.PushLock(lock)
}

func unreachable(name string) error {
	return vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "concatenate.%s: unreachable", name)
}
