/*
Copyright 2019 The Vitess Authors.

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
)

var _ builder = (*pulloutSubquery)(nil)

// pulloutSubquery is the builder for engine.PulloutSubquery.
// This gets built if a subquery is not correlated and can
// therefore can be pulled out and executed upfront.
type pulloutSubquery struct {
	order      int
	subquery   builder
	underlying builder
	eSubquery  *engine.PulloutSubquery
}

// newPulloutSubquery builds a new pulloutSubquery.
func newPulloutSubquery(opcode engine.PulloutOpcode, sqName, hasValues string, subquery builder) *pulloutSubquery {
	return &pulloutSubquery{
		subquery: subquery,
		eSubquery: &engine.PulloutSubquery{
			Opcode:         opcode,
			SubqueryResult: sqName,
			HasValues:      hasValues,
		},
	}
}

func (ps *pulloutSubquery) getInput() builder {
	return ps.underlying
}

// setUnderlying sets the underlying primitive.
func (ps *pulloutSubquery) setUnderlying(underlying builder) {
	ps.underlying = underlying
	ps.underlying.Reorder(ps.subquery.Order())
	ps.order = ps.underlying.Order() + 1
}

// Order satisfies the builder interface.
func (ps *pulloutSubquery) Order() int {
	return ps.order
}

// Reorder satisfies the builder interface.
func (ps *pulloutSubquery) Reorder(order int) {
	ps.subquery.Reorder(order)
	ps.underlying.Reorder(ps.subquery.Order())
	ps.order = ps.underlying.Order() + 1
}

// Primitive satisfies the builder interface.
func (ps *pulloutSubquery) Primitive() engine.Primitive {
	ps.eSubquery.Subquery = ps.subquery.Primitive()
	ps.eSubquery.Underlying = ps.underlying.Primitive()
	return ps.eSubquery
}

// First satisfies the builder interface.
func (ps *pulloutSubquery) First() builder {
	return ps.underlying.First()
}

// ResultColumns satisfies the builder interface.
func (ps *pulloutSubquery) ResultColumns() []*resultColumn {
	return ps.underlying.ResultColumns()
}

// SetUpperLimit satisfies the builder interface.
// This is a no-op because we actually call SetLimit for this primitive.
// In the future, we may have to honor this call for subqueries.
func (ps *pulloutSubquery) SetUpperLimit(count sqlparser.Expr) {
	ps.underlying.SetUpperLimit(count)
}

// PushMisc satisfies the builder interface.
func (ps *pulloutSubquery) PushMisc(sel *sqlparser.Select) error {
	err := ps.subquery.PushMisc(sel)
	if err != nil {
		return err
	}
	return ps.underlying.PushMisc(sel)
}

// Wireup satisfies the builder interface.
func (ps *pulloutSubquery) Wireup(bldr builder, jt *jointab) error {
	if err := ps.underlying.Wireup(bldr, jt); err != nil {
		return err
	}
	return ps.subquery.Wireup(bldr, jt)
}

// SupplyVar satisfies the builder interface.
func (ps *pulloutSubquery) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	if from <= ps.subquery.Order() {
		ps.subquery.SupplyVar(from, to, col, varname)
		return
	}
	ps.underlying.SupplyVar(from, to, col, varname)
}

// SupplyCol satisfies the builder interface.
func (ps *pulloutSubquery) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colNumber int) {
	return ps.underlying.SupplyCol(col)
}

// SupplyWeightString satisfies the builder interface.
func (ps *pulloutSubquery) SupplyWeightString(colNumber int) (weightcolNumber int, err error) {
	return ps.underlying.SupplyWeightString(colNumber)
}

func (ps *pulloutSubquery) Rewrite(inputs ...builder) error {
	if len(inputs) != 2 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "pulloutSubquery: wrong number of inputs")
	}
	ps.underlying = inputs[0]
	ps.subquery = inputs[1]
	return nil
}

func (ps *pulloutSubquery) Inputs() []builder {
	return []builder{ps.underlying, ps.subquery}
}
