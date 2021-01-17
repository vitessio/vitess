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
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

var _ logicalPlan = (*pulloutSubquery)(nil)

// pulloutSubquery is the logicalPlan for engine.PulloutSubquery.
// This gets built if a subquery is not correlated and can
// therefore can be pulled out and executed upfront.
type pulloutSubquery struct {
	order      int
	subquery   logicalPlan
	underlying logicalPlan
	eSubquery  *engine.PulloutSubquery
}

// newPulloutSubquery builds a new pulloutSubquery.
func newPulloutSubquery(opcode engine.PulloutOpcode, sqName, hasValues string, subquery logicalPlan) *pulloutSubquery {
	return &pulloutSubquery{
		subquery: subquery,
		eSubquery: &engine.PulloutSubquery{
			Opcode:         opcode,
			SubqueryResult: sqName,
			HasValues:      hasValues,
		},
	}
}

// setUnderlying sets the underlying primitive.
func (ps *pulloutSubquery) setUnderlying(underlying logicalPlan) {
	ps.underlying = underlying
	ps.underlying.Reorder(ps.subquery.Order())
	ps.order = ps.underlying.Order() + 1
}

// Order implements the logicalPlan interface
func (ps *pulloutSubquery) Order() int {
	return ps.order
}

// Reorder implements the logicalPlan interface
func (ps *pulloutSubquery) Reorder(order int) {
	ps.subquery.Reorder(order)
	ps.underlying.Reorder(ps.subquery.Order())
	ps.order = ps.underlying.Order() + 1
}

// Primitive implements the logicalPlan interface
func (ps *pulloutSubquery) Primitive() engine.Primitive {
	ps.eSubquery.Subquery = ps.subquery.Primitive()
	ps.eSubquery.Underlying = ps.underlying.Primitive()
	return ps.eSubquery
}

// ResultColumns implements the logicalPlan interface
func (ps *pulloutSubquery) ResultColumns() []*resultColumn {
	return ps.underlying.ResultColumns()
}

// Wireup implements the logicalPlan interface
func (ps *pulloutSubquery) Wireup(plan logicalPlan, jt *jointab) error {
	if err := ps.underlying.Wireup(plan, jt); err != nil {
		return err
	}
	return ps.subquery.Wireup(plan, jt)
}

// Wireup2 implements the logicalPlan interface
func (ps *pulloutSubquery) WireupV4(semTable *semantics.SemTable) error {
	if err := ps.underlying.WireupV4(semTable); err != nil {
		return err
	}
	return ps.subquery.WireupV4(semTable)
}

// SupplyVar implements the logicalPlan interface
func (ps *pulloutSubquery) SupplyVar(from, to int, col *sqlparser.ColName, varname string) {
	if from <= ps.subquery.Order() {
		ps.subquery.SupplyVar(from, to, col, varname)
		return
	}
	ps.underlying.SupplyVar(from, to, col, varname)
}

// SupplyCol implements the logicalPlan interface
func (ps *pulloutSubquery) SupplyCol(col *sqlparser.ColName) (rc *resultColumn, colNumber int) {
	return ps.underlying.SupplyCol(col)
}

// SupplyWeightString implements the logicalPlan interface
func (ps *pulloutSubquery) SupplyWeightString(colNumber int) (weightcolNumber int, err error) {
	return ps.underlying.SupplyWeightString(colNumber)
}

// Rewrite implements the logicalPlan interface
func (ps *pulloutSubquery) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 2 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "pulloutSubquery: wrong number of inputs")
	}
	ps.underlying = inputs[0]
	ps.subquery = inputs[1]
	return nil
}

// Solves implements the logicalPlan interface
func (ps *pulloutSubquery) ContainsTables() semantics.TableSet {
	return ps.underlying.ContainsTables().Merge(ps.subquery.ContainsTables())
}

// Inputs implements the logicalPlan interface
func (ps *pulloutSubquery) Inputs() []logicalPlan {
	return []logicalPlan{ps.underlying, ps.subquery}
}
