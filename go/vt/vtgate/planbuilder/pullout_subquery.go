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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	popcode "vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
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
func newPulloutSubquery(opcode popcode.PulloutOpcode, sqName, hasValues string, subquery logicalPlan) *pulloutSubquery {
	return &pulloutSubquery{
		subquery: subquery,
		eSubquery: &engine.PulloutSubquery{
			Opcode:         opcode,
			SubqueryResult: sqName,
			HasValues:      hasValues,
		},
	}
}

// Primitive implements the logicalPlan interface
func (ps *pulloutSubquery) Primitive() engine.Primitive {
	ps.eSubquery.Subquery = ps.subquery.Primitive()
	ps.eSubquery.Underlying = ps.underlying.Primitive()
	return ps.eSubquery
}

// WireupGen4 implements the logicalPlan interface
func (ps *pulloutSubquery) Wireup(ctx *plancontext.PlanningContext) error {
	if err := ps.underlying.Wireup(ctx); err != nil {
		return err
	}
	return ps.subquery.Wireup(ctx)
}

// Rewrite implements the logicalPlan interface
func (ps *pulloutSubquery) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 2 {
		return vterrors.VT13001("pulloutSubquery: wrong number of inputs")
	}
	ps.underlying = inputs[0]
	ps.subquery = inputs[1]
	return nil
}

// ContainsTables implements the logicalPlan interface
func (ps *pulloutSubquery) ContainsTables() semantics.TableSet {
	return ps.underlying.ContainsTables().Merge(ps.subquery.ContainsTables())
}

// Inputs implements the logicalPlan interface
func (ps *pulloutSubquery) Inputs() []logicalPlan {
	return []logicalPlan{ps.underlying, ps.subquery}
}

// OutputColumns implements the logicalPlan interface
func (ps *pulloutSubquery) OutputColumns() []sqlparser.SelectExpr {
	return ps.underlying.OutputColumns()
}
