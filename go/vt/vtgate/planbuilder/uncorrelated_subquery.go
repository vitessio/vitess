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

var _ logicalPlan = (*uncorrelatedSubquery)(nil)

// uncorrelatedSubquery is the logicalPlan for engine.UncorrelatedSubquery.
// This gets built if a subquery is not correlated and can
// therefore can be pulled out and executed upfront.
type uncorrelatedSubquery struct {
	subquery  logicalPlan
	outer     logicalPlan
	eSubquery *engine.UncorrelatedSubquery
}

// newUncorrelatedSubquery builds a new uncorrelatedSubquery.
func newUncorrelatedSubquery(opcode popcode.PulloutOpcode, sqName, hasValues string, subquery, outer logicalPlan) *uncorrelatedSubquery {
	return &uncorrelatedSubquery{
		subquery: subquery,
		outer:    outer,
		eSubquery: &engine.UncorrelatedSubquery{
			Opcode:         opcode,
			SubqueryResult: sqName,
			HasValues:      hasValues,
		},
	}
}

// Primitive implements the logicalPlan interface
func (ps *uncorrelatedSubquery) Primitive() engine.Primitive {
	ps.eSubquery.Subquery = ps.subquery.Primitive()
	ps.eSubquery.Outer = ps.outer.Primitive()
	return ps.eSubquery
}

// Wireup implements the logicalPlan interface
func (ps *uncorrelatedSubquery) Wireup(ctx *plancontext.PlanningContext) error {
	if err := ps.outer.Wireup(ctx); err != nil {
		return err
	}
	return ps.subquery.Wireup(ctx)
}

// Rewrite implements the logicalPlan interface
func (ps *uncorrelatedSubquery) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 2 {
		return vterrors.VT13001("uncorrelatedSubquery: wrong number of inputs")
	}
	ps.outer = inputs[0]
	ps.subquery = inputs[1]
	return nil
}

// ContainsTables implements the logicalPlan interface
func (ps *uncorrelatedSubquery) ContainsTables() semantics.TableSet {
	return ps.outer.ContainsTables().Merge(ps.subquery.ContainsTables())
}

// Inputs implements the logicalPlan interface
func (ps *uncorrelatedSubquery) Inputs() []logicalPlan {
	return []logicalPlan{ps.outer, ps.subquery}
}

// OutputColumns implements the logicalPlan interface
func (ps *uncorrelatedSubquery) OutputColumns() []sqlparser.SelectExpr {
	return ps.outer.OutputColumns()
}
