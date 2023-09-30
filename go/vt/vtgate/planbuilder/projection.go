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

package planbuilder

import (
	"fmt"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type projection struct {
	source      logicalPlan
	columnNames []string
	columns     []sqlparser.Expr
	primitive   *engine.Projection
	// unorderedColumnIdx is used to find the index at which we should add any column output from projection
	// we don't care for the ordering of. It should also be updated when such a column is added
	unorderedColumnIdx int
}

var _ logicalPlan = (*projection)(nil)

// Wireup implements the logicalPlan interface
func (p *projection) Wireup(ctx *plancontext.PlanningContext) error {
	if p.primitive == nil {
		return vterrors.VT13001("should already be done")
	}

	return p.source.Wireup(ctx)
}

// Inputs implements the logicalPlan interface
func (p *projection) Inputs() []logicalPlan {
	return []logicalPlan{p.source}
}

// Rewrite implements the logicalPlan interface
func (p *projection) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 1 {
		return vterrors.VT13001(fmt.Sprintf("wrong number of inputs, got: %d; expected: %d", len(inputs), 1))
	}
	p.source = inputs[0]
	return nil
}

// ContainsTables implements the logicalPlan interface
func (p *projection) ContainsTables() semantics.TableSet {
	return p.source.ContainsTables()
}

// OutputColumns implements the logicalPlan interface
func (p *projection) OutputColumns() []sqlparser.SelectExpr {
	columns := make([]sqlparser.SelectExpr, 0, len(p.columns))
	for i, expr := range p.columns {
		columns = append(columns, &sqlparser.AliasedExpr{
			Expr: expr,
			As:   sqlparser.NewIdentifierCI(p.columnNames[i]),
		})
	}
	return columns
}

// Primitive implements the logicalPlan interface
func (p *projection) Primitive() engine.Primitive {
	if p.primitive == nil {
		panic("WireUp not yet run")
	}
	p.primitive.Input = p.source.Primitive()
	return p.primitive
}
