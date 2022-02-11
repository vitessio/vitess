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
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type projection struct {
	gen4Plan
	source      logicalPlan
	columnNames []string
	columns     []sqlparser.Expr
	primitive   *engine.Projection
}

var _ logicalPlan = (*projection)(nil)

// WireupGen4 implements the logicalPlan interface
func (p *projection) WireupGen4(semTable *semantics.SemTable) error {
	columns := make([]evalengine.Expr, 0, len(p.columns))
	for _, expr := range p.columns {
		convert, err := evalengine.Convert(expr, semTable)
		if err != nil {
			return err
		}
		columns = append(columns, convert)
	}
	p.primitive = &engine.Projection{
		Cols:  p.columnNames,
		Exprs: columns,
		Input: p.source.Primitive(),
	}

	return p.source.WireupGen4(semTable)
}

// Inputs implements the logicalPlan interface
func (p *projection) Inputs() []logicalPlan {
	return []logicalPlan{p.source}
}

// Rewrite implements the logicalPlan interface
func (p *projection) Rewrite(inputs ...logicalPlan) error {
	if len(inputs) != 1 {
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "wrong number of inputs")
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
	columns := p.source.OutputColumns()
	for i, expr := range p.columns {
		columns = append(columns, &sqlparser.AliasedExpr{
			Expr: expr,
			As:   sqlparser.NewColIdent(p.columnNames[i]),
		})
	}
	return columns
}

// Primitive implements the logicalPlan interface
func (p *projection) Primitive() engine.Primitive {
	if p.primitive == nil {
		panic("WireUp not yet run")
	}
	return p.primitive
}
