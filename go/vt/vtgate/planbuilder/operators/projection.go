/*
Copyright 2023 The Vitess Authors.

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

package operators

import (
	"golang.org/x/exp/slices"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type (
	// Projection is used when we need to evaluate expressions on the vtgate
	// It uses the evalengine to accomplish its goal
	Projection struct {
		Source      ops.Operator
		ColumnNames []string
		Columns     []ProjExpr
	}
	ProjExpr interface {
		GetExpr() sqlparser.Expr
	}

	// Offset is used when we are only passing through data from an incoming column
	Offset struct {
		Expr   sqlparser.Expr
		Offset int
	}

	// Eval is used for expressions that have to be evaluated in the vtgate using the evalengine
	Eval struct {
		Expr  sqlparser.Expr
		EExpr evalengine.Expr
	}

	// Expr is used before we have planned, or if we are able to push this down to mysql
	Expr struct {
		E sqlparser.Expr
	}
)

func (p *Projection) AddColumn(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr) (ops.Operator, int, error) {
	// TODO implement me
	panic("implement me")
}

func (po Offset) GetExpr() sqlparser.Expr { return po.Expr }
func (po Eval) GetExpr() sqlparser.Expr   { return po.Expr }
func (po Expr) GetExpr() sqlparser.Expr   { return po.E }

func NewProjection(src ops.Operator) *Projection {
	return &Projection{
		Source: src,
	}
}

func (p *Projection) Clone(inputs []ops.Operator) ops.Operator {
	return &Projection{
		Source:      inputs[0],
		ColumnNames: slices.Clone(p.ColumnNames),
		Columns:     slices.Clone(p.Columns),
	}
}

func (p *Projection) Inputs() []ops.Operator {
	return []ops.Operator{p.Source}
}

func (p *Projection) SetInputs(operators []ops.Operator) {
	p.Source = operators[0]
}

func (p *Projection) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	// we just pass through the predicate to our source
	src, err := p.Source.AddPredicate(ctx, expr)
	if err != nil {
		return nil, err
	}
	p.Source = src
	return p, nil
}

func (p *Projection) expressions() (result []sqlparser.Expr) {
	for _, col := range p.Columns {
		result = append(result, col.GetExpr())
	}
	return
}

func (p *Projection) GetColumns() ([]sqlparser.Expr, error) {
	return p.expressions(), nil
}

func (p *Projection) IPhysical() {}
