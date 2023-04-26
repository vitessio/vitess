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
	"fmt"
	"strings"

	"golang.org/x/exp/slices"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/semantics"

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

		// TableID will be non-nil for derived tables
		TableID *semantics.TableSet
		Alias   string
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
	colAsExpr := func(pe ProjExpr) sqlparser.Expr { return pe.GetExpr() }
	if offset, found := canReuseColumn(ctx, p.Columns, expr.Expr, colAsExpr); found {
		return p, offset, nil
	}
	sourceOp, offset, err := p.Source.AddColumn(ctx, expr)
	if err != nil {
		return nil, 0, err
	}
	p.Source = sourceOp
	p.Columns = append(p.Columns, Offset{Offset: offset, Expr: expr.Expr})
	p.ColumnNames = append(p.ColumnNames, expr.As.String())
	return p, len(p.Columns) - 1, nil
}

func (po Offset) GetExpr() sqlparser.Expr { return po.Expr }
func (po Eval) GetExpr() sqlparser.Expr   { return po.Expr }
func (po Expr) GetExpr() sqlparser.Expr   { return po.E }

func (p *Projection) Clone(inputs []ops.Operator) ops.Operator {
	return &Projection{
		Source:      inputs[0],
		ColumnNames: slices.Clone(p.ColumnNames),
		Columns:     slices.Clone(p.Columns),
		TableID:     p.TableID,
		Alias:       p.Alias,
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

func (p *Projection) expressions() (result []*sqlparser.AliasedExpr) {
	for i, col := range p.Columns {
		expr := col.GetExpr()
		result = append(result, &sqlparser.AliasedExpr{
			Expr: expr,
			As:   sqlparser.NewIdentifierCI(p.ColumnNames[i]),
		})
	}
	return
}

func (p *Projection) GetColumns() ([]*sqlparser.AliasedExpr, error) {
	if p.TableID != nil {
		return nil, nil
	}
	return p.expressions(), nil
}

func (p *Projection) GetOrdering() ([]ops.OrderBy, error) {
	return p.Source.GetOrdering()
}

// AllOffsets returns a slice of integer offsets for all columns in the Projection
// if all columns are of type Offset. If any column is not of type Offset, it returns nil.
func (p *Projection) AllOffsets() (cols []int) {
	for _, c := range p.Columns {
		offset, ok := c.(Offset)
		if !ok {
			return nil
		}

		cols = append(cols, offset.Offset)
	}
	return
}

func (p *Projection) Description() ops.OpDescription {
	var columns []string
	for i, col := range p.Columns {
		alias := p.ColumnNames[i]
		if alias == "" {
			columns = append(columns, sqlparser.String(col.GetExpr()))
		} else {
			columns = append(columns, fmt.Sprintf("%s AS %s", sqlparser.String(col.GetExpr()), alias))
		}
	}

	other := map[string]any{
		"OutputColumns": strings.Join(columns, ", "),
	}
	if p.TableID != nil {
		other["Derived"] = true
		other["Alias"] = p.Alias
	}
	return ops.OpDescription{
		OperatorType: "Projection",
		Other:        other,
	}
}

func (p *Projection) ShortDescription() string {
	var columns []string
	if p.Alias != "" {
		columns = append(columns, "derived["+p.Alias+"]")
	}
	for i, column := range p.Columns {
		expr := sqlparser.String(column.GetExpr())
		alias := p.ColumnNames[i]
		if alias == "" {
			columns = append(columns, expr)
			continue
		}
		columns = append(columns, fmt.Sprintf("%s AS %s", expr, alias))

	}
	return strings.Join(columns, ", ")
}

func (p *Projection) Compact(*plancontext.PlanningContext) (ops.Operator, rewrite.ApplyResult, error) {
	switch src := p.Source.(type) {
	case *Route:
		return p.compactWithRoute(src)
	case *ApplyJoin:
		return p.compactWithJoin(src)
	}
	return p, rewrite.SameTree, nil
}

func (p *Projection) compactWithJoin(src *ApplyJoin) (ops.Operator, rewrite.ApplyResult, error) {
	var newColumns []int
	var newColumnsAST []JoinColumn
	for _, col := range p.Columns {
		offset, ok := col.(Offset)
		if !ok {
			return p, rewrite.SameTree, nil
		}

		newColumns = append(newColumns, src.Columns[offset.Offset])
		newColumnsAST = append(newColumnsAST, src.ColumnsAST[offset.Offset])
	}

	src.Columns = newColumns
	src.ColumnsAST = newColumnsAST
	return src, rewrite.NewTree, nil
}

func (p *Projection) compactWithRoute(rb *Route) (ops.Operator, rewrite.ApplyResult, error) {
	for i, col := range p.Columns {
		offset, ok := col.(Offset)
		if !ok || offset.Offset != i {
			return p, rewrite.SameTree, nil
		}
	}
	columns, err := rb.GetColumns()
	if err != nil {
		return nil, false, err
	}

	if len(columns) == len(p.Columns) {
		return rb, rewrite.NewTree, nil
	}
	rb.ResultColumns = len(columns)
	return rb, rewrite.SameTree, nil
}
