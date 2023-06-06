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

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/evalengine"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/rewrite"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	// Projection is used when we need to evaluate expressions on the vtgate
	// It uses the evalengine to accomplish its goal
	Projection struct {
		Source ops.Operator

		// Columns contain the expressions as viewed from the outside of this operator
		Columns []*sqlparser.AliasedExpr

		// Projections will contain the actual evaluations we need to
		// do if this operator is still above a route after optimisation
		Projections []ProjExpr

		// TableID will be non-nil for derived tables
		TableID *semantics.TableSet
		Alias   string

		FromAggr bool
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

	// UnexploredExpression is used before we have planned - one of two end results are possible for it
	// - we are able to push this projection under a route, and then this is not used at all - we'll just
	//   use the ColumnNames field of the Projection struct
	// - we have to evaluate this on the vtgate, and either it's just a copy from the input,
	//   or it's an evalengine expression that we have to evaluate
	UnexploredExpression struct {
		E sqlparser.Expr
	}
)

var _ selectExpressions = (*Projection)(nil)

func (p *Projection) addUnexploredExpr(ae *sqlparser.AliasedExpr, e sqlparser.Expr) int {
	p.Projections = append(p.Projections, UnexploredExpression{E: e})
	p.Columns = append(p.Columns, ae)
	return len(p.Projections) - 1
}

func (p *Projection) addColumnWithoutPushing(expr *sqlparser.AliasedExpr, _ bool) int {
	return p.addUnexploredExpr(expr, expr.Expr)
}

func (p *Projection) isDerived() bool {
	return p.TableID != nil
}

func (p *Projection) AddColumn(ctx *plancontext.PlanningContext, expr *sqlparser.AliasedExpr, _, addToGroupBy bool) (ops.Operator, int, error) {
	if offset, found := canReuseColumn(ctx, p.Columns, expr.Expr, extractExpr); found {
		return p, offset, nil
	}
	sourceOp, offset, err := p.Source.AddColumn(ctx, expr, true, addToGroupBy)
	if err != nil {
		return nil, 0, err
	}
	p.Source = sourceOp
	p.Projections = append(p.Projections, Offset{Offset: offset, Expr: expr.Expr})
	p.Columns = append(p.Columns, expr)
	return p, len(p.Projections) - 1, nil
}

func (po Offset) GetExpr() sqlparser.Expr               { return po.Expr }
func (po Eval) GetExpr() sqlparser.Expr                 { return po.Expr }
func (po UnexploredExpression) GetExpr() sqlparser.Expr { return po.E }

func (p *Projection) Clone(inputs []ops.Operator) ops.Operator {
	return &Projection{
		Source:      inputs[0],
		Columns:     slices.Clone(p.Columns),
		Projections: slices.Clone(p.Projections),
		TableID:     p.TableID,
		Alias:       p.Alias,
		FromAggr:    p.FromAggr,
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

func (p *Projection) GetColumns() ([]*sqlparser.AliasedExpr, error) {
	if p.TableID != nil {
		return nil, nil
	}
	return p.Columns, nil
}

func (p *Projection) GetOrdering() ([]ops.OrderBy, error) {
	return p.Source.GetOrdering()
}

// AllOffsets returns a slice of integer offsets for all columns in the Projection
// if all columns are of type Offset. If any column is not of type Offset, it returns nil.
func (p *Projection) AllOffsets() (cols []int) {
	for _, c := range p.Projections {
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
	for i, col := range p.Projections {
		aliasExpr := p.Columns[i]
		if aliasExpr.Expr == col.GetExpr() {
			columns = append(columns, sqlparser.String(aliasExpr))
		} else {
			columns = append(columns, fmt.Sprintf("%s AS %s", sqlparser.String(col.GetExpr()), aliasExpr.ColumnName()))
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
	for i, col := range p.Projections {
		aliasExpr := p.Columns[i]
		if aliasExpr.Expr == col.GetExpr() {
			columns = append(columns, sqlparser.String(aliasExpr))
		} else {
			columns = append(columns, fmt.Sprintf("%s AS %s", sqlparser.String(col.GetExpr()), aliasExpr.ColumnName()))
		}
	}
	return strings.Join(columns, ", ")
}

func (p *Projection) Compact(*plancontext.PlanningContext) (ops.Operator, *rewrite.ApplyResult, error) {
	switch src := p.Source.(type) {
	case *Route:
		return p.compactWithRoute(src)
	case *ApplyJoin:
		return p.compactWithJoin(src)
	}
	return p, rewrite.SameTree, nil
}

func (p *Projection) compactWithJoin(src *ApplyJoin) (ops.Operator, *rewrite.ApplyResult, error) {
	var newColumns []int
	var newColumnsAST []JoinColumn
	for _, col := range p.Projections {
		offset, ok := col.(Offset)
		if !ok {
			return p, rewrite.SameTree, nil
		}

		newColumns = append(newColumns, src.Columns[offset.Offset])
		newColumnsAST = append(newColumnsAST, src.ColumnsAST[offset.Offset])
	}

	src.Columns = newColumns
	src.ColumnsAST = newColumnsAST
	return src, rewrite.NewTree("remove projection from before join", src), nil
}

func (p *Projection) compactWithRoute(rb *Route) (ops.Operator, *rewrite.ApplyResult, error) {
	for i, col := range p.Projections {
		offset, ok := col.(Offset)
		if !ok || offset.Offset != i {
			return p, rewrite.SameTree, nil
		}
	}
	columns, err := rb.GetColumns()
	if err != nil {
		return nil, nil, err
	}

	if len(columns) == len(p.Projections) {
		return rb, rewrite.NewTree("remove projection from before route", rb), nil
	}
	rb.ResultColumns = len(columns)
	return rb, rewrite.SameTree, nil
}

func stopAtAggregations(node, _ sqlparser.SQLNode) bool {
	_, aggr := node.(sqlparser.AggrFunc)
	b := !aggr
	return b
}

func (p *Projection) needsEvaluation(ctx *plancontext.PlanningContext, e sqlparser.Expr) bool {
	offset := slices.IndexFunc(p.Columns, func(expr *sqlparser.AliasedExpr) bool {
		return ctx.SemTable.EqualsExprWithDeps(expr.Expr, e)
	})

	if offset < 0 {
		return false
	}

	inside := p.Projections[offset].GetExpr()
	outside := p.Columns[offset].Expr
	return inside != outside
}

func (p *Projection) planOffsets(ctx *plancontext.PlanningContext) error {
	for i, col := range p.Projections {
		_, unexplored := col.(UnexploredExpression)
		if !unexplored {
			continue
		}

		// first step is to replace the expressions we expect to get from our input with the offsets for these
		visitor, errCheck := offsetter(ctx,
			func() ops.Operator { return p.Source },
			func(o ops.Operator) { p.Source = o },
		)
		rewritten := sqlparser.CopyOnRewrite(col.GetExpr(), stopAtAggregations, visitor, nil).(sqlparser.Expr)
		if err := errCheck(); err != nil {
			return err
		}

		offset, ok := rewritten.(*sqlparser.Offset)
		if ok {
			// we got a pure offset back. No need to do anything else
			p.Projections[i] = Offset{
				Expr:   col.GetExpr(),
				Offset: offset.V,
			}
			continue
		}

		// for everything else, we'll turn to the evalengine
		eexpr, err := evalengine.Translate(rewritten, nil)
		if err != nil {
			return err
		}

		p.Projections[i] = Eval{
			Expr:  rewritten,
			EExpr: eexpr,
		}
	}

	return nil
}

func offsetter(ctx *plancontext.PlanningContext, src func() ops.Operator, setSource func(ops.Operator)) (func(cursor *sqlparser.CopyOnWriteCursor), func() error) {
	var err error
	return func(cursor *sqlparser.CopyOnWriteCursor) {
			expr, ok := cursor.Node().(sqlparser.Expr)
			if !ok || !fetchByOffset(expr) {
				return
			}

			newSrc, offset, terr := src().AddColumn(ctx, aeWrap(expr), true, false)
			if terr != nil {
				err = terr
				return
			}
			setSource(newSrc)
			cursor.Replace(sqlparser.NewOffset(offset, expr))

		}, func() error {
			return err
		}
}
