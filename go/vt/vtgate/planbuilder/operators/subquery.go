/*
Copyright 2021 The Vitess Authors.

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
	"maps"
	"slices"

	"vitess.io/vitess/go/slice"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

// SubQuery represents a subquery used for filtering rows in an
// outer query through a join.
type SubQuery struct {
	// Fields filled in at the time of construction:
	Outer             ops.Operator         // Outer query operator.
	Subquery          ops.Operator         // Subquery operator.
	FilterType        opcode.PulloutOpcode // Type of subquery filter.
	Original          sqlparser.Expr       // This is the expression we should use if we can merge the inner to the outer
	originalSubquery  *sqlparser.Subquery  // Subquery representation, e.g., (SELECT foo from user LIMIT 1).
	Predicates        sqlparser.Exprs      // Predicates joining outer and inner queries. Empty for uncorrelated subqueries.
	OuterPredicate    sqlparser.Expr       // This is the predicate that is using the subquery expression. It will not be empty for projections
	ArgName           string               // This is the name of the ColName or Argument used to replace the subquery
	TopLevel          bool                 // will be false if the subquery is deeply nested
	JoinColumns       []JoinColumn         // Broken up join predicates.
	SubqueryValueName string               // Value name returned by the subquery (uncorrelated queries).
	HasValuesName     string               // Argument name passed to the subquery (uncorrelated queries).

	// Fields related to correlated subqueries:
	Vars    map[string]int // Arguments copied from outer to inner, set during offset planning.
	outerID semantics.TableSet

	IsProjection bool
}

func (sq *SubQuery) planOffsets(ctx *plancontext.PlanningContext) error {
	sq.Vars = make(map[string]int)
	columns, err := sq.GetJoinColumns(ctx, sq.Outer)
	if err != nil {
		return err
	}
	for _, jc := range columns {
		for _, lhsExpr := range jc.LHSExprs {
			offset, err := sq.Outer.AddColumn(ctx, true, false, aeWrap(lhsExpr.Expr))
			if err != nil {
				return err
			}
			sq.Vars[lhsExpr.Name] = offset
		}
	}
	return nil
}

func (sq *SubQuery) OuterExpressionsNeeded(ctx *plancontext.PlanningContext, outer ops.Operator) (result []*sqlparser.ColName, err error) {
	joinColumns, err := sq.GetJoinColumns(ctx, outer)
	if err != nil {
		return nil, err
	}
	for _, jc := range joinColumns {
		for _, lhsExpr := range jc.LHSExprs {
			col, ok := lhsExpr.Expr.(*sqlparser.ColName)
			if !ok {
				return nil, vterrors.VT13001("joins can only compare columns: %s", sqlparser.String(lhsExpr.Expr))
			}
			result = append(result, col)
		}
	}
	return result, nil
}

func (sq *SubQuery) GetJoinColumns(ctx *plancontext.PlanningContext, outer ops.Operator) ([]JoinColumn, error) {
	if outer == nil {
		return nil, vterrors.VT13001("outer operator cannot be nil")
	}
	outerID := TableID(outer)
	if sq.JoinColumns != nil {
		if sq.outerID == outerID {
			return sq.JoinColumns, nil
		}
	}
	sq.outerID = outerID
	mapper := func(in sqlparser.Expr) (JoinColumn, error) {
		return BreakExpressionInLHSandRHS(ctx, in, outerID)
	}
	joinPredicates, err := slice.MapWithError(sq.Predicates, mapper)
	if err != nil {
		return nil, err
	}
	sq.JoinColumns = joinPredicates
	return sq.JoinColumns, nil
}

// Clone implements the Operator interface
func (sq *SubQuery) Clone(inputs []ops.Operator) ops.Operator {
	klone := *sq
	switch len(inputs) {
	case 1:
		klone.Subquery = inputs[0]
	case 2:
		klone.Outer = inputs[0]
		klone.Subquery = inputs[1]
	default:
		panic("wrong number of inputs")
	}
	klone.JoinColumns = slices.Clone(sq.JoinColumns)
	klone.Vars = maps.Clone(sq.Vars)
	klone.Predicates = sqlparser.CloneExprs(sq.Predicates)
	return &klone
}

func (sq *SubQuery) GetOrdering() ([]ops.OrderBy, error) {
	return sq.Outer.GetOrdering()
}

// Inputs implements the Operator interface
func (sq *SubQuery) Inputs() []ops.Operator {
	if sq.Outer == nil {
		return []ops.Operator{sq.Subquery}
	}

	return []ops.Operator{sq.Outer, sq.Subquery}
}

// SetInputs implements the Operator interface
func (sq *SubQuery) SetInputs(inputs []ops.Operator) {
	switch len(inputs) {
	case 1:
		sq.Subquery = inputs[0]
	case 2:
		sq.Outer = inputs[0]
		sq.Subquery = inputs[1]
	default:
		panic("wrong number of inputs")
	}
}

func (sq *SubQuery) ShortDescription() string {
	var typ string
	if sq.IsProjection {
		typ = "PROJ"
	} else {
		typ = "FILTER"
	}
	var pred string

	if len(sq.Predicates) > 0 || sq.OuterPredicate != nil {
		preds := append(sq.Predicates, sq.OuterPredicate)
		pred = " MERGE ON " + sqlparser.String(sqlparser.AndExpressions(preds...))
	}
	return fmt.Sprintf("%s %v%s", typ, sq.FilterType.String(), pred)
}

func (sq *SubQuery) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	newOuter, err := sq.Outer.AddPredicate(ctx, expr)
	if err != nil {
		return nil, err
	}
	sq.Outer = newOuter
	return sq, nil
}

func (sq *SubQuery) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, exprs *sqlparser.AliasedExpr) (int, error) {
	return sq.Outer.AddColumn(ctx, reuseExisting, addToGroupBy, exprs)
}

func (sq *SubQuery) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) (int, error) {
	return sq.Outer.FindCol(ctx, expr, underRoute)
}

func (sq *SubQuery) GetColumns(ctx *plancontext.PlanningContext) ([]*sqlparser.AliasedExpr, error) {
	return sq.Outer.GetColumns(ctx)
}

func (sq *SubQuery) GetSelectExprs(ctx *plancontext.PlanningContext) (sqlparser.SelectExprs, error) {
	return sq.Outer.GetSelectExprs(ctx)
}

// GetMergePredicates returns the predicates that we can use to try to merge this subquery with the outer query.
func (sq *SubQuery) GetMergePredicates() []sqlparser.Expr {
	if sq.OuterPredicate != nil {
		return append(sq.Predicates, sq.OuterPredicate)
	}
	return sq.Predicates
}

func (sq *SubQuery) settle(ctx *plancontext.PlanningContext, outer ops.Operator) (ops.Operator, error) {
	if !sq.TopLevel {
		return nil, subqueryNotAtTopErr
	}
	if sq.IsProjection {
		if len(sq.GetMergePredicates()) > 0 {
			// this means that we have a correlated subquery on our hands
			return nil, correlatedSubqueryErr
		}
		sq.SubqueryValueName = sq.ArgName
		return outer, nil
	}
	return sq.settleFilter(ctx, outer)
}

var correlatedSubqueryErr = vterrors.VT12001("correlated subquery is only supported for EXISTS")
var subqueryNotAtTopErr = vterrors.VT12001("unmergable subquery can not be inside complex expression")

func (sq *SubQuery) settleFilter(ctx *plancontext.PlanningContext, outer ops.Operator) (ops.Operator, error) {
	if len(sq.Predicates) > 0 {
		if sq.FilterType != opcode.PulloutExists {
			return nil, correlatedSubqueryErr
		}
		return outer, nil
	}

	hasValuesArg := func() string {
		s := ctx.ReservedVars.ReserveVariable(string(sqlparser.HasValueSubQueryBaseName))
		sq.HasValuesName = s
		return s
	}
	post := func(cursor *sqlparser.CopyOnWriteCursor) {
		node := cursor.Node()
		if _, ok := node.(*sqlparser.Subquery); !ok {
			return
		}

		var arg sqlparser.Expr
		if sq.FilterType.NeedsListArg() {
			arg = sqlparser.NewListArg(sq.ArgName)
		} else {
			arg = sqlparser.NewArgument(sq.ArgName)
		}
		cursor.Replace(arg)
	}
	rhsPred := sqlparser.CopyOnRewrite(sq.Original, dontEnterSubqueries, post, ctx.SemTable.CopySemanticInfo).(sqlparser.Expr)

	var predicates []sqlparser.Expr
	switch sq.FilterType {
	case opcode.PulloutExists:
		predicates = append(predicates, sqlparser.NewArgument(hasValuesArg()))
	case opcode.PulloutNotExists:
		sq.FilterType = opcode.PulloutExists // it's the same pullout as EXISTS, just with a NOT in front of the predicate
		predicates = append(predicates, sqlparser.NewNotExpr(sqlparser.NewArgument(hasValuesArg())))
	case opcode.PulloutIn:
		predicates = append(predicates, sqlparser.NewArgument(hasValuesArg()), rhsPred)
		sq.SubqueryValueName = sq.ArgName
	case opcode.PulloutNotIn:
		predicates = append(predicates, &sqlparser.OrExpr{
			Left:  sqlparser.NewNotExpr(sqlparser.NewArgument(hasValuesArg())),
			Right: rhsPred,
		})
		sq.SubqueryValueName = sq.ArgName
	case opcode.PulloutValue:
		predicates = append(predicates, rhsPred)
		sq.SubqueryValueName = sq.ArgName
	}
	return &Filter{
		Source:     outer,
		Predicates: predicates,
	}, nil
}

func dontEnterSubqueries(node, _ sqlparser.SQLNode) bool {
	if _, ok := node.(*sqlparser.Subquery); ok {
		return false
	}
	return true
}

func (sq *SubQuery) isMerged(ctx *plancontext.PlanningContext) bool {
	return slices.Index(ctx.MergedSubqueries, sq.originalSubquery) >= 0
}

// mapExpr rewrites all expressions according to the provided function
func (sq *SubQuery) mapExpr(f func(expr sqlparser.Expr) (sqlparser.Expr, error)) error {
	newPredicates, err := slice.MapWithError(sq.Predicates, f)
	if err != nil {
		return err
	}
	sq.Predicates = newPredicates

	sq.Original, err = f(sq.Original)
	if err != nil {
		return err
	}

	originalSubquery, err := f(sq.originalSubquery)
	if err != nil {
		return err
	}
	sq.originalSubquery = originalSubquery.(*sqlparser.Subquery)
	return nil
}
