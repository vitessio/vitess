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

package operators

import (
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
	Original          sqlparser.Expr       // Original comparison or EXISTS expression.
	_sq               *sqlparser.Subquery  // Subquery representation, e.g., (SELECT foo from user LIMIT 1).
	Predicates        sqlparser.Exprs      // Predicates joining outer and inner queries. Empty for uncorrelated subqueries.
	OuterPredicate    sqlparser.Expr       // This is the predicate that is using the subquery expression. It will be empty for projections
	ReplacedSqColName *sqlparser.ColName

	// Fields filled in at the subquery settling phase:
	JoinColumns       []JoinColumn         // Broken up join predicates.
	LHSColumns        []*sqlparser.ColName // Left hand side columns of join predicates.
	SubqueryValueName string               // Value name returned by the subquery (uncorrelated queries).
	HasValuesName     string               // Argument name passed to the subquery (uncorrelated queries).

	// Fields related to correlated subqueries:
	Vars    map[string]int // Arguments copied from outer to inner, set during offset planning.
	outerID semantics.TableSet
}

func (sj *SubQuery) IsProjection() bool {
	return sj.ReplacedSqColName != nil
}

func (sj *SubQuery) planOffsets(ctx *plancontext.PlanningContext) error {
	sj.Vars = make(map[string]int)
	for _, jc := range sj.JoinColumns {
		for i, lhsExpr := range jc.LHSExprs {
			offset, err := sj.Outer.AddColumn(ctx, true, false, aeWrap(lhsExpr))
			if err != nil {
				return err
			}
			sj.Vars[jc.BvNames[i]] = offset
		}
	}
	return nil
}

func (sj *SubQuery) OuterExpressionsNeeded(ctx *plancontext.PlanningContext, outer ops.Operator) ([]*sqlparser.ColName, error) {
	joinColumns, err := sj.GetJoinColumns(ctx, outer)
	if err != nil {
		return nil, err
	}
	for _, jc := range joinColumns {
		for _, lhsExpr := range jc.LHSExprs {
			col, ok := lhsExpr.(*sqlparser.ColName)
			if !ok {
				return nil, vterrors.VT13001("joins can only compare columns: %s", sqlparser.String(lhsExpr))
			}
			sj.LHSColumns = append(sj.LHSColumns, col)
		}
	}
	return sj.LHSColumns, nil
}

func (sj *SubQuery) GetJoinColumns(ctx *plancontext.PlanningContext, outer ops.Operator) ([]JoinColumn, error) {
	if outer == nil {
		return nil, vterrors.VT13001("outer operator cannot be nil")
	}
	outerID := TableID(outer)
	if sj.JoinColumns != nil {
		if sj.outerID == outerID {
			return sj.JoinColumns, nil
		}
	}
	sj.outerID = outerID
	mapper := func(in sqlparser.Expr) (JoinColumn, error) {
		return BreakExpressionInLHSandRHS(ctx, in, outerID)
	}
	joinPredicates, err := slice.MapWithError(sj.Predicates, mapper)
	if err != nil {
		return nil, err
	}
	sj.JoinColumns = joinPredicates
	return sj.JoinColumns, nil
}

// Clone implements the Operator interface
func (sj *SubQuery) Clone(inputs []ops.Operator) ops.Operator {
	klone := *sj
	switch len(inputs) {
	case 1:
		klone.Subquery = inputs[0]
	case 2:
		klone.Outer = inputs[0]
		klone.Subquery = inputs[1]
	default:
		panic("wrong number of inputs")
	}
	klone.JoinColumns = slices.Clone(sj.JoinColumns)
	klone.LHSColumns = slices.Clone(sj.LHSColumns)
	klone.Vars = maps.Clone(sj.Vars)
	klone.Predicates = sqlparser.CloneExprs(sj.Predicates)
	return &klone
}

func (sj *SubQuery) GetOrdering() ([]ops.OrderBy, error) {
	return sj.Outer.GetOrdering()
}

// Inputs implements the Operator interface
func (sj *SubQuery) Inputs() []ops.Operator {
	if sj.Outer == nil {
		return []ops.Operator{sj.Subquery}
	}

	return []ops.Operator{sj.Outer, sj.Subquery}
}

// SetInputs implements the Operator interface
func (sj *SubQuery) SetInputs(inputs []ops.Operator) {
	switch len(inputs) {
	case 1:
		sj.Subquery = inputs[0]
	case 2:
		sj.Outer = inputs[0]
		sj.Subquery = inputs[1]
	default:
		panic("wrong number of inputs")
	}
}

func (sj *SubQuery) ShortDescription() string {
	return sj.FilterType.String() + " WHERE " + sqlparser.String(sj.Predicates)
}

func (sj *SubQuery) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	newOuter, err := sj.Outer.AddPredicate(ctx, expr)
	if err != nil {
		return nil, err
	}
	sj.Outer = newOuter
	return sj, nil
}

func (sj *SubQuery) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, exprs *sqlparser.AliasedExpr) (int, error) {
	return sj.Outer.AddColumn(ctx, reuseExisting, addToGroupBy, exprs)
}

func (sj *SubQuery) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) (int, error) {
	return sj.Outer.FindCol(ctx, expr, underRoute)
}

func (sj *SubQuery) GetColumns(ctx *plancontext.PlanningContext) ([]*sqlparser.AliasedExpr, error) {
	return sj.Outer.GetColumns(ctx)
}

func (sj *SubQuery) GetSelectExprs(ctx *plancontext.PlanningContext) (sqlparser.SelectExprs, error) {
	return sj.Outer.GetSelectExprs(ctx)
}

// GetMergePredicates returns the predicates that we can use to try to merge this subquery with the outer query.
func (sj *SubQuery) GetMergePredicates() []sqlparser.Expr {
	if sj.OuterPredicate != nil {
		return append(sj.Predicates, sj.OuterPredicate)
	}
	return sj.Predicates
}

func (sj *SubQuery) settle(ctx *plancontext.PlanningContext, outer ops.Operator) (ops.Operator, error) {
	if sj.IsProjection() {
		sj.SubqueryValueName = sj.ReplacedSqColName.Name.String()
		return outer, nil
	}
	return sj.settleFilter(ctx, outer)
}

func (sj *SubQuery) settleFilter(ctx *plancontext.PlanningContext, outer ops.Operator) (ops.Operator, error) {
	if len(sj.Predicates) > 0 {
		if sj.FilterType != opcode.PulloutExists {
			return nil, vterrors.VT12001("correlated subquery is only supported for EXISTS")
		}
		return sj.settleExistSubquery(ctx, outer)
	}

	resultArg, hasValuesArg := ctx.ReservedVars.ReserveSubQueryWithHasValues()
	dontEnterSubqueries := func(node, _ sqlparser.SQLNode) bool {
		if _, ok := node.(*sqlparser.Subquery); ok {
			return false
		}
		return true
	}
	post := func(cursor *sqlparser.CopyOnWriteCursor) {
		node := cursor.Node()
		if _, ok := node.(*sqlparser.Subquery); !ok {
			return
		}

		var arg sqlparser.Expr
		if sj.FilterType == opcode.PulloutIn || sj.FilterType == opcode.PulloutNotIn {
			arg = sqlparser.NewListArg(resultArg)
		} else {
			arg = sqlparser.NewArgument(resultArg)
		}
		cursor.Replace(arg)
	}
	rhsPred := sqlparser.CopyOnRewrite(sj.Original, dontEnterSubqueries, post, ctx.SemTable.CopyDependenciesOnSQLNodes).(sqlparser.Expr)

	var predicates []sqlparser.Expr
	switch sj.FilterType {
	case opcode.PulloutExists:
		predicates = append(predicates, sqlparser.NewArgument(hasValuesArg))
		sj.HasValuesName = hasValuesArg
	case opcode.PulloutNotExists:
		sj.FilterType = opcode.PulloutExists // it's the same pullout as EXISTS, just with a NOT in front of the predicate
		predicates = append(predicates, sqlparser.NewNotExpr(sqlparser.NewArgument(hasValuesArg)))
		sj.HasValuesName = hasValuesArg
	case opcode.PulloutIn:
		predicates = append(predicates, sqlparser.NewArgument(hasValuesArg), rhsPred)
		sj.HasValuesName = hasValuesArg
		sj.SubqueryValueName = resultArg
	case opcode.PulloutNotIn:
		predicates = append(predicates, sqlparser.NewNotExpr(sqlparser.NewArgument(hasValuesArg)), rhsPred)
		sj.HasValuesName = hasValuesArg
		sj.SubqueryValueName = resultArg
	case opcode.PulloutValue:
		predicates = append(predicates, rhsPred)
		sj.SubqueryValueName = resultArg
	}
	return &Filter{
		Source:     outer,
		Predicates: predicates,
	}, nil
}

func (sj *SubQuery) settleExistSubquery(ctx *plancontext.PlanningContext, outer ops.Operator) (ops.Operator, error) {
	jcs, err := sj.GetJoinColumns(ctx, outer)
	if err != nil {
		return nil, err
	}

	sj.Subquery = &Filter{
		Source:     sj.Subquery,
		Predicates: slice.Map(jcs, func(col JoinColumn) sqlparser.Expr { return col.RHSExpr }),
	}

	// the columns needed by the RHS expression are handled during offset planning time

	return outer, nil
}
