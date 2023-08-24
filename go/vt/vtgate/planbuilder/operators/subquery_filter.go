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

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// SubQueryFilter represents a subquery used for filtering rows in an
// outer query through a join.
type SubQueryFilter struct {
	// Fields filled in at the time of construction:
	Outer      ops.Operator         // Outer query operator.
	Subquery   ops.Operator         // Subquery operator.
	FilterType opcode.PulloutOpcode // Type of subquery filter.
	Original   sqlparser.Expr       // Original comparison or EXISTS expression.
	_sq        *sqlparser.Subquery  // Subquery representation, e.g., (SELECT foo from user LIMIT 1).
	Predicates sqlparser.Exprs      // Predicates joining outer and inner queries. Empty for uncorrelated subqueries.

	// Fields filled in at the subquery settling phase:
	JoinPredicates    []JoinColumn         // Broken up join predicates.
	LHSColumns        []*sqlparser.ColName // Left hand side columns of join predicates.
	SubqueryValueName string               // Value name returned by the subquery (uncorrelated queries).
	HasValuesName     string               // Argument name passed to the subquery (uncorrelated queries).

	// Fields related to correlated subqueries:
	Vars map[string]int // Arguments copied from outer to inner, set during offset planning.
}

func (sj *SubQueryFilter) planOffsets(ctx *plancontext.PlanningContext) error {
	sj.Vars = make(map[string]int)
	for _, jc := range sj.JoinPredicates {
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

func (sj *SubQueryFilter) SetOuter(operator ops.Operator) {
	sj.Outer = operator
}

func (sj *SubQueryFilter) OuterExpressionsNeeded() []*sqlparser.ColName {
	return sj.LHSColumns
}

var _ SubQuery = (*SubQueryFilter)(nil)

func (sj *SubQueryFilter) Inner() ops.Operator {
	return sj.Subquery
}

func (sj *SubQueryFilter) OriginalExpression() sqlparser.Expr {
	return sj.Original
}

func (sj *SubQueryFilter) SetOriginal(expr sqlparser.Expr) {
	sj.Original = expr
}

func (sj *SubQueryFilter) sq() *sqlparser.Subquery {
	return sj._sq
}

// Clone implements the Operator interface
func (sj *SubQueryFilter) Clone(inputs []ops.Operator) ops.Operator {
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
	klone.JoinPredicates = slices.Clone(sj.JoinPredicates)
	klone.LHSColumns = slices.Clone(sj.LHSColumns)
	klone.Vars = maps.Clone(sj.Vars)
	klone.Predicates = sqlparser.CloneExprs(sj.Predicates)
	return &klone
}

func (sj *SubQueryFilter) GetOrdering() ([]ops.OrderBy, error) {
	return sj.Outer.GetOrdering()
}

// Inputs implements the Operator interface
func (sj *SubQueryFilter) Inputs() []ops.Operator {
	if sj.Outer == nil {
		return []ops.Operator{sj.Subquery}
	}

	return []ops.Operator{sj.Outer, sj.Subquery}
}

// SetInputs implements the Operator interface
func (sj *SubQueryFilter) SetInputs(inputs []ops.Operator) {
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

func (sj *SubQueryFilter) ShortDescription() string {
	return sj.FilterType.String() + " WHERE " + sqlparser.String(sj.Predicates)
}

func (sj *SubQueryFilter) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	newOuter, err := sj.Outer.AddPredicate(ctx, expr)
	if err != nil {
		return nil, err
	}
	sj.Outer = newOuter
	return sj, nil
}

func (sj *SubQueryFilter) AddColumn(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy bool, exprs *sqlparser.AliasedExpr) (int, error) {
	return sj.Outer.AddColumn(ctx, reuseExisting, addToGroupBy, exprs)
}

func (sj *SubQueryFilter) FindCol(ctx *plancontext.PlanningContext, expr sqlparser.Expr, underRoute bool) (int, error) {
	return sj.Outer.FindCol(ctx, expr, underRoute)
}

func (sj *SubQueryFilter) GetColumns(ctx *plancontext.PlanningContext) ([]*sqlparser.AliasedExpr, error) {
	return sj.Outer.GetColumns(ctx)
}

func (sj *SubQueryFilter) GetSelectExprs(ctx *plancontext.PlanningContext) (sqlparser.SelectExprs, error) {
	return sj.Outer.GetSelectExprs(ctx)
}

func (sj *SubQueryFilter) GetJoinPredicates() []sqlparser.Expr {
	return sj.Predicates
}

func (sj *SubQueryFilter) ReplaceJoinPredicates(predicates sqlparser.Exprs) {
	sj.Predicates = predicates
}
