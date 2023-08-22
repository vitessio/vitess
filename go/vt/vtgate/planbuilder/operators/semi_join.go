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
	"golang.org/x/exp/maps"

	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine/opcode"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

// SubQueryFilter represents a subquery used for filtering rows in an outer query through a join.
// The positioning of the outer query and subquery (left or right) depends on their correlation.
type SubQueryFilter struct {
	Outer      ops.Operator         // Operator of the outer query.
	Subquery   ops.Operator         // Operator of the subquery.
	FilterType opcode.PulloutOpcode // Type of the subquery filter.
	Original   sqlparser.Expr       // Original expression (comparison or EXISTS).

	// comparisonColumns are columns from the LHS and RHS used in the semi join.
	// Columns are included only if they are simple ColNames.
	// E.g., for the predicate `tbl.id IN (SELECT bar(foo) from user WHERE tbl.id = user.id)`,
	// `tbl.id` would be stored in JoinVars but not expressions like `foo(tbl.id)`.
	comparisonColumns [][2]*sqlparser.ColName

	_sq *sqlparser.Subquery // Represents a subquery like (SELECT foo from user LIMIT 1).

	// Join-related fields:
	// - JoinVars: Columns from the LHS used for the join (also found in Vars field).
	// - JoinVarOffsets: Arguments copied from outer to inner, set during offset planning.
	// For correlated subqueries, correlations might be in JoinVars, JoinVarOffsets, and comparisonColumns.
	JoinVars       map[string]*sqlparser.ColName
	JoinVarOffsets map[string]int

	// For uncorrelated queries:
	// - SubqueryValueName: Name of the value returned by the subquery.
	// - HasValuesName: Name of the argument passed to the subquery.
	SubqueryValueName string
	HasValuesName     string

	corrSubPredicate sqlparser.Expr // Expression pushed to RHS if subquery merge fails.
}

func (sj *SubQueryFilter) planOffsets(ctx *plancontext.PlanningContext) error {
	sj.JoinVarOffsets = make(map[string]int, len(sj.JoinVars))
	for bindvarName, col := range sj.JoinVars {
		offsets, err := sj.Outer.AddColumns(ctx, true, []bool{false}, []*sqlparser.AliasedExpr{aeWrap(col)})
		if err != nil {
			return err
		}
		sj.JoinVarOffsets[bindvarName] = offsets[0]
	}
	return nil
}

func (sj *SubQueryFilter) SetOuter(operator ops.Operator) {
	sj.Outer = operator
}

func (sj *SubQueryFilter) OuterExpressionsNeeded() []*sqlparser.ColName {
	return maps.Values(sj.JoinVars)
}

var _ SubQuery = (*SubQueryFilter)(nil)

func (sj *SubQueryFilter) Inner() ops.Operator {
	return sj.Subquery
}

func (sj *SubQueryFilter) OriginalExpression() sqlparser.Expr {
	return sj.Original
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
	klone.JoinVars = maps.Clone(sj.JoinVars)
	klone.JoinVarOffsets = maps.Clone(sj.JoinVarOffsets)
	return &klone
}

func (sj *SubQueryFilter) GetOrdering() ([]ops.OrderBy, error) {
	return nil, nil
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
	return ""
}

func (sj *SubQueryFilter) AddPredicate(ctx *plancontext.PlanningContext, expr sqlparser.Expr) (ops.Operator, error) {
	return nil, vterrors.VT13001("cannot add predicate to SubQueryFilter")
}

func (sj *SubQueryFilter) AddColumns(ctx *plancontext.PlanningContext, reuseExisting bool, addToGroupBy []bool, exprs []*sqlparser.AliasedExpr) ([]int, error) {
	return sj.Outer.AddColumns(ctx, reuseExisting, addToGroupBy, exprs)
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
