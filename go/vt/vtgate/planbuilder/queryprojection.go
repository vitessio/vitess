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

package planbuilder

import (
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type queryProjection struct {
	selectExprs []*sqlparser.AliasedExpr
	aggrExprs   []*sqlparser.AliasedExpr
	//groupOrderingCommonExpr map[sqlparser.Expr]*sqlparser.Order

	orderExprs sqlparser.OrderBy

	// orderExprColMap keeps a map between the Order object and the offset into the select expressions list
	orderExprColMap map[*sqlparser.Order]int
}

func newQueryProjection() *queryProjection {
	return &queryProjection{
		//groupOrderingCommonExpr: map[sqlparser.Expr]*sqlparser.Order{},
		orderExprColMap: map[*sqlparser.Order]int{},
	}
}

func createQPFromSelect(sel *sqlparser.Select, semTable *semantics.SemTable) (*queryProjection, error) {
	qp := newQueryProjection()

	for _, selExp := range sel.SelectExprs {
		exp, ok := selExp.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, semantics.Gen4NotSupportedF("%T in select list", selExp)
		}
		fExpr, ok := exp.Expr.(*sqlparser.FuncExpr)
		if ok && fExpr.IsAggregate() {
			if len(fExpr.Exprs) != 1 {
				return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.SyntaxError, "multiple arguments inside the function '%s'", sqlparser.String(fExpr))
			}
			qp.aggrExprs = append(qp.aggrExprs, exp)
			continue
		}
		if nodeHasAggregates(exp.Expr) {
			return nil, vterrors.New(vtrpcpb.Code_UNIMPLEMENTED, "unsupported: in scatter query: complex aggregate expression")
		}
		qp.selectExprs = append(qp.selectExprs, exp)
	}

	qp.orderExprs = sel.OrderBy

	allExpr := append(qp.selectExprs, qp.aggrExprs...)
	for _, order := range sel.OrderBy {
		for offset, expr := range allExpr {
			if sqlparser.EqualsExpr(order.Expr, expr.Expr) {
				qp.orderExprColMap[order] = offset
				break
			}
			// TODO: handle alias and column offset
		}
	}

	return qp, nil
}

func (qp *queryProjection) isDistinct(semTable *semantics.SemTable) (bool, error) {
	// figure out if we are dealing with a qp that is inherently distinct

	for _, aliasedExpr := range qp.selectExprs {
		col, ok := aliasedExpr.Expr.(*sqlparser.ColName)
		if !ok {
			continue
		}
		tableInfo, err := semTable.TableInfoForCol(col)
		if err != nil {
			return false, err
		}
		for _, vindex := range tableInfo.Table.ColumnVindexes {
			if len(vindex.Columns) == 1 &&
				vindex.Columns[0].EqualString(col.Name.String()) &&
				vindex.Vindex.IsUnique() {
				// we know the results will be unique
				return true, nil
			}
		}
	}

	// if we got here, we might still get distinct results in the end, but we can't guarantee it
	return false, nil
}
