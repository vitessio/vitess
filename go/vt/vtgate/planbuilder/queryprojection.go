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
	/*
		select id1, id3, count(id2) as iddd from... group by id3, id1 order by id1, id3
		select id1, id3, count(id2) as iddd from... group by id3, id1 order by id1, id1

		exprs : id1, id3
		aggr : count(id2)
		grouping : id3, id1
		ordering: id1, id3
	*/

	selectExprs             []*sqlparser.AliasedExpr
	aggrExprs               []*sqlparser.AliasedExpr
	groupOrderingCommonExpr map[sqlparser.Expr]*sqlparser.Order
	//groupExprs  sqlparser.GroupBy
	//orderExprs  sqlparser.OrderBy
}

func newQueryProjection() *queryProjection {
	return &queryProjection{
		groupOrderingCommonExpr: map[sqlparser.Expr]*sqlparser.Order{},
	}
}

func createQPFromSelect(sel *sqlparser.Select) (*queryProjection, error) {
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

	if sel.GroupBy == nil || sel.OrderBy == nil {
		return qp, nil
	}
	for _, exp := range sel.GroupBy {
		for _, order := range sel.OrderBy {
			if sqlparser.EqualsExpr(exp, order.Expr) {
				qp.groupOrderingCommonExpr[exp] = order
			}
		}
	}

	return qp, nil
}
