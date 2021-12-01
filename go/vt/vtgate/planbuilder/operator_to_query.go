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
	"fmt"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
)

func toSQL(ctx *planningContext, op abstract.PhysicalOperator) sqlparser.SelectStatement {
	q := &queryBuilder{ctx: ctx}
	buildQuery(op, q)
	return q.produce()
}

func buildQuery(op abstract.PhysicalOperator, qb *queryBuilder) {
	switch op := op.(type) {
	case *tableOp:
		qb.addTable(op.qtable.Table.Name.String(), op.qtable.Alias.As.String())
		for _, pred := range op.qtable.Predicates {
			qb.addPredicate(pred)
		}
	case *applyJoin:
		buildQuery(op.LHS, qb)
		qbR := &queryBuilder{ctx: qb.ctx}
		buildQuery(op.RHS, qbR)
		qb.joinWith(qbR, op.predicate)
	case *filterOp:
		buildQuery(op.source, qb)
		for _, pred := range op.predicates {
			qb.addPredicate(pred)
		}

	// case *project:
	// 	buildQuery(op.src, qb)
	// 	qb.project(op.projections)
	// case *vectorAggr:
	// 	buildQuery(op.src, qb)
	// 	qb.vectorGroupBy(op.groupBy, op.aggrF)
	default:
		panic(fmt.Sprintf("%T", op))
	}
}

func (qb *queryBuilder) produce() sqlparser.SelectStatement {
	query := qb.sel
	if sel, ok := query.(*sqlparser.Select); ok && len(sel.SelectExprs) == 0 {
		sel.SelectExprs = append(sel.SelectExprs, &sqlparser.StarExpr{})
	}
	return query
}

func (qb *queryBuilder) addTable(tableName, alias string) {
	if qb.sel == nil {
		qb.sel = &sqlparser.Select{}
	}
	sel := qb.sel.(*sqlparser.Select)
	sel.From = append(sel.From, &sqlparser.AliasedTableExpr{
		Expr:       sqlparser.TableName{Name: sqlparser.NewTableIdent(tableName)},
		Partitions: nil,
		As:         sqlparser.NewTableIdent(alias),
		Hints:      nil,
		Columns:    nil,
	})
	qb.sel = sel
	qb.tableNames = append(qb.tableNames, tableName)
}

func (qb *queryBuilder) addPredicate(expr sqlparser.Expr) {
	if _, isJoinCond := qb.ctx.joinPredicates[expr]; isJoinCond {
		// This is a predicate that was added to the RHS of an applyJoin.
		// The original predicate will be added, so we don't have to add this here
		return
	}

	sel := qb.sel.(*sqlparser.Select)
	if qb.grouped {
		if sel.Having == nil {
			sel.Having = &sqlparser.Where{
				Type: sqlparser.HavingClause,
				Expr: expr,
			}
		} else {
			sel.Having.Expr = sqlparser.AndExpressions(sel.Having.Expr, expr)
		}
	} else if sel.Where == nil {
		sel.Where = &sqlparser.Where{
			Type: sqlparser.WhereClause,
			Expr: expr,
		}
	} else {
		sel.Where.Expr = sqlparser.AndExpressions(sel.Where.Expr, expr)
	}
}

func (qb *queryBuilder) vectorGroupBy(grouping []*sqlparser.AliasedExpr, aggrFuncs []*sqlparser.AliasedExpr) {
	sel := qb.sel.(*sqlparser.Select)
	var exprs []sqlparser.SelectExpr
	var groupBy sqlparser.GroupBy
	for _, expr := range aggrFuncs {
		exprs = append(exprs, expr)
	}
	for _, expr := range grouping {
		exprs = append(exprs, expr)
		groupBy = append(groupBy, expr.Expr)
	}
	sel.SelectExprs = append(sel.SelectExprs, exprs...)
	sel.GroupBy = groupBy
	qb.grouped = true
}

func (qb *queryBuilder) project(projections []*sqlparser.AliasedExpr) {
	sel := qb.sel.(*sqlparser.Select)
	var exprs []sqlparser.SelectExpr
	for _, expr := range projections {
		exprs = append(exprs, expr)
	}
	sel.SelectExprs = append(sel.SelectExprs, exprs...)
}

func (qb *queryBuilder) joinWith(other *queryBuilder, onCondition sqlparser.Expr) {
	if other.grouped {
		dtName := other.convertToDerivedTable()
		other.rewriteExprForDerivedTable(onCondition, dtName)
	}

	if qb.grouped {
		dtName := qb.convertToDerivedTable()
		qb.rewriteExprForDerivedTable(onCondition, dtName)
	}

	// neither of the inputs needs to be put into a derived table. we can just merge them together
	sel := qb.sel.(*sqlparser.Select)
	otherSel := other.sel.(*sqlparser.Select)
	sel.From = append(sel.From, otherSel.From...)
	sel.SelectExprs = append(sel.SelectExprs, otherSel.SelectExprs...)

	var predicate sqlparser.Expr
	if sel.Where != nil {
		predicate = sel.Where.Expr
	}
	if otherSel.Where != nil {
		predicate = sqlparser.AndExpressions(predicate, otherSel.Where.Expr)
	}
	if predicate != nil {
		sel.Where = &sqlparser.Where{Type: sqlparser.WhereClause, Expr: predicate}
	}

	qb.addPredicate(onCondition)
}

func (qb *queryBuilder) rewriteExprForDerivedTable(expr sqlparser.Expr, dtName string) {
	sqlparser.Rewrite(expr, func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *sqlparser.ColName:
			hasTable := qb.hasTable(node.Qualifier.Name.String())
			if hasTable {
				node.Qualifier = sqlparser.TableName{
					Name: sqlparser.NewTableIdent(dtName),
				}
			}
		}
		return true
	}, nil)
}

func (qb *queryBuilder) hasTable(tableName string) bool {
	for _, name := range qb.tableNames {
		if strings.EqualFold(tableName, name) {
			return true
		}
	}
	return false
}

func (qb *queryBuilder) convertToDerivedTable() string {
	sel := &sqlparser.Select{
		From: []sqlparser.TableExpr{
			&sqlparser.AliasedTableExpr{
				Expr: &sqlparser.DerivedTable{
					Select: qb.sel,
				},
				// TODO: use number generators for dt1
				As: sqlparser.NewTableIdent("dt1"),
				// TODO: also alias the column names to avoid collision with other tables
			},
		},
		SelectExprs: sqlparser.SelectExprs{&sqlparser.StarExpr{}},
	}
	qb.sel = sel

	return "dt1"
}

type queryBuilder struct {
	ctx        *planningContext
	sel        sqlparser.SelectStatement
	grouped    bool
	tableNames []string
}
