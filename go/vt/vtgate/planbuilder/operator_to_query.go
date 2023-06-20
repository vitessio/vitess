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
	"sort"
	"strings"

	"vitess.io/vitess/go/vt/log"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/physical"

	"vitess.io/vitess/go/vt/vtgate/semantics"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
)

type queryBuilder struct {
	ctx        *plancontext.PlanningContext
	sel        sqlparser.SelectStatement
	tableNames []string
}

func toSQL(ctx *plancontext.PlanningContext, op abstract.PhysicalOperator) sqlparser.SelectStatement {
	q := &queryBuilder{ctx: ctx}
	buildQuery(op, q)
	q.sortTables()
	return q.sel
}

func buildQuery(op abstract.PhysicalOperator, qb *queryBuilder) {
	switch op := op.(type) {
	case *physical.Table:
		dbName := ""

		if op.QTable.IsInfSchema {
			dbName = op.QTable.Table.Qualifier.String()
		}
		qb.addTable(dbName, op.QTable.Table.Name.String(), op.QTable.Alias.As.String(), op.TableID(), op.QTable.Alias.Hints)
		for _, pred := range op.QTable.Predicates {
			qb.addPredicate(pred)
		}
		for _, name := range op.Columns {
			qb.addProjection(&sqlparser.AliasedExpr{Expr: name})
		}
	case *physical.ApplyJoin:
		buildQuery(op.LHS, qb)
		// If we are going to add the predicate used in join here
		// We should not add the predicate's copy of when it was split into
		// two parts. To avoid this, we use the SkipPredicates map.
		for _, expr := range qb.ctx.JoinPredicates[op.Predicate] {
			qb.ctx.SkipPredicates[expr] = nil
		}
		qbR := &queryBuilder{ctx: qb.ctx}
		buildQuery(op.RHS, qbR)
		if op.LeftJoin {
			qb.joinOuterWith(qbR, op.Predicate)
		} else {
			qb.joinInnerWith(qbR, op.Predicate)
		}
	case *physical.Filter:
		buildQuery(op.Source, qb)
		for _, pred := range op.Predicates {
			qb.addPredicate(pred)
		}
	case *physical.Derived:
		buildQuery(op.Source, qb)
		sel := qb.sel.(*sqlparser.Select) // we can only handle SELECT in derived tables at the moment
		qb.sel = nil
		opQuery := sqlparser.RemoveKeyspace(op.Query).(*sqlparser.Select)
		sel.Limit = opQuery.Limit
		sel.OrderBy = opQuery.OrderBy
		sel.GroupBy = opQuery.GroupBy
		sel.Having = mergeHaving(sel.Having, opQuery.Having)
		sel.SelectExprs = opQuery.SelectExprs
		qb.addTableExpr(op.Alias, op.Alias, op.TableID(), &sqlparser.DerivedTable{
			Select: sel,
		}, nil, op.ColumnAliases)
		for _, col := range op.Columns {
			qb.addProjection(&sqlparser.AliasedExpr{Expr: col})
		}
	default:
		panic(fmt.Sprintf("%T", op))
	}
}

func (qb *queryBuilder) sortTables() {
	_ = sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		sel, isSel := node.(*sqlparser.Select)
		if !isSel {
			return true, nil
		}
		ts := &tableSorter{
			sel: sel,
			tbl: qb.ctx.SemTable,
		}
		sort.Sort(ts)
		return true, nil
	}, qb.sel)

}

func (qb *queryBuilder) addTable(db, tableName, alias string, tableID semantics.TableSet, hints sqlparser.IndexHints) {
	tableExpr := sqlparser.TableName{
		Name:      sqlparser.NewIdentifierCS(tableName),
		Qualifier: sqlparser.NewIdentifierCS(db),
	}
	qb.addTableExpr(tableName, alias, tableID, tableExpr, hints, nil)
}

func (qb *queryBuilder) addTableExpr(
	tableName, alias string,
	tableID semantics.TableSet,
	tblExpr sqlparser.SimpleTableExpr,
	hints sqlparser.IndexHints,
	columnAliases sqlparser.Columns,
) {
	if qb.sel == nil {
		qb.sel = &sqlparser.Select{}
	}
	sel := qb.sel.(*sqlparser.Select)
	elems := &sqlparser.AliasedTableExpr{
		Expr:       tblExpr,
		Partitions: nil,
		As:         sqlparser.NewIdentifierCS(alias),
		Hints:      hints,
		Columns:    columnAliases,
	}
	err := qb.ctx.SemTable.ReplaceTableSetFor(tableID, elems)
	if err != nil {
		log.Warningf("error in replacing table expression in semtable: %v", err)
	}
	sel.From = append(sel.From, elems)
	qb.sel = sel
	qb.tableNames = append(qb.tableNames, tableName)
}

func (qb *queryBuilder) addPredicate(expr sqlparser.Expr) {
	if _, toBeSkipped := qb.ctx.SkipPredicates[expr]; toBeSkipped {
		// This is a predicate that was added to the RHS of an ApplyJoin.
		// The original predicate will be added, so we don't have to add this here
		return
	}

	sel := qb.sel.(*sqlparser.Select)
	_, isSubQuery := expr.(*sqlparser.ExtractedSubquery)
	var addPred func(sqlparser.Expr)

	if sqlparser.ContainsAggregation(expr) && !isSubQuery {
		addPred = sel.AddHaving
	} else {
		addPred = sel.AddWhere
	}
	for _, exp := range sqlparser.SplitAndExpression(nil, expr) {
		addPred(exp)
	}
}

func (qb *queryBuilder) addProjection(projection *sqlparser.AliasedExpr) {
	sel := qb.sel.(*sqlparser.Select)
	sel.SelectExprs = append(sel.SelectExprs, projection)
}

func (qb *queryBuilder) joinInnerWith(other *queryBuilder, onCondition sqlparser.Expr) {
	sel := qb.sel.(*sqlparser.Select)
	otherSel := other.sel.(*sqlparser.Select)
	sel.From = append(sel.From, otherSel.From...)
	sel.SelectExprs = append(sel.SelectExprs, otherSel.SelectExprs...)

	var predicate sqlparser.Expr
	if sel.Where != nil {
		predicate = sel.Where.Expr
	}
	if otherSel.Where != nil {
		predExprs := sqlparser.SplitAndExpression(nil, predicate)
		otherExprs := sqlparser.SplitAndExpression(nil, otherSel.Where.Expr)
		predicate = sqlparser.AndExpressions(append(predExprs, otherExprs...)...)
	}
	if predicate != nil {
		sel.Where = &sqlparser.Where{Type: sqlparser.WhereClause, Expr: predicate}
	}

	qb.addPredicate(onCondition)
}

func (qb *queryBuilder) joinOuterWith(other *queryBuilder, onCondition sqlparser.Expr) {
	sel := qb.sel.(*sqlparser.Select)
	otherSel := other.sel.(*sqlparser.Select)
	var lhs sqlparser.TableExpr
	if len(sel.From) == 1 {
		lhs = sel.From[0]
	} else {
		lhs = &sqlparser.ParenTableExpr{Exprs: sel.From}
	}
	var rhs sqlparser.TableExpr
	if len(otherSel.From) == 1 {
		rhs = otherSel.From[0]
	} else {
		rhs = &sqlparser.ParenTableExpr{Exprs: otherSel.From}
	}
	sel.From = []sqlparser.TableExpr{&sqlparser.JoinTableExpr{
		LeftExpr:  lhs,
		RightExpr: rhs,
		Join:      sqlparser.LeftJoinType,
		Condition: &sqlparser.JoinCondition{
			On: onCondition,
		},
	}}

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
}

func (qb *queryBuilder) rewriteExprForDerivedTable(expr sqlparser.Expr, dtName string) {
	sqlparser.Rewrite(expr, func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *sqlparser.ColName:
			hasTable := qb.hasTable(node.Qualifier.Name.String())
			if hasTable {
				node.Qualifier = sqlparser.TableName{
					Name: sqlparser.NewIdentifierCS(dtName),
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

type tableSorter struct {
	sel *sqlparser.Select
	tbl *semantics.SemTable
}

// Len implements the Sort interface
func (ts *tableSorter) Len() int {
	return len(ts.sel.From)
}

// Less implements the Sort interface
func (ts *tableSorter) Less(i, j int) bool {
	lhs := ts.sel.From[i]
	rhs := ts.sel.From[j]
	left, ok := lhs.(*sqlparser.AliasedTableExpr)
	if !ok {
		return i < j
	}
	right, ok := rhs.(*sqlparser.AliasedTableExpr)
	if !ok {
		return i < j
	}

	return ts.tbl.TableSetFor(left).TableOffset() < ts.tbl.TableSetFor(right).TableOffset()
}

// Swap implements the Sort interface
func (ts *tableSorter) Swap(i, j int) {
	ts.sel.From[i], ts.sel.From[j] = ts.sel.From[j], ts.sel.From[i]
}

func mergeHaving(h1, h2 *sqlparser.Where) *sqlparser.Where {
	switch {
	case h1 == nil && h2 == nil:
		return nil
	case h1 == nil:
		return h2
	case h2 == nil:
		return h1
	default:
		h1.Expr = sqlparser.AndExpressions(h1.Expr, h2.Expr)
		return h1
	}
}
