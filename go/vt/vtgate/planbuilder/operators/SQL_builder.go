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
	"sort"
	"strings"

	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators/ops"

	"vitess.io/vitess/go/vt/log"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type (
	queryBuilder struct {
		ctx        *plancontext.PlanningContext
		sel        sqlparser.SelectStatement
		tableNames []string
	}
)

func ToSQL(ctx *plancontext.PlanningContext, op ops.Operator) (sqlparser.SelectStatement, error) {
	q := &queryBuilder{ctx: ctx}
	err := buildQuery(op, q)
	if err != nil {
		return nil, err
	}
	q.sortTables()
	return q.sel, nil
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
	if sel.Where == nil {
		sel.AddWhere(expr)
		return
	}
	for _, exp := range sqlparser.SplitAndExpression(nil, expr) {
		sel.AddWhere(exp)
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

func (h *Horizon) toSQL(qb *queryBuilder) error {
	err := stripDownQuery(h.Select, qb.sel)
	if err != nil {
		return err
	}
	sqlparser.Rewrite(qb.sel, func(cursor *sqlparser.Cursor) bool {
		if aliasedExpr, ok := cursor.Node().(sqlparser.SelectExpr); ok {
			removeKeyspaceFromSelectExpr(aliasedExpr)
		}
		return true
	}, nil)
	return nil
}

func removeKeyspaceFromSelectExpr(expr sqlparser.SelectExpr) {
	switch expr := expr.(type) {
	case *sqlparser.AliasedExpr:
		sqlparser.RemoveKeyspaceFromColName(expr.Expr)
	case *sqlparser.StarExpr:
		expr.TableName.Qualifier = sqlparser.NewIdentifierCS("")
	}
}

func stripDownQuery(from, to sqlparser.SelectStatement) error {
	var err error

	switch node := from.(type) {
	case *sqlparser.Select:
		toNode, ok := to.(*sqlparser.Select)
		if !ok {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "AST did not match")
		}
		toNode.Distinct = node.Distinct
		toNode.GroupBy = node.GroupBy
		toNode.Having = node.Having
		toNode.OrderBy = node.OrderBy
		toNode.Comments = node.Comments
		toNode.SelectExprs = node.SelectExprs
		for _, expr := range toNode.SelectExprs {
			removeKeyspaceFromSelectExpr(expr)
		}
	case *sqlparser.Union:
		toNode, ok := to.(*sqlparser.Union)
		if !ok {
			return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "AST did not match")
		}
		err = stripDownQuery(node.Left, toNode.Left)
		if err != nil {
			return err
		}
		err = stripDownQuery(node.Right, toNode.Right)
		if err != nil {
			return err
		}
		toNode.OrderBy = node.OrderBy
	default:
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "BUG: this should not happen - we have covered all implementations of SelectStatement %T", from)
	}
	return nil
}

func buildQuery(op ops.Operator, qb *queryBuilder) error {
	switch op := op.(type) {
	case *Table:
		dbName := ""

		if op.QTable.IsInfSchema {
			dbName = op.QTable.Table.Qualifier.String()
		}
		qb.addTable(dbName, op.QTable.Table.Name.String(), op.QTable.Alias.As.String(), TableID(op), op.QTable.Alias.Hints)
		for _, pred := range op.QTable.Predicates {
			qb.addPredicate(pred)
		}
		for _, name := range op.Columns {
			qb.addProjection(&sqlparser.AliasedExpr{Expr: name})
		}
	case *ApplyJoin:
		err := buildQuery(op.LHS, qb)
		if err != nil {
			return err
		}
		// If we are going to add the predicate used in join here
		// We should not add the predicate's copy of when it was split into
		// two parts. To avoid this, we use the SkipPredicates map.
		for _, expr := range qb.ctx.JoinPredicates[op.Predicate] {
			qb.ctx.SkipPredicates[expr] = nil
		}
		qbR := &queryBuilder{ctx: qb.ctx}
		err = buildQuery(op.RHS, qbR)
		if err != nil {
			return err
		}
		if op.LeftJoin {
			qb.joinOuterWith(qbR, op.Predicate)
		} else {
			qb.joinInnerWith(qbR, op.Predicate)
		}
	case *Filter:
		err := buildQuery(op.Source, qb)
		if err != nil {
			return err
		}
		for _, pred := range op.Predicates {
			qb.addPredicate(pred)
		}
	case *Derived:
		err := buildQuery(op.Source, qb)
		if err != nil {
			return err
		}
		sel := qb.sel.(*sqlparser.Select) // we can only handle SELECT in derived tables at the moment
		qb.sel = nil
		opQuery := sqlparser.RemoveKeyspace(op.Query).(*sqlparser.Select)
		sel.Limit = opQuery.Limit
		sel.OrderBy = opQuery.OrderBy
		sel.GroupBy = opQuery.GroupBy
		sel.Having = opQuery.Having
		sel.SelectExprs = opQuery.SelectExprs
		qb.addTableExpr(op.Alias, op.Alias, TableID(op), &sqlparser.DerivedTable{
			Select: sel,
		}, nil, op.ColumnAliases)
		for _, col := range op.Columns {
			qb.addProjection(&sqlparser.AliasedExpr{Expr: col})
		}
	case *Horizon:
		err := buildQuery(op.Source, qb)
		if err != nil {
			return err
		}

		err = stripDownQuery(op.Select, qb.sel)
		if err != nil {
			return err
		}
		sqlparser.Rewrite(qb.sel, func(cursor *sqlparser.Cursor) bool {
			if aliasedExpr, ok := cursor.Node().(sqlparser.SelectExpr); ok {
				removeKeyspaceFromSelectExpr(aliasedExpr)
			}
			return true
		}, nil)
		return nil

	default:
		return vterrors.Errorf(vtrpcpb.Code_INTERNAL, "don't know how to turn %T into SQL", op)
	}
	return nil
}
