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

	"vitess.io/vitess/go/vt/vtgate/semantics"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/abstract"
)

func toSQL(ctx *planningContext, op abstract.PhysicalOperator) sqlparser.SelectStatement {
	q := &queryBuilder{ctx: ctx}
	buildQuery(op, q)
	q.produce()
	return q.sel
}

func buildQuery(op abstract.PhysicalOperator, qb *queryBuilder) {
	switch op := op.(type) {
	case *tableOp:
		qb.addTable(op.qtable.Table.Name.String(), op.qtable.Alias.As.String(), op.TableID())
		for _, pred := range op.qtable.Predicates {
			qb.addPredicate(pred)
		}
		for _, name := range op.columns {
			qb.addProjection(&sqlparser.AliasedExpr{Expr: name})
		}
	case *applyJoin:
		buildQuery(op.LHS, qb)
		// If we are going to add the predicate used in join here
		// We should not add the predicate's copy of when it was split into
		// two parts. To avoid this, we use the skipPredicates map.
		for _, expr := range qb.ctx.joinPredicates[op.predicate] {
			qb.ctx.skipPredicates[expr] = nil
		}
		qbR := &queryBuilder{ctx: qb.ctx}
		buildQuery(op.RHS, qbR)
		if op.leftJoin {
			qb.joinOuterWith(qbR, op.predicate)
		} else {
			qb.joinInnerWith(qbR, op.predicate)
		}
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

func (qb *queryBuilder) produce() {
	sort.Sort(qb)
}

func (qb *queryBuilder) addTable(tableName, alias string, tableID semantics.TableSet) {
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
	qb.tableIDsInFrom = append(qb.tableIDsInFrom, tableID)
}

func (qb *queryBuilder) addPredicate(expr sqlparser.Expr) {
	if _, toBeSkipped := qb.ctx.skipPredicates[expr]; toBeSkipped {
		// This is a predicate that was added to the RHS of an applyJoin.
		// The original predicate will be added, so we don't have to add this here
		return
	}

	sel := qb.sel.(*sqlparser.Select)
	if sel.Where == nil {
		sel.Where = &sqlparser.Where{
			Type: sqlparser.WhereClause,
			Expr: expr,
		}
	} else {
		sel.Where.Expr = sqlparser.AndExpressions(sel.Where.Expr, expr)
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
	qb.tableIDsInFrom = append(qb.tableIDsInFrom, other.tableIDsInFrom...)
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
	tableSet := semantics.EmptyTableSet()
	for _, set := range qb.tableIDsInFrom {
		tableSet.MergeInPlace(set)
	}
	for _, set := range other.tableIDsInFrom {
		tableSet.MergeInPlace(set)
	}

	qb.tableIDsInFrom = []semantics.TableSet{tableSet}
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
	ctx            *planningContext
	sel            sqlparser.SelectStatement
	tableIDsInFrom []semantics.TableSet
	tableNames     []string
}

// Len implements the Sort interface
func (qb *queryBuilder) Len() int {
	return len(qb.tableIDsInFrom)
}

// Less implements the Sort interface
func (qb *queryBuilder) Less(i, j int) bool {
	return qb.tableIDsInFrom[i].TableOffset() < qb.tableIDsInFrom[j].TableOffset()
}

// Swap implements the Sort interface
func (qb *queryBuilder) Swap(i, j int) {
	sel, isSel := qb.sel.(*sqlparser.Select)
	if isSel {
		sel.From[i], sel.From[j] = sel.From[j], sel.From[i]
	}
	qb.tableIDsInFrom[i], qb.tableIDsInFrom[j] = qb.tableIDsInFrom[j], qb.tableIDsInFrom[i]
}
