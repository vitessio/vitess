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
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/operators"
	"vitess.io/vitess/go/vt/vtgate/planbuilder/plancontext"
)

type rewriter struct {
	err error
	ctx *plancontext.PlanningContext
}

func queryRewrite(ctx *plancontext.PlanningContext, statement sqlparser.Statement) error {
	r := rewriter{
		ctx: ctx,
	}
	sqlparser.Rewrite(statement, r.rewriteDown, nil)
	return nil
}

func (r *rewriter) rewriteDown(cursor *sqlparser.Cursor) bool {
	switch node := cursor.Node().(type) {
	case *sqlparser.Select:
		r.rewriteHavingClause(node)
	case *sqlparser.AliasedTableExpr:
		if _, isDerived := node.Expr.(*sqlparser.DerivedTable); isDerived {
			break
		}
		// find the tableSet and tableInfo that this table points to
		// tableInfo should contain the information for the original table that the routed table points to
		tableSet := r.ctx.SemTable.TableSetFor(node)
		tableInfo, err := r.ctx.SemTable.TableInfoFor(tableSet)
		if err != nil {
			// Fail-safe code, should never happen
			break
		}
		// vindexTable is the original table
		vindexTable := tableInfo.GetVindexTable()
		if vindexTable == nil {
			break
		}
		tableName := node.Expr.(sqlparser.TableName)
		// if the table name matches what the original is, then we do not need to rewrite
		if sqlparser.Equals.IdentifierCS(vindexTable.Name, tableName.Name) {
			break
		}
		// if there is no as clause, then move the routed table to the as clause.
		// i.e
		// routed as x -> original as x
		// routed -> original as routed
		if node.As.IsEmpty() {
			node.As = tableName.Name
		}
		// replace the table name with the original table
		tableName.Qualifier = sqlparser.IdentifierCS{}
		tableName.Name = vindexTable.Name
		node.Expr = tableName
	}
	return true
}

func (r *rewriter) rewriteHavingClause(node *sqlparser.Select) {
	if node.Having == nil {
		return
	}

	// for each expression in the having clause, we check if it contains aggregation.
	// if it does, we keep the expression in the having clause ; and if it does not
	// and the expression is in the select list, we replace the expression by the one
	// used in the select list and add it to the where clause instead of the having clause.
	exprs := sqlparser.SplitAndExpression(nil, node.Having.Expr)
	node.Having = nil
	for _, expr := range exprs {
		if operators.ContainsAggr(r.ctx, expr) {
			node.AddHaving(expr)
		} else {
			node.AddWhere(expr)
		}
	}
}
