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
	"vitess.io/vitess/go/vt/vtgate/engine"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

type rewriter struct {
	err          error
	semTable     *semantics.SemTable
	reservedVars *sqlparser.ReservedVars
	isInSubquery int
}

func starRewrite(statement sqlparser.SelectStatement, semTable *semantics.SemTable) error {
	r := rewriter{
		semTable: semTable,
	}
	sqlparser.Rewrite(statement, r.starRewrite, nil)
	return r.err
}

func (r *rewriter) starRewrite(cursor *sqlparser.Cursor) bool {
	switch node := cursor.Node().(type) {
	case *sqlparser.Select:
		tables := r.semTable.GetSelectTables(node)
		var selExprs sqlparser.SelectExprs
		for _, selectExpr := range node.SelectExprs {
			starExpr, isStarExpr := selectExpr.(*sqlparser.StarExpr)
			if !isStarExpr {
				selExprs = append(selExprs, selectExpr)
				continue
			}
			starExpanded, colNames, err := expandTableColumns(tables, starExpr)
			if err != nil {
				r.err = err
				return false
			}
			if !starExpanded {
				selExprs = append(selExprs, selectExpr)
				continue
			}
			selExprs = append(selExprs, colNames...)
		}
		node.SelectExprs = selExprs
	}
	return true
}

func expandTableColumns(tables []semantics.TableInfo, starExpr *sqlparser.StarExpr) (bool, sqlparser.SelectExprs, error) {
	unknownTbl := true
	var colNames sqlparser.SelectExprs
	starExpanded := true
	for _, tbl := range tables {
		if !starExpr.TableName.IsEmpty() && !tbl.Matches(starExpr.TableName) {
			continue
		}
		unknownTbl = false
		if !tbl.Authoritative() {
			starExpanded = false
			break
		}
		tblName, err := tbl.Name()
		if err != nil {
			return false, nil, err
		}

		withAlias := len(tables) > 1
		withQualifier := withAlias || !tbl.GetExpr().As.IsEmpty()
		for _, col := range tbl.GetColumns() {
			var colName *sqlparser.ColName
			var alias sqlparser.ColIdent
			if withQualifier {
				colName = sqlparser.NewColNameWithQualifier(col.Name, tblName)
			} else {
				colName = sqlparser.NewColName(col.Name)
			}
			if withAlias {
				alias = sqlparser.NewColIdent(col.Name)
			}
			colNames = append(colNames, &sqlparser.AliasedExpr{Expr: colName, As: alias})
		}
	}

	if unknownTbl {
		// This will only happen for case when starExpr has qualifier.
		return false, nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.BadDb, "Unknown table '%s'", sqlparser.String(starExpr.TableName))
	}
	return starExpanded, colNames, nil
}

func queryRewrite(ctx planningContext, statement sqlparser.SelectStatement) error {
	r := rewriter{
		semTable:     ctx.semTable,
		reservedVars: ctx.reservedVars,
	}
	sqlparser.Rewrite(statement, r.rewriteDown, r.rewriteUp)
	return nil
}

func (r *rewriter) rewriteDown(cursor *sqlparser.Cursor) bool {
	switch node := cursor.Node().(type) {
	case *sqlparser.Select:
		rewriteHavingClause(node)
	case *sqlparser.ExistsExpr:
		return r.rewriteExistsSubquery(cursor, node)
	case *sqlparser.Subquery:
		r.isInSubquery++
		rewriteSubquery(cursor, r, node)
	case *sqlparser.AliasedTableExpr:
		// rewrite names of the routed tables for the subquery
		// We only need to do this for non-derived tables and if they are in a subquery
		if _, isDerived := node.Expr.(*sqlparser.DerivedTable); isDerived || r.isInSubquery == 0 {
			break
		}
		// find the tableSet and tableInfo that this table points to
		// tableInfo should contain the information for the original table that the routed table points to
		tableSet := r.semTable.TableSetFor(node)
		tableInfo, err := r.semTable.TableInfoFor(tableSet)
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
		if sqlparser.EqualsTableIdent(vindexTable.Name, tableName.Name) {
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
		tableName.Name = vindexTable.Name
		node.Expr = tableName
	}
	return true
}

func (r *rewriter) rewriteUp(cursor *sqlparser.Cursor) bool {
	switch cursor.Node().(type) {
	case *sqlparser.Subquery:
		r.isInSubquery--
	}
	return true
}

func rewriteSubquery(cursor *sqlparser.Cursor, r *rewriter, node *sqlparser.Subquery) {
	semTableSQ, found := r.semTable.SubqueryRef[node]
	if !found {
		// should never happen
		return
	}

	argName := r.reservedVars.ReserveSubQuery()
	semTableSQ.ArgName = argName

	switch semTableSQ.OpCode {
	case engine.PulloutIn, engine.PulloutNotIn:
		cursor.Replace(sqlparser.NewListArg(argName))
	default:
		cursor.Replace(sqlparser.NewArgument(argName))
	}
}

func (r *rewriter) rewriteExistsSubquery(cursor *sqlparser.Cursor, node *sqlparser.ExistsExpr) bool {
	semTableSQ, found := r.semTable.SubqueryRef[node.Subquery]
	if !found {
		// should never happen
		return false
	}

	argName := r.reservedVars.ReserveHasValuesSubQuery()
	semTableSQ.ArgName = argName

	cursor.Replace(sqlparser.NewArgument(argName))
	return false
}

func rewriteHavingClause(node *sqlparser.Select) {
	if node.Having == nil {
		return
	}

	selectExprMap := map[string]sqlparser.Expr{}
	for _, selectExpr := range node.SelectExprs {
		aliasedExpr, isAliased := selectExpr.(*sqlparser.AliasedExpr)
		if !isAliased || aliasedExpr.As.IsEmpty() {
			continue
		}
		selectExprMap[aliasedExpr.As.Lowered()] = aliasedExpr.Expr
	}

	sqlparser.Rewrite(node.Having.Expr, func(cursor *sqlparser.Cursor) bool {
		switch x := cursor.Node().(type) {
		case *sqlparser.ColName:
			if !x.Qualifier.IsEmpty() {
				return false
			}
			originalExpr, isInMap := selectExprMap[x.Name.Lowered()]
			if isInMap {
				cursor.Replace(originalExpr)
				return false
			}
			return false
		}
		return true
	}, nil)

	exprs := sqlparser.SplitAndExpression(nil, node.Having.Expr)
	node.Having = nil
	for _, expr := range exprs {
		if sqlparser.ContainsAggregation(expr) {
			node.AddHaving(expr)
		} else {
			node.AddWhere(expr)
		}
	}
}
