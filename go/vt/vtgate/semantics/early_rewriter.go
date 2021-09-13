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

package semantics

import (
	"strconv"

	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
)

type earlyRewriter struct {
	err      error
	semTable *SemTable
	scoper   *scoper
	clause   string
}

// earlyRewrite rewrites the query before the binder has had a chance to work on the query
// it introduces new expressions that the binder will later need to bind correctly
func earlyRewrite(statement sqlparser.SelectStatement, semTable *SemTable, scoper *scoper) error {
	r := earlyRewriter{
		semTable: semTable,
		scoper:   scoper,
	}
	sqlparser.Rewrite(statement, r.rewrite, nil)
	return r.err
}

func (r *earlyRewriter) rewrite(cursor *sqlparser.Cursor) bool {
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
	case *sqlparser.Order:
		r.clause = "order clause"
	case sqlparser.GroupBy:
		r.clause = "group statement"

	case *sqlparser.Literal:
		currScope, found := r.scoper.specialExprScopes[node]
		if !found {
			break
		}
		num, err := strconv.Atoi(node.Val)
		if err != nil {
			r.err = vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "error parsing column number: %s", node.Val)
			break
		}
		if num < 1 || num > len(currScope.selectStmt.SelectExprs) {
			r.err = vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.BadFieldError, "Unknown column '%d' in '%s'", num, r.clause)
			break
		}

		for i := 0; i < num; i++ {
			expr := currScope.selectStmt.SelectExprs[i]
			_, ok := expr.(*sqlparser.AliasedExpr)
			if !ok {
				r.err = vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "cannot use column offsets in %s when using `%s`", r.clause, sqlparser.String(expr))
				return true
			}
		}

		aliasedExpr, ok := currScope.selectStmt.SelectExprs[num-1].(*sqlparser.AliasedExpr)
		if !ok {
			r.err = vterrors.Errorf(vtrpcpb.Code_INTERNAL, "don't know how to handle %s", sqlparser.String(node))
			break
		}

		if !aliasedExpr.As.IsEmpty() {
			cursor.Replace(sqlparser.NewColName(aliasedExpr.As.String()))
		} else {
			cursor.Replace(aliasedExpr.Expr)
		}
	}
	return true
}

func expandTableColumns(tables []TableInfo, starExpr *sqlparser.StarExpr) (bool, sqlparser.SelectExprs, error) {
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
