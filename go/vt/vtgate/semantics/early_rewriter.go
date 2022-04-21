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
	scoper  *scoper
	clause  string
	warning string
}

func (r *earlyRewriter) down(cursor *sqlparser.Cursor) error {
	switch node := cursor.Node().(type) {
	case sqlparser.SelectExprs:
		_, isSel := cursor.Parent().(*sqlparser.Select)
		if !isSel {
			return nil
		}
		tables := r.scoper.currentScope().tables
		var selExprs sqlparser.SelectExprs
		changed := false
		for _, selectExpr := range node {
			starExpr, isStarExpr := selectExpr.(*sqlparser.StarExpr)
			if !isStarExpr {
				selExprs = append(selExprs, selectExpr)
				continue
			}
			starExpanded, colNames, err := expandTableColumns(tables, starExpr)
			if err != nil {
				return err
			}
			if !starExpanded || colNames == nil {
				selExprs = append(selExprs, selectExpr)
				continue
			}
			selExprs = append(selExprs, colNames...)
			changed = true
		}
		if changed {
			cursor.ReplaceAndRevisit(selExprs)
		}
	case *sqlparser.JoinTableExpr:
		if node.Join == sqlparser.StraightJoinType {
			node.Join = sqlparser.NormalJoinType
			r.warning = "straight join is converted to normal join"
		}
	case *sqlparser.Order:
		r.clause = "order clause"
	case sqlparser.GroupBy:
		r.clause = "group statement"

	case *sqlparser.Literal:
		newNode, err := r.rewriteOrderByExpr(node)
		if err != nil {
			return err
		}
		if newNode != nil {
			cursor.Replace(newNode)
		}
	case *sqlparser.CollateExpr:
		lit, ok := node.Expr.(*sqlparser.Literal)
		if !ok {
			return nil
		}
		newNode, err := r.rewriteOrderByExpr(lit)
		if err != nil {
			return err
		}
		if newNode != nil {
			node.Expr = newNode
		}
	}
	return nil
}

func (r *earlyRewriter) rewriteOrderByExpr(node *sqlparser.Literal) (sqlparser.Expr, error) {
	currScope, found := r.scoper.specialExprScopes[node]
	if !found {
		return nil, nil
	}
	num, err := strconv.Atoi(node.Val)
	if err != nil {
		return nil, vterrors.Errorf(vtrpcpb.Code_INVALID_ARGUMENT, "error parsing column number: %s", node.Val)
	}
	stmt, isSel := currScope.stmt.(*sqlparser.Select)
	if !isSel {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "error invalid statement type, expect Select, got: %T", currScope.stmt)
	}

	if num < 1 || num > len(stmt.SelectExprs) {
		return nil, vterrors.NewErrorf(vtrpcpb.Code_INVALID_ARGUMENT, vterrors.BadFieldError, "Unknown column '%d' in '%s'", num, r.clause)
	}

	for i := 0; i < num; i++ {
		expr := stmt.SelectExprs[i]
		_, ok := expr.(*sqlparser.AliasedExpr)
		if !ok {
			return nil, vterrors.Errorf(vtrpcpb.Code_UNIMPLEMENTED, "cannot use column offsets in %s when using `%s`", r.clause, sqlparser.String(expr))
		}
	}

	aliasedExpr, ok := stmt.SelectExprs[num-1].(*sqlparser.AliasedExpr)
	if !ok {
		return nil, vterrors.Errorf(vtrpcpb.Code_INTERNAL, "don't know how to handle %s", sqlparser.String(node))
	}

	if !aliasedExpr.As.IsEmpty() {
		return sqlparser.NewColName(aliasedExpr.As.String()), nil
	}

	expr := realCloneOfColNames(aliasedExpr.Expr, currScope.isUnion)
	return expr, nil
}

// realCloneOfColNames clones all the expressions including ColName.
// Since sqlparser.CloneRefOfColName does not clone col names, this method is needed.
func realCloneOfColNames(expr sqlparser.Expr, union bool) sqlparser.Expr {
	return sqlparser.Rewrite(sqlparser.CloneExpr(expr), func(cursor *sqlparser.Cursor) bool {
		switch exp := cursor.Node().(type) {
		case *sqlparser.ColName:
			newColName := *exp
			if union {
				newColName.Qualifier = sqlparser.TableName{}
			}
			cursor.Replace(&newColName)
		}
		return true
	}, nil).(sqlparser.Expr)
}

func expandTableColumns(tables []TableInfo, starExpr *sqlparser.StarExpr) (bool, sqlparser.SelectExprs, error) {
	unknownTbl := true
	var colNames sqlparser.SelectExprs
	starExpanded := true
	for _, tbl := range tables {
		if !starExpr.TableName.IsEmpty() && !tbl.matches(starExpr.TableName) {
			continue
		}
		unknownTbl = false
		if !tbl.authoritative() {
			starExpanded = false
			break
		}
		tblName, err := tbl.Name()
		if err != nil {
			return false, nil, err
		}

		withAlias := len(tables) > 1
		withQualifier := withAlias || !tbl.getExpr().As.IsEmpty()
		for _, col := range tbl.getColumns() {
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
