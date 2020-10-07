/*
Copyright 2019 The Vitess Authors.

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

package sqlparser

import (
	"strings"

	"vitess.io/vitess/go/vt/sysvars"

	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

func shouldRewriteDatabaseFunc(in Statement) bool {
	selct, ok := in.(*Select)
	if !ok {
		return false
	}
	if len(selct.From) != 1 {
		return false
	}
	aliasedTable, ok := selct.From[0].(*AliasedTableExpr)
	if !ok {
		return false
	}
	tableName, ok := aliasedTable.Expr.(TableName)
	if !ok {
		return false
	}
	return tableName.Name.String() == "dual"
}

type expressionRewriter struct {
	bindVars                  *BindVarNeeds
	shouldRewriteDatabaseFunc bool
	err                       error
}

func newExpressionRewriter() *expressionRewriter {
	return &expressionRewriter{bindVars: &BindVarNeeds{}}
}

const (
	//LastInsertIDName is a reserved bind var name for last_insert_id()
	LastInsertIDName = "__lastInsertId"

	//DBVarName is a reserved bind var name for database()
	DBVarName = "__vtdbname"

	//FoundRowsName is a reserved bind var name for found_rows()
	FoundRowsName = "__vtfrows"

	//RowCountName is a reserved bind var name for row_count()
	RowCountName = "__vtrcount"

	//UserDefinedVariableName is what we prepend bind var names for user defined variables
	UserDefinedVariableName = "__vtudv"
)

func (er *expressionRewriter) rewriteAliasedExpr(node *AliasedExpr) (*BindVarNeeds, error) {
	inner := newExpressionRewriter()
	inner.shouldRewriteDatabaseFunc = er.shouldRewriteDatabaseFunc
	tmp := Rewrite(node.Expr, inner.rewrite, nil)
	newExpr, ok := tmp.(Expr)
	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "failed to rewrite AST. function expected to return Expr returned a %s", String(tmp))
	}
	node.Expr = newExpr
	return inner.bindVars, nil
}

func (er *expressionRewriter) rewrite(cursor *Cursor) bool {
	switch node := cursor.Node().(type) {
	// select last_insert_id() -> select :__lastInsertId as `last_insert_id()`
	case *Select:
		for _, col := range node.SelectExprs {
			aliasedExpr, ok := col.(*AliasedExpr)
			if ok && aliasedExpr.As.IsEmpty() {
				buf := NewTrackedBuffer(nil)
				aliasedExpr.Expr.Format(buf)
				innerBindVarNeeds, err := er.rewriteAliasedExpr(aliasedExpr)
				if err != nil {
					er.err = err
					return false
				}
				if innerBindVarNeeds.HasRewrites() {
					aliasedExpr.As = NewColIdent(buf.String())
				}
				er.bindVars.MergeWith(innerBindVarNeeds)
			}
		}
	case *FuncExpr:
		er.funcRewrite(cursor, node)
	case *ColName:
		switch node.Name.at {
		case SingleAt:
			er.udvRewrite(cursor, node)
		case DoubleAt:
			er.sysVarRewrite(cursor, node)
		}
	case *Subquery:
		er.unnestSubQueries(cursor, node)
	}
	return true
}

func (er *expressionRewriter) sysVarRewrite(cursor *Cursor, node *ColName) {
	lowered := node.Name.Lowered()
	switch lowered {
	case sysvars.Autocommit.Name,
		sysvars.ClientFoundRows.Name,
		sysvars.SkipQueryPlanCache.Name,
		sysvars.SQLSelectLimit.Name,
		sysvars.TransactionMode.Name,
		sysvars.Workload.Name:
		cursor.Replace(bindVarExpression("__vt" + lowered))
		er.bindVars.AddSysVar(lowered)
	}
}

func (er *expressionRewriter) udvRewrite(cursor *Cursor, node *ColName) {
	udv := strings.ToLower(node.Name.CompliantName())
	cursor.Replace(bindVarExpression(UserDefinedVariableName + udv))
	er.bindVars.AddUserDefVar(udv)
}

var funcRewrites = map[string]string{
	"last_insert_id": LastInsertIDName,
	"database":       DBVarName,
	"schema":         DBVarName,
	"found_rows":     FoundRowsName,
	"row_count":      RowCountName,
}

func (er *expressionRewriter) funcRewrite(cursor *Cursor, node *FuncExpr) {
	bindVar, found := funcRewrites[node.Name.Lowered()]
	if found {
		if bindVar == DBVarName && !er.shouldRewriteDatabaseFunc {
			return
		}
		if len(node.Exprs) > 0 {
			er.err = vterrors.Errorf(vtrpc.Code_UNIMPLEMENTED, "Argument to %s() not supported", node.Name.Lowered())
			return
		}
		cursor.Replace(bindVarExpression(bindVar))
		er.bindVars.AddFuncResult(bindVar)
	}
}

func (er *expressionRewriter) unnestSubQueries(cursor *Cursor, subquery *Subquery) {
	sel, isSimpleSelect := subquery.Select.(*Select)
	// Today, subqueries and derived tables use the same AST struct,
	// so we have to check what the parent is so we don't accidentally
	// rewrite a FROM clause instead of an expression
	_, isDerivedTable := cursor.Parent().(*AliasedTableExpr)

	if isDerivedTable || !isSimpleSelect {
		return
	}

	if !(len(sel.SelectExprs) != 1 ||
		len(sel.OrderBy) != 0 ||
		len(sel.GroupBy) != 0 ||
		len(sel.From) != 1 ||
		sel.Where == nil ||
		sel.Having == nil ||
		sel.Limit == nil) && sel.Lock == NoLock {
		return
	}
	aliasedTable, ok := sel.From[0].(*AliasedTableExpr)
	if !ok {
		return
	}
	table, ok := aliasedTable.Expr.(TableName)
	if !ok || table.Name.String() != "dual" {
		return
	}
	expr, ok := sel.SelectExprs[0].(*AliasedExpr)
	if !ok {
		return
	}
	er.bindVars.NoteRewrite()
	// we need to make sure that the inner expression also gets rewritten,
	// so we fire off another rewriter traversal here
	rewrittenExpr := Rewrite(expr.Expr, er.rewrite, nil)
	cursor.Replace(rewrittenExpr)
}

func bindVarExpression(name string) Expr {
	return NewArgument([]byte(":" + name))
}
