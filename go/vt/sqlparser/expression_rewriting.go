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

	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// PrepareAST will normalize the query
func PrepareAST(in Statement, bindVars map[string]*querypb.BindVariable, prefix string, parameterize bool) (*RewriteASTResult, error) {
	if parameterize {
		Normalize(in, bindVars, prefix)
	}
	return RewriteAST(in)
}

// RewriteAST rewrites the whole AST, replacing function calls and adding column aliases to queries
func RewriteAST(in Statement) (*RewriteASTResult, error) {
	er := newExpressionRewriter()
	er.shouldRewriteDatabaseFunc = shouldRewriteDatabaseFunc(in)
	setRewriter := &setNormalizer{}
	out, ok := Rewrite(in, er.goingDown, setRewriter.rewriteSetComingUp).(Statement)
	if !ok {
		return nil, vterrors.Errorf(vtrpc.Code_INTERNAL, "statement rewriting returned a non statement: %s", String(out))
	}
	if setRewriter.err != nil {
		return nil, setRewriter.err
	}

	r := &RewriteASTResult{
		AST:          out,
		BindVarNeeds: er.bindVars,
	}
	return r, nil
}

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

// RewriteASTResult contains the rewritten ast and meta information about it
type RewriteASTResult struct {
	*BindVarNeeds
	AST Statement // The rewritten AST
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

func (er *expressionRewriter) goingDown(cursor *Cursor) bool {
	switch node := cursor.Node().(type) {
	// select last_insert_id() -> select :__lastInsertId as `last_insert_id()`
	case *Select:
		for _, col := range node.SelectExprs {
			aliasedExpr, ok := col.(*AliasedExpr)
			if ok && aliasedExpr.As.IsEmpty() {
				buf := NewTrackedBuffer(nil)
				aliasedExpr.Expr.Format(buf)
				inner := newExpressionRewriter()
				inner.shouldRewriteDatabaseFunc = er.shouldRewriteDatabaseFunc
				tmp := Rewrite(aliasedExpr.Expr, inner.goingDown, nil)
				newExpr, ok := tmp.(Expr)
				if !ok {
					log.Errorf("failed to rewrite AST. function expected to return Expr returned a %s", String(tmp))
					return false
				}
				aliasedExpr.Expr = newExpr
				if inner.didAnythingChange() {
					aliasedExpr.As = NewColIdent(buf.String())
				}
				er.bindVars.MergeWith(inner.bindVars)
			}
		}
	case *FuncExpr:
		er.funcRewrite(cursor, node)
	case *ColName:
		if node.Name.at == SingleAt {
			udv := strings.ToLower(node.Name.CompliantName())
			cursor.Replace(bindVarExpression(UserDefinedVariableName + udv))
			er.bindVars.AddUserDefVar(udv)
		}
	}
	return true
}

func (er *expressionRewriter) funcRewrite(cursor *Cursor, node *FuncExpr) {
	switch {
	// last_insert_id() -> :__lastInsertId
	case node.Name.EqualString("last_insert_id"):
		if len(node.Exprs) > 0 { //last_insert_id(x)
			er.err = vterrors.New(vtrpc.Code_UNIMPLEMENTED, "Argument to LAST_INSERT_ID() not supported")
		} else {
			cursor.Replace(bindVarExpression(LastInsertIDName))
			er.bindVars.AddFuncResult(LastInsertIDName)
		}
	// database() -> :__vtdbname
	case er.shouldRewriteDatabaseFunc &&
		(node.Name.EqualString("database") ||
			node.Name.EqualString("schema")):
		if len(node.Exprs) > 0 {
			er.err = vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "Syntax error. %s() takes no arguments", node.Name.String())
		} else {
			cursor.Replace(bindVarExpression(DBVarName))
			er.bindVars.AddFuncResult(DBVarName)
		}
	// found_rows() -> :__vtfrows
	case node.Name.EqualString("found_rows"):
		if len(node.Exprs) > 0 {
			er.err = vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "Arguments to FOUND_ROWS() not supported")
		} else {
			cursor.Replace(bindVarExpression(FoundRowsName))
			er.bindVars.AddFuncResult(FoundRowsName)
		}
	// row_count() -> :__vtrcount
	case node.Name.EqualString("row_count"):
		if len(node.Exprs) > 0 {
			er.err = vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "Arguments to ROW_COUNT() not supported")
		} else {
			cursor.Replace(bindVarExpression(RowCountName))
			er.bindVars.AddFuncResult(RowCountName)
		}
	}
}

func (er *expressionRewriter) didAnythingChange() bool {
	return len(er.bindVars.NeedUserDefinedVariables) > 0 ||
		len(er.bindVars.needSystemVariable) > 0 ||
		len(er.bindVars.needFunctionResult) > 0
}

func bindVarExpression(name string) Expr {
	return NewArgument([]byte(":" + name))
}
