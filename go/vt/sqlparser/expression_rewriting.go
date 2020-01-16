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
	"vitess.io/vitess/go/vt/log"
	querypb "vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/vterrors"
)

// PrepareAST will normalize the query
func PrepareAST(in Statement, bindVars map[string]*querypb.BindVariable, prefix string) (*RewriteASTResult, error) {
	Normalize(in, bindVars, prefix)
	return RewriteAST(in)
}

// RewriteAST rewrites the whole AST, replacing function calls and adding column aliases to queries
func RewriteAST(in Statement) (*RewriteASTResult, error) {
	er := new(expressionRewriter)
	er.shouldRewriteDatabaseFunc = shouldRewriteDatabaseFunc(in)
	Rewrite(in, er.goingDown, nil)

	return &RewriteASTResult{
		AST:              in,
		NeedLastInsertID: er.lastInsertID,
		NeedDatabase:     er.database,
	}, nil
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
	AST              Statement
	NeedLastInsertID bool
	NeedDatabase     bool
}

type expressionRewriter struct {
	lastInsertID, database    bool
	shouldRewriteDatabaseFunc bool
	err                       error
}

const (
	//LastInsertIDName is a reserved bind var name for last_insert_id()
	LastInsertIDName = "__lastInsertId"
	//DBVarName is a reserved bind var name for database()
	DBVarName = "__vtdbname"
)

func (er *expressionRewriter) goingDown(cursor *Cursor) bool {
	switch node := cursor.Node().(type) {
	case *AliasedExpr:
		if node.As.IsEmpty() {
			buf := NewTrackedBuffer(nil)
			node.Expr.Format(buf)
			inner := new(expressionRewriter)
			inner.shouldRewriteDatabaseFunc = er.shouldRewriteDatabaseFunc
			tmp := Rewrite(node.Expr, inner.goingDown, nil)
			newExpr, ok := tmp.(Expr)
			if !ok {
				log.Errorf("failed to rewrite AST. function expected to return Expr returned a %s", String(tmp))
				return false
			}
			node.Expr = newExpr
			er.database = er.database || inner.database
			er.lastInsertID = er.lastInsertID || inner.lastInsertID
			if inner.didAnythingChange() {
				node.As = NewColIdent(buf.String())
			}
			return false
		}

	case *FuncExpr:
		switch {
		case node.Name.EqualString("last_insert_id"):
			if len(node.Exprs) > 0 {
				er.err = vterrors.New(vtrpc.Code_UNIMPLEMENTED, "Argument to LAST_INSERT_ID() not supported")
			} else {
				cursor.Replace(bindVarExpression(LastInsertIDName))
				er.lastInsertID = true
			}
		case er.shouldRewriteDatabaseFunc &&
			(node.Name.EqualString("database") ||
			 node.Name.EqualString("schema")):
			if len(node.Exprs) > 0 {
				er.err = vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "Syntax error. %s() takes no arguments", node.Name.String())
			} else {
				cursor.Replace(bindVarExpression(DBVarName))
				er.database = true
			}
		}
	}
	return true
}

func (er *expressionRewriter) didAnythingChange() bool {
	return er.database || er.lastInsertID
}

func bindVarExpression(name string) *SQLVal {
	return NewValArg([]byte(":" + name))
}
