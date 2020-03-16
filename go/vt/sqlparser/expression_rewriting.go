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

// BindVarNeeds represents the bind vars that need to be provided as the result of expression rewriting.
type BindVarNeeds struct {
	NeedLastInsertID         bool
	NeedDatabase             bool
	NeedFoundRows            bool
	NeedUserDefinedVariables bool
}

// RewriteAST rewrites the whole AST, replacing function calls and adding column aliases to queries
func RewriteAST(in Statement) (*RewriteASTResult, error) {
	er := newExpressionRewriter()
	er.shouldRewriteDatabaseFunc = shouldRewriteDatabaseFunc(in)
	Rewrite(in, er.goingDown, nil)

	r := &RewriteASTResult{
		AST: in,
	}
	if _, ok := er.bindVars[LastInsertIDName]; ok {
		r.NeedLastInsertID = true
	}
	if _, ok := er.bindVars[DBVarName]; ok {
		r.NeedDatabase = true
	}
	if _, ok := er.bindVars[FoundRowsName]; ok {
		r.NeedFoundRows = true
	}
	if _, ok := er.bindVars[UserDefinedVariableName]; ok {
		r.NeedUserDefinedVariables = true
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
	BindVarNeeds
	AST Statement // The rewritten AST
}

type expressionRewriter struct {
	bindVars                  map[string]struct{}
	shouldRewriteDatabaseFunc bool
	err                       error
}

func newExpressionRewriter() *expressionRewriter {
	return &expressionRewriter{bindVars: make(map[string]struct{})}
}

const (
	//LastInsertIDName is a reserved bind var name for last_insert_id()
	LastInsertIDName = "__lastInsertId"

	//DBVarName is a reserved bind var name for database()
	DBVarName = "__vtdbname"

	//FoundRowsName is a reserved bind var name for found_rows()
	FoundRowsName = "__vtfrows"

	//UserDefinedVariableName is what we prepend bind var names for user defined variables
	UserDefinedVariableName = "__vtudv"
)

func (er *expressionRewriter) goingDown(cursor *Cursor) bool {
	switch node := cursor.Node().(type) {
	// select last_insert_id() -> select :__lastInsertId as `last_insert_id()`
	case *AliasedExpr:
		if node.As.IsEmpty() {
			buf := NewTrackedBuffer(nil)
			node.Expr.Format(buf)
			inner := newExpressionRewriter()
			inner.shouldRewriteDatabaseFunc = er.shouldRewriteDatabaseFunc
			tmp := Rewrite(node.Expr, inner.goingDown, nil)
			newExpr, ok := tmp.(Expr)
			if !ok {
				log.Errorf("failed to rewrite AST. function expected to return Expr returned a %s", String(tmp))
				return false
			}
			node.Expr = newExpr
			if inner.didAnythingChange() {
				node.As = NewColIdent(buf.String())
			}
			for k := range inner.bindVars {
				er.needBindVarFor(k)
			}
			return false
		}
	case *FuncExpr:
		switch {
		// last_insert_id() -> :__lastInsertId
		case node.Name.EqualString("last_insert_id"):
			if len(node.Exprs) > 0 { //last_insert_id(x)
				er.err = vterrors.New(vtrpc.Code_UNIMPLEMENTED, "Argument to LAST_INSERT_ID() not supported")
			} else {
				cursor.Replace(bindVarExpression(LastInsertIDName))
				er.needBindVarFor(LastInsertIDName)
			}
		case er.shouldRewriteDatabaseFunc &&
			(node.Name.EqualString("database") ||
				node.Name.EqualString("schema")):
			if len(node.Exprs) > 0 {
				er.err = vterrors.Errorf(vtrpc.Code_INVALID_ARGUMENT, "Syntax error. %s() takes no arguments", node.Name.String())
			} else {
				cursor.Replace(bindVarExpression(DBVarName))
				er.needBindVarFor(DBVarName)
			}
		case node.Name.EqualString("found_rows"):
			if len(node.Exprs) > 0 {
				er.err = vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "Arguments to FOUND_ROWS() not supported")
			} else {
				cursor.Replace(bindVarExpression(FoundRowsName))
				er.needBindVarFor(FoundRowsName)
			}
		}
	case *ColName:
		if node.Name.at == SingleAt {
			cursor.Replace(bindVarExpression(UserDefinedVariableName + node.Name.CompliantName()))
			er.needBindVarFor(UserDefinedVariableName)
		}
	}
	return true
}

// instead of creating new objects, we'll reuse this one
var token = struct{}{}

func (er *expressionRewriter) needBindVarFor(name string) {
	er.bindVars[name] = token
}

func (er *expressionRewriter) didAnythingChange() bool {
	return len(er.bindVars) > 0
}

func bindVarExpression(name string) *SQLVal {
	return NewValArg([]byte(":" + name))
}
