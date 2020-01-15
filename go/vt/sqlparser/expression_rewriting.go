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
	Rewrite(in, er.goingDown, er.comingUp)

	return &RewriteASTResult{
		AST:              in,
		NeedLastInsertID: er.lastInsertID,
		NeedDatabase:     er.database,
	}, nil
}

// RewriteASTResult contains the rewritten ast and meta information about it
type RewriteASTResult struct {
	AST              Statement
	NeedLastInsertID bool
	NeedDatabase     bool
}

type expressionRewriter struct {
	lastInsertID, database bool
	err                    error
	aliases                []*AliasedExpr
}

func (er *expressionRewriter) comingUp(cursor *Cursor) bool {
	if er.err != nil {
		return false
	}

	n := len(er.aliases) - 1
	if n >= 0 {
		// if we encounter the last alias when coming up, we'll pop it from the stack
		topOfStack := er.aliases[n]
		if cursor.Node() == topOfStack {
			er.aliases = er.aliases[:n]
		}
	}

	return true
}

// walks the stack of seen AliasedExpr and adds column aliases where there isn't any already
func (er *expressionRewriter) addAliasIfNeeded() {
	idents := make([]ColIdent, len(er.aliases))
	for i, node := range er.aliases {
		if node.As.IsEmpty() {
			buf := NewTrackedBuffer(nil)
			node.Expr.Format(buf)
			idents[i] = NewColIdent(buf.String())
		} else {
			idents[i] = node.As
		}
	}
	for i, node := range er.aliases {
		node.As = idents[i]
	}
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
		er.aliases = append(er.aliases, node)

	case *FuncExpr:
		switch {
		case node.Name.EqualString("last_insert_id"):
			if len(node.Exprs) > 0 {
				er.err = vterrors.New(vtrpc.Code_UNIMPLEMENTED, "Argument to LAST_INSERT_ID() not supported")
			} else {
				er.addAliasIfNeeded()
				cursor.Replace(bindVarExpression(LastInsertIDName))
				er.lastInsertID = true
			}
		case node.Name.EqualString("database"):
			if len(node.Exprs) > 0 {
				er.err = vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "Syntax error. DATABASE() takes no arguments")
			} else {
				er.addAliasIfNeeded()
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
