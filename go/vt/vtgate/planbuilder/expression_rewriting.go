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

package planbuilder

import (
	"vitess.io/vitess/go/vt/proto/vtrpc"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vterrors"
	"vitess.io/vitess/go/vt/vtgate/engine"
)

// RewriteResult contains the rewritten expression and meta information about it
type RewriteResult struct {
	Expression       sqlparser.Expr
	NeedLastInsertID bool
	NeedDatabase     bool
}

// RewriteASTResult contains the rewritten ast and meta information about it
type RewriteASTResult struct {
	AST              sqlparser.Statement
	NeedLastInsertID bool
	NeedDatabase     bool
}

// UpdateBindVarNeeds copies bind var needs from primitiveBuilders used for subqueries
func (rr *RewriteResult) UpdateBindVarNeeds(pb *primitiveBuilder) {
	pb.needsDbName = pb.needsDbName || rr.NeedDatabase
	pb.needsLastInsertID = pb.needsLastInsertID || rr.NeedLastInsertID
}

// RewriteAndUpdateBuilder rewrites expressions and updates the primitive builder to remember what bindvar needs it has
func RewriteAndUpdateBuilder(in sqlparser.Expr, pb *primitiveBuilder) (sqlparser.Expr, error) {
	out, err := Rewrite(in)
	if err != nil {
		return nil, err
	}
	out.UpdateBindVarNeeds(pb)
	return out.Expression, nil
}

type expressionRewriter struct {
	lastInsertID, database bool
	err                    error
	aliases                []*sqlparser.AliasedExpr
}

func (er *expressionRewriter) comingUp(cursor *sqlparser.Cursor) bool {
	if er.err != nil {
		return false
	}

	n := len(er.aliases) - 1
	if n > 0 {
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
	idents := make([]sqlparser.ColIdent, len(er.aliases))
	for i, node := range er.aliases {
		if node.As.IsEmpty() {
			buf := sqlparser.NewTrackedBuffer(nil)
			node.Expr.Format(buf)
			idents[i] = sqlparser.NewColIdent(buf.String())
		} else {
			idents[i] = node.As
		}
	}
	for i, node := range er.aliases {
		node.As = idents[i]
	}
}

func (er *expressionRewriter) goingDown(cursor *sqlparser.Cursor) bool {
	switch node := cursor.Node().(type) {
	case *sqlparser.AliasedExpr:
		er.aliases = append(er.aliases, node)

	case *sqlparser.FuncExpr:
		switch {
		case node.Name.EqualString("last_insert_id"):
			if len(node.Exprs) > 0 {
				er.err = vterrors.New(vtrpc.Code_UNIMPLEMENTED, "Argument to LAST_INSERT_ID() not supported")
			} else {
				er.addAliasIfNeeded()
				cursor.Replace(bindVarExpression(engine.LastInsertIDName))
				er.lastInsertID = true
			}
		case node.Name.EqualString("database"):
			if len(node.Exprs) > 0 {
				er.err = vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "Syntax error. DATABASE() takes no arguments")
			} else {
				er.addAliasIfNeeded()
				cursor.Replace(bindVarExpression(engine.DBVarName))
				er.database = true
			}
		}
	}
	return true
}

func (er *expressionRewriter) didAnythingChange() bool {
	return er.database || er.lastInsertID
}

// RewriteAST rewrites the whole AST, replacing function calls and adding column aliases to queries
func RewriteAST(in sqlparser.Statement) (*RewriteASTResult, error) {
	er := new(expressionRewriter)
	sqlparser.Rewrite(in, er.goingDown, er.comingUp)

	return &RewriteASTResult{
		AST:              in,
		NeedLastInsertID: er.lastInsertID,
		NeedDatabase:     er.database,
	}, nil
}

// Rewrite will rewrite an expression. Currently it does the following rewrites:
//  - `last_insert_id()` => `:__lastInsertId`
//  - `database()`       => `:__vtdbname`
func Rewrite(in sqlparser.Expr) (*RewriteResult, error) {
	rewrites := make(map[*sqlparser.FuncExpr]sqlparser.Expr)
	liid := false
	db := false

	err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.FuncExpr:
			switch {
			case node.Name.EqualString("last_insert_id"):
				if len(node.Exprs) > 0 {
					return false, vterrors.New(vtrpc.Code_UNIMPLEMENTED, "Argument to LAST_INSERT_ID() not supported")
				}
				rewrites[node] = bindVarExpression(engine.LastInsertIDName)
				liid = true
			case node.Name.EqualString("database"):
				if len(node.Exprs) > 0 {
					return false, vterrors.New(vtrpc.Code_INVALID_ARGUMENT, "Syntax error. DATABASE() takes no arguments")
				}
				rewrites[node] = bindVarExpression(engine.DBVarName)
				db = true
			}
			return true, nil
		}
		return true, nil
	}, in)

	if err != nil {
		return nil, err
	}

	for from, to := range rewrites {
		in = sqlparser.ReplaceExpr(in, from, to)
	}

	return &RewriteResult{
		Expression:       in,
		NeedLastInsertID: liid,
		NeedDatabase:     db,
	}, nil
}

func bindVarExpression(name string) *sqlparser.SQLVal {
	return sqlparser.NewValArg([]byte(":" + name))
}
