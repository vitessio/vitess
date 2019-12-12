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

// UpdateBindVarNeeds copies bind var needs from primitiveBuilders used for subqueries
func (rr *RewriteResult) UpdateBindVarNeeds(pb *primitiveBuilder) {
	pb.needsDbName = pb.needsDbName || rr.NeedDatabase
	pb.needsLastInsertID = pb.needsLastInsertID || rr.NeedLastInsertID
}

// Rewrite will rewrite an expression. Currently it does the following rewrites:
//  - `last_insert_id()` => `:vtlastid`
//  - `database()`       => `:vtdbname`
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
