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
}

// Rewrite will rewrite an expression. Currently it does the following rewrites:
//  - `last_insert_id()` => `:vtlastid`
func Rewrite(in sqlparser.Expr) (*RewriteResult, error) {
	result, needLastInsertID, err := rewriteLastInsertID(in)
	if err != nil {
		return nil, err
	}
	return &RewriteResult{
		Expression:       result,
		NeedLastInsertID: needLastInsertID,
	}, nil
}

func rewriteLastInsertID(in sqlparser.Expr) (_ sqlparser.Expr, needLastInsertID bool, _ error) {
	matchingNodes := make([]*sqlparser.FuncExpr, 0)

	err := sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch node := node.(type) {
		case *sqlparser.FuncExpr:
			switch {
			case node.Name.EqualString("last_insert_id"):
				if len(node.Exprs) > 0 {
					return false, vterrors.New(vtrpc.Code_UNIMPLEMENTED, "Argument to LAST_INSERT_ID() not supported")
				}
				matchingNodes = append(matchingNodes, node)
			}
			return true, nil
		}
		return true, nil
	}, in)

	if err != nil {
		return nil, false, err
	}

	for _, node := range matchingNodes {
		in = sqlparser.ReplaceExpr(in, node, bindVarExpression())
	}

	return in, len(matchingNodes) > 0, nil
}

func bindVarExpression() *sqlparser.SQLVal {
	return sqlparser.NewValArg([]byte(":" + engine.LastInsertIDName))
}
