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

package planbuilder

import (
	"testing"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
)

func TestSimplifyUnsupportedQuery(t *testing.T) {
	query := "select user.id, count(*), unsharded.name from user join unsharded where unsharded.id = 42"
	vschema := &vschemaWrapper{
		v: loadSchema(t, "schema_test.json", true),
	}
	vschema.version = Gen4
	stmt, reserved, err := sqlparser.Parse2(query)
	require.NoError(t, err)
	result, _ := sqlparser.RewriteAST(stmt, vschema.currentDb(), sqlparser.SQLSelectLimitUnset)
	vschema.currentDb()

	reservedVars := sqlparser.NewReservedVars("vtg", reserved)
	plan, err := BuildFromStmt(query, result.AST, reservedVars, vschema, result.BindVarNeeds, true, true)
	out := getPlanOrErrorOutput(err, plan)

	simplified := simplifyStatement(result.AST.(sqlparser.SelectStatement), vschema.currentDb(), vschema, func(statement sqlparser.SelectStatement) bool {
		plan, err := BuildFromStmt(query, statement, reservedVars, vschema, result.BindVarNeeds, true, true)
		out2 := getPlanOrErrorOutput(err, plan)
		return out == out2
	})

	t.Fatal(sqlparser.String(simplified))
}

func simplifyStatement(
	in sqlparser.SelectStatement,
	currentDB string,
	si semantics.SchemaInformation,
	test func(sqlparser.SelectStatement) bool,
) sqlparser.SelectStatement {
	// we start by removing one table at a time until we can't anymore
	semTable, err := semantics.Analyze(in, currentDB, si)
	if err != nil {
		return nil
	}
	for idx := range semTable.Tables {
		clone := sqlparser.CloneSelectStatement(in)
		inner, err := semantics.Analyze(clone, currentDB, si)
		if err != nil {
			panic(err) // this should never happen
		}
		searchedTS := semantics.SingleTableSet(idx)
		simplified := removeTable(clone, searchedTS, inner)
		if simplified && test(clone) {
			return simplifyStatement(clone, currentDB, si, test)
		}
	}
	return in
}

func removeTable(clone sqlparser.SelectStatement, searchedTS semantics.TableSet, inner *semantics.SemTable) bool {
	simplified := false
	sqlparser.Rewrite(clone, func(cursor *sqlparser.Cursor) bool {
		switch node := cursor.Node().(type) {
		case *sqlparser.JoinTableExpr:
			lft, ok := node.LeftExpr.(*sqlparser.AliasedTableExpr)
			if ok {
				ts := inner.TableSetFor(lft)
				if ts == searchedTS {
					cursor.Replace(node.RightExpr)
					simplified = true
				}
			}
			rgt, ok := node.RightExpr.(*sqlparser.AliasedTableExpr)
			if ok {
				ts := inner.TableSetFor(rgt)
				if ts == searchedTS {
					cursor.Replace(node.LeftExpr)
					simplified = true
				}
			}
		case *sqlparser.Select:
			if len(node.From) == 1 {
				return true
			}
			for i, tbl := range node.From {
				lft, ok := tbl.(*sqlparser.AliasedTableExpr)
				if ok {
					ts := inner.TableSetFor(lft)
					if ts == searchedTS {
						node.From = append(node.From[:i], node.From[i+1:]...)
						simplified = true
						return true
					}
				}
			}
		case *sqlparser.Where:
			exprs := sqlparser.SplitAndExpression(nil, node.Expr)
			var newPredicate sqlparser.Expr
			for _, expr := range exprs {
				if !inner.RecursiveDeps(expr).IsOverlapping(searchedTS) {
					newPredicate = sqlparser.AndExpressions(newPredicate, expr)
				}
			}
			node.Expr = newPredicate
		case sqlparser.SelectExprs:
			_, isSel := cursor.Parent().(*sqlparser.Select)
			if !isSel {
				return true
			}

			var newExprs sqlparser.SelectExprs
			for _, ae := range node {
				expr, ok := ae.(*sqlparser.AliasedExpr)
				if !ok {
					newExprs = append(newExprs, ae)
					continue
				}
				if !inner.RecursiveDeps(expr.Expr).IsOverlapping(searchedTS) || sqlparser.ContainsAggregation(expr.Expr) {
					newExprs = append(newExprs, ae)
				}
			}
			cursor.Replace(newExprs)
		}
		return true
	}, nil)
	return simplified
}
