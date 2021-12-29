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
	"fmt"
	"testing"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vterrors"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

// TestSimplifyUnsupportedQuery should be used to whenever we get a planner bug reported
// It will try to minimize the query to make it easier to understand and work with the bug.
func TestSimplifyUnsupportedQuery(t *testing.T) {
	query := "select user.id, user.name, count(*), unsharded.name from user join unsharded where unsharded.id = 42"
	vschema := &vschemaWrapper{
		v:       loadSchema(t, "schema_test.json", true),
		version: Gen4,
	}
	stmt, reserved, err := sqlparser.Parse2(query)
	require.NoError(t, err)
	result, _ := sqlparser.RewriteAST(stmt, vschema.currentDb(), sqlparser.SQLSelectLimitUnset)
	vschema.currentDb()

	reservedVars := sqlparser.NewReservedVars("vtg", reserved)
	_, err = BuildFromStmt(query, result.AST, reservedVars, vschema, result.BindVarNeeds, true, true)
	require.Error(t, err)
	state := vterrors.ErrState(err)

	simplified := simplifyStatement(result.AST.(sqlparser.SelectStatement), vschema.currentDb(), vschema, func(statement sqlparser.SelectStatement) bool {
		_, err := BuildFromStmt(query, statement, reservedVars, vschema, result.BindVarNeeds, true, true)
		if err == nil {
			return false
		}
		return vterrors.ErrState(err) == state
	})

	fmt.Println(sqlparser.String(simplified))
}

func TestUnsupportedFile(t *testing.T) {
	vschema := &vschemaWrapper{
		v:       loadSchema(t, "schema_test.json", true),
		version: Gen4,
	}
	fmt.Println(vschema)
	for tcase := range iterateExecFile("unsupported_cases.txt") {
		t.Run(fmt.Sprintf("%d:%s", tcase.lineno, tcase.input), func(t *testing.T) {
			log.Errorf("%s:%d - %s", tcase.file, tcase.lineno, tcase.input)
			stmt, reserved, err := sqlparser.Parse2(tcase.input)
			require.NoError(t, err)
			_, ok := stmt.(sqlparser.SelectStatement)
			if !ok {
				t.Skip()
				return
			}
			result, _ := sqlparser.RewriteAST(stmt, vschema.currentDb(), sqlparser.SQLSelectLimitUnset)
			vschema.currentDb()

			reservedVars := sqlparser.NewReservedVars("vtg", reserved)
			plan, err := BuildFromStmt(sqlparser.String(result.AST), result.AST, reservedVars, vschema, result.BindVarNeeds, true, true)
			out := getPlanOrErrorOutput(err, plan)

			simplified := simplifyStatement(result.AST.(sqlparser.SelectStatement), vschema.currentDb(), vschema, func(statement sqlparser.SelectStatement) bool {
				plan, err := BuildFromStmt(sqlparser.String(statement), statement, reservedVars, vschema, result.BindVarNeeds, true, true)
				out2 := getPlanOrErrorOutput(err, plan)
				return out == out2
			})

			fmt.Println(sqlparser.String(simplified))
		})
	}
}

func TestFindAllExpressions(t *testing.T) {
	query := `
select 
	user.selectExpr1, 
	unsharded.selectExpr2,
	count(*) as leCount
from 
	user join 
	unsharded on 
		user.joinCond = unsharded.joinCond 
where
	unsharded.wherePred = 42 and
	wherePred = 'foo' and 
	user.id = unsharded.id
group by 
	user.groupByExpr1 + unsharded.groupByExpr2
order by 
	user.orderByExpr1 desc, 
	unsharded.orderByExpr2 asc
limit 123 offset 456
`
	ast, err := sqlparser.Parse(query)
	require.NoError(t, err)
	ch := make(chan expressionCursor)
	findExpressions(ast.(sqlparser.SelectStatement), ch)
	for cursor := range ch {
		exploreExpression(cursor, ast)
	}
}

func exploreExpression(cursor expressionCursor, ast sqlparser.Statement) {
	defer cursor.wg.Done()
	fmt.Printf(">> found expression: %s\n", sqlparser.String(cursor.expr))
	cursor.replace(sqlparser.NewIntLiteral("1"))
	fmt.Printf("remove: %s\n", sqlparser.String(ast))
	cursor.restore()
	fmt.Printf("restore: %s\n", sqlparser.String(ast))
	cursor.remove()
	fmt.Printf("replace it with literal: %s\n", sqlparser.String(ast))
	cursor.restore()
	fmt.Printf("restore: %s\n", sqlparser.String(ast))
}

func TestAbortExpressionCursor(t *testing.T) {
	query := "select user.id, count(*), unsharded.name from user join unsharded on 13 = 14 where unsharded.id = 42 and name = 'foo' and user.id = unsharded.id"
	ast, err := sqlparser.Parse(query)
	require.NoError(t, err)
	ch := make(chan expressionCursor)
	findExpressions(ast.(sqlparser.SelectStatement), ch)
	for cursor := range ch {
		fmt.Println(sqlparser.String(cursor.expr))
		cursor.replace(sqlparser.NewIntLiteral("1"))
		fmt.Println(sqlparser.String(ast))
		cursor.replace(cursor.expr)
		if _, ok := cursor.expr.(*sqlparser.FuncExpr); ok {
			cursor.abort()
			break
		}
		cursor.wg.Done()
	}
}
