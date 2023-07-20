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

package simplifier

import (
	"fmt"
	"testing"

	"vitess.io/vitess/go/mysql/collations"
	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/vtgate/evalengine"

	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/sqlparser"
)

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
	visitAllExpressionsInAST(ast.(sqlparser.SelectStatement), func(cursor expressionCursor) bool {
		fmt.Printf(">> found expression: %s\n", sqlparser.String(cursor.expr))
		cursor.replace(sqlparser.NewIntLiteral("1"))
		fmt.Printf("remove: %s\n", sqlparser.String(ast))
		cursor.restore()
		fmt.Printf("restore: %s\n", sqlparser.String(ast))
		cursor.remove()
		fmt.Printf("replace it with literal: %s\n", sqlparser.String(ast))
		cursor.restore()
		fmt.Printf("restore: %s\n", sqlparser.String(ast))
		return true
	})
}

func TestAbortExpressionCursor(t *testing.T) {
	query := "select user.id, count(*), unsharded.name from user join unsharded on 13 = 14 where unsharded.id = 42 and name = 'foo' and user.id = unsharded.id"
	ast, err := sqlparser.Parse(query)
	require.NoError(t, err)
	visitAllExpressionsInAST(ast.(sqlparser.SelectStatement), func(cursor expressionCursor) bool {
		fmt.Println(sqlparser.String(cursor.expr))
		cursor.replace(sqlparser.NewIntLiteral("1"))
		fmt.Println(sqlparser.String(ast))
		cursor.replace(cursor.expr)
		_, isFunc := cursor.expr.(sqlparser.AggrFunc)
		return !isFunc
	})
}

func TestSimplifyEvalEngineExpr(t *testing.T) {
	// ast struct for L0         +
	// L1                +            +
	// L2             +     +      +     +
	// L3            1 2   3 4    5 6   7 8

	// L3
	i1, i2, i3, i4, i5, i6, i7, i8 :=
		sqlparser.NewIntLiteral("1"),
		sqlparser.NewIntLiteral("2"),
		sqlparser.NewIntLiteral("3"),
		sqlparser.NewIntLiteral("4"),
		sqlparser.NewIntLiteral("5"),
		sqlparser.NewIntLiteral("6"),
		sqlparser.NewIntLiteral("7"),
		sqlparser.NewIntLiteral("8")
	// L2
	p21, p22, p23, p24 :=
		plus(i1, i2),
		plus(i3, i4),
		plus(i5, i6),
		plus(i7, i8)

	// L1
	p11, p12 :=
		plus(p21, p22),
		plus(p23, p24)

	// L0
	p0 := plus(p11, p12)

	expr := SimplifyExpr(p0, func(expr sqlparser.Expr) bool {
		local, err := evalengine.Translate(expr, nil)
		if err != nil {
			return false
		}
		res, err := evalengine.EmptyExpressionEnv().Evaluate(local)
		if err != nil {
			return false
		}
		toInt64, err := res.Value(collations.Default()).ToInt64()
		if err != nil {
			return false
		}
		return toInt64 >= 8
	})
	log.Infof("simplest expr to evaluate to >= 8: [%s], started from: [%s]", sqlparser.String(expr), sqlparser.String(p0))
}

func plus(a, b sqlparser.Expr) sqlparser.Expr {
	return &sqlparser.BinaryExpr{
		Operator: sqlparser.PlusOp,
		Left:     a,
		Right:    b,
	}
}
