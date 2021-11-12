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

package sqlparser

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

func BenchmarkVisitLargeExpression(b *testing.B) {
	gen := newGenerator(1, 5)
	exp := gen.expression()

	depth := 0
	for i := 0; i < b.N; i++ {
		_ = Rewrite(exp, func(cursor *Cursor) bool {
			depth++
			return true
		}, func(cursor *Cursor) bool {
			depth--
			return true
		})
	}
}

func TestReplaceWorksInLaterCalls(t *testing.T) {
	q := "select * from tbl1"
	stmt, err := Parse(q)
	require.NoError(t, err)
	count := 0
	Rewrite(stmt, func(cursor *Cursor) bool {
		switch node := cursor.Node().(type) {
		case *Select:
			node.SelectExprs[0] = &AliasedExpr{
				Expr: NewStrLiteral("apa"),
			}
			node.SelectExprs = append(node.SelectExprs, &AliasedExpr{
				Expr: NewStrLiteral("foobar"),
			})
		case *StarExpr:
			t.Errorf("should not have seen the star")
		case *Literal:
			count++
		}
		return true
	}, nil)
	assert.Equal(t, 2, count)
}

func TestReplaceAndRevisitWorksInLaterCalls(t *testing.T) {
	q := "select * from tbl1"
	stmt, err := Parse(q)
	require.NoError(t, err)
	count := 0
	Rewrite(stmt, func(cursor *Cursor) bool {
		switch node := cursor.Node().(type) {
		case SelectExprs:
			if len(node) != 1 {
				return true
			}
			expr1 := &AliasedExpr{
				Expr: NewStrLiteral("apa"),
			}
			expr2 := &AliasedExpr{
				Expr: NewStrLiteral("foobar"),
			}
			cursor.ReplaceAndRevisit(SelectExprs{expr1, expr2})
		case *StarExpr:
			t.Errorf("should not have seen the star")
		case *Literal:
			count++
		}
		return true
	}, nil)
	assert.Equal(t, 2, count)
}

func TestChangeValueTypeGivesError(t *testing.T) {
	parse, err := Parse("select * from a join b on a.id = b.id")
	require.NoError(t, err)

	defer func() {
		if r := recover(); r != nil {
			require.Equal(t, "[BUG] tried to replace 'On' on 'JoinCondition'", r)
		}
	}()
	_ = Rewrite(parse, func(cursor *Cursor) bool {
		_, ok := cursor.Node().(*ComparisonExpr)
		if ok {
			cursor.Replace(&NullVal{}) // this is not a valid replacement because the container is a value type
		}
		return true
	}, nil)

}
