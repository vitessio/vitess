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
	gen := NewGenerator(5)
	exp := gen.Expression(ExprGeneratorConfig{})

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
	parser := NewTestParser()
	stmt, err := parser.Parse(q)
	require.NoError(t, err)
	count := 0
	Rewrite(stmt, func(cursor *Cursor) bool {
		switch node := cursor.Node().(type) {
		case *Select:
			node.SelectExprs.Exprs[0] = &AliasedExpr{
				Expr: NewStrLiteral("apa"),
			}
			node.SelectExprs.Exprs = append(node.SelectExprs.Exprs, &AliasedExpr{
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

func TestFindColNamesWithPaths(t *testing.T) {
	// this is the tpch query #1
	q := `
select l_returnflag,
       l_linestatus,
       sum(l_quantity)                                       as sum_qty,
       sum(l_extendedprice)                                  as sum_base_price,
       sum(l_extendedprice * (1 - l_discount))               as sum_disc_price,
       sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
       avg(l_quantity)                                       as avg_qty,
       avg(l_extendedprice)                                  as avg_price,
       avg(l_discount)                                       as avg_disc,
       count(*)                                              as count_order
from lineitem
where l_shipdate <= '1998-12-01' - interval '108' day
group by l_returnflag, l_linestatus
order by l_returnflag, l_linestatus`
	ast, err := NewTestParser().Parse(q)
	require.NoError(t, err)

	var foundColNames []string
	RewriteWithPath(ast,
		func(cursor *Cursor) bool {
			_, ok := cursor.Node().(*ColName)
			if ok {
				foundColNames = append(foundColNames, cursor.CurrentPath().DebugString())
			}

			return true
		}, nil)

	expected := []string{
		"(*SelectExprs).ExprsOffset(0)->(*AliasedExpr).Expr",                                                       // l_returnflag
		"(*SelectExprs).ExprsOffset(1)->(*AliasedExpr).Expr",                                                       // l_linestatus
		"(*SelectExprs).ExprsOffset(2)->(*AliasedExpr).Expr->(*Sum).Arg",                                           // sum(l_quantity)
		"(*SelectExprs).ExprsOffset(3)->(*AliasedExpr).Expr->(*Sum).Arg",                                           // sum(l_extendedprice)
		"(*SelectExprs).ExprsOffset(4)->(*AliasedExpr).Expr->(*Sum).Arg->(*BinaryExpr).Left",                       // sum(->l_extendedprice<- * (1 - l_discount))
		"(*SelectExprs).ExprsOffset(4)->(*AliasedExpr).Expr->(*Sum).Arg->(*BinaryExpr).Right->(*BinaryExpr).Right", // sum(l_extendedprice * (1 - ->l_discount<-))
		"(*SelectExprs).ExprsOffset(5)->(*AliasedExpr).Expr->(*Sum).Arg->(*BinaryExpr).Left->(*BinaryExpr).Left",   // etc
		"(*SelectExprs).ExprsOffset(5)->(*AliasedExpr).Expr->(*Sum).Arg->(*BinaryExpr).Left->(*BinaryExpr).Right->(*BinaryExpr).Right",
		"(*SelectExprs).ExprsOffset(5)->(*AliasedExpr).Expr->(*Sum).Arg->(*BinaryExpr).Right->(*BinaryExpr).Right",
		"(*SelectExprs).ExprsOffset(6)->(*AliasedExpr).Expr->(*Avg).Arg",
		"(*SelectExprs).ExprsOffset(7)->(*AliasedExpr).Expr->(*Avg).Arg",
		"(*SelectExprs).ExprsOffset(8)->(*AliasedExpr).Expr->(*Avg).Arg",
		"(*Select).Where->(*Where).Expr->(*ComparisonExpr).Left",
		"(*GroupBy).ExprsOffset(0)",
		"(*GroupBy).ExprsOffset(1)",
		"(*Select).OrderBy->(OrderBy)[]Offset(0)->(*Order).Expr",
		"(*Select).OrderBy->(OrderBy)[]Offset(1)->(*Order).Expr",
	}

	assert.Equal(t, expected, foundColNames)
}

func TestReplaceAndRevisitWorksInLaterCalls(t *testing.T) {
	q := "select * from tbl1"
	parser := NewTestParser()
	stmt, err := parser.Parse(q)
	require.NoError(t, err)
	count := 0
	Rewrite(stmt, func(cursor *Cursor) bool {
		switch node := cursor.Node().(type) {
		case *SelectExprs:
			if len(node.Exprs) != 1 {
				return true
			}
			expr1 := &AliasedExpr{
				Expr: NewStrLiteral("apa"),
			}
			expr2 := &AliasedExpr{
				Expr: NewStrLiteral("foobar"),
			}
			cursor.ReplaceAndRevisit(&SelectExprs{Exprs: []SelectExpr{expr1, expr2}})
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
	parser := NewTestParser()
	parse, err := parser.Parse("select * from a join b on a.id = b.id")
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
