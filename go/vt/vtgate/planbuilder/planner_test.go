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
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestBindingSubquery(t *testing.T) {
	testcases := []struct {
		query            string
		requiredTableSet semantics.TableSet
		extractor        func(p *sqlparser.Select) sqlparser.Expr
		rewrite          bool
	}{
		{
			query:            "select (select col from tabl limit 1) as a from foo join tabl order by a + 1",
			requiredTableSet: semantics.EmptyTableSet(),
			extractor: func(sel *sqlparser.Select) sqlparser.Expr {
				return sel.OrderBy[0].Expr
			},
			rewrite: true,
		}, {
			query:            "select t.a from (select (select col from tabl limit 1) as a from foo join tabl) t",
			requiredTableSet: semantics.EmptyTableSet(),
			extractor: func(sel *sqlparser.Select) sqlparser.Expr {
				return extractExpr(sel, 0)
			},
			rewrite: true,
		}, {
			query:            "select (select col from tabl where foo.id = 4 limit 1) as a from foo",
			requiredTableSet: semantics.SingleTableSet(0),
			extractor: func(sel *sqlparser.Select) sqlparser.Expr {
				return extractExpr(sel, 0)
			},
			rewrite: false,
		},
	}
	for _, testcase := range testcases {
		t.Run(testcase.query, func(t *testing.T) {
			parse, err := sqlparser.Parse(testcase.query)
			require.NoError(t, err)
			selStmt := parse.(*sqlparser.Select)
			semTable, err := semantics.Analyze(selStmt, "d", &semantics.FakeSI{
				Tables: map[string]*vindexes.Table{
					"tabl": {Name: sqlparser.NewIdentifierCS("tabl")},
					"foo":  {Name: sqlparser.NewIdentifierCS("foo")},
				},
			})
			require.NoError(t, err)
			if testcase.rewrite {
				err = queryRewrite(semTable, sqlparser.NewReservedVars("vt", make(sqlparser.BindVars)), selStmt)
				require.NoError(t, err)
			}
			expr := testcase.extractor(selStmt)
			tableset := semTable.RecursiveDeps(expr)
			require.Equal(t, testcase.requiredTableSet, tableset)
		})
	}
}

func extractExpr(in *sqlparser.Select, idx int) sqlparser.Expr {
	return in.SelectExprs[idx].(*sqlparser.AliasedExpr).Expr
}
