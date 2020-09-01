/*
Copyright 2020 The Vitess Authors.

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

package semantic

import (
	"testing"

	"gotest.tools/assert"

	"github.com/stretchr/testify/require"
	"vitess.io/vitess/go/vt/sqlparser"
)

func TestAnalyse(t *testing.T) {
	type testCase struct {
		name                     string
		query                    string
		col1Qualifier, col1Table string
		col2Qualifier, col2Table string
		error                    string
	}
	tests := []testCase{{
		name:          "one table, explicit alias usage",
		query:         "select x.col from information_schema.tables as x",
		col1Qualifier: "information_schema", col1Table: "tables",
	}, {
		name:          "one table, no alias and no explicit columns",
		query:         "select col from information_schema.tables",
		col1Qualifier: "information_schema", col1Table: "tables",
	}, {
		name:  "missing table ",
		query: "select x.col from y",
		error: "ERROR 1054 (42S22): Unknown column 'x.col' in 'field list'",
	}, {
		name:  "aliased tables stays aliased",
		query: "select t.col from t as x",
		error: "ERROR 1054 (42S22): Unknown column 't.col' in 'field list'",
	}, {
		name:          "two tables with qualifiers, and columns do not specify qualifier",
		query:         "select y.col1, x.col2 from qualify1.y, qualify2.x",
		col1Qualifier: "qualify1", col1Table: "y",
		col2Qualifier: "qualify2", col2Table: "x",
	}, {
		name:          "subqueries can use columns from the outer scope",
		query:         "select col1 from t as x where EXISTS (select id from z where x.id = id)",
		col1Qualifier: "", col1Table: "t",
	}, {
		name:  "derived tables cannot access tables from the outer scope",
		query: "select t.col from t, (select * from x where x.col = t.col) as y",
		error: "ERROR 1054 (42S22): Unknown column 't.col' in 'field list'",
	}, {
		name:  "can't do a subquery in a group by",
		query: "select (select count(*) from x) as col from dual group by (select count(*) from x)",
		error: "unsupported: subqueries disallowed in GROUP or ORDER BY",
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ast, err := sqlparser.Parse(tc.query)
			require.NoError(t, err)
			_, err = Analyse(ast)
			if tc.error == "" {
				require.NoError(t, err)

				// get the first expression of the query as a ColName
				colName := ast.(*sqlparser.Select).SelectExprs[0].(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName)
				col1Table := colName.Metadata.(Table)
				assert.Equal(t, col1Table.Qualifier, tc.col1Qualifier, "first column qualifier")
				assert.Equal(t, col1Table.Name, tc.col1Table, "first column table name")

				if tc.col2Table != "" {
					colName2 := ast.(*sqlparser.Select).SelectExprs[1].(*sqlparser.AliasedExpr).Expr.(*sqlparser.ColName)
					col2Table := colName2.Metadata.(Table)
					assert.Equal(t, col2Table.Qualifier, tc.col2Qualifier, "first column qualifier")
					assert.Equal(t, col2Table.Name, tc.col2Table, "first column table name")
				}
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.error)
			}
		})
	}
}
