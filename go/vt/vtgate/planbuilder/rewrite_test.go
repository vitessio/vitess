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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/semantics"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestExpandStar(t *testing.T) {
	schemaInfo := &semantics.FakeSI{
		Tables: map[string]*vindexes.Table{
			"t1": {
				Name: sqlparser.NewTableIdent("t1"),
				Columns: []vindexes.Column{{
					Name: sqlparser.NewColIdent("a"),
					Type: sqltypes.VarChar,
				}, {
					Name: sqlparser.NewColIdent("b"),
					Type: sqltypes.VarChar,
				}, {
					Name: sqlparser.NewColIdent("c"),
					Type: sqltypes.VarChar,
				}},
				ColumnListAuthoritative: true,
			},
			"t2": {
				Name: sqlparser.NewTableIdent("t2"),
				Columns: []vindexes.Column{{
					Name: sqlparser.NewColIdent("c1"),
					Type: sqltypes.VarChar,
				}, {
					Name: sqlparser.NewColIdent("c2"),
					Type: sqltypes.VarChar,
				}},
				ColumnListAuthoritative: true,
			},
			"t3": { // non authoritative table.
				Name: sqlparser.NewTableIdent("t3"),
				Columns: []vindexes.Column{{
					Name: sqlparser.NewColIdent("col"),
					Type: sqltypes.VarChar,
				}},
				ColumnListAuthoritative: false,
			},
		},
	}
	cDB := "db"
	tcases := []struct {
		sql    string
		expSQL string
		expErr string
	}{{
		sql:    "select * from t1",
		expSQL: "select t1.a as a, t1.b as b, t1.c as c from t1",
	}, {
		sql:    "select t1.* from t1",
		expSQL: "select t1.a as a, t1.b as b, t1.c as c from t1",
	}, {
		sql:    "select *, 42, t1.* from t1",
		expSQL: "select t1.a as a, t1.b as b, t1.c as c, 42, t1.a as a, t1.b as b, t1.c as c from t1",
	}, {
		sql:    "select 42, t1.* from t1",
		expSQL: "select 42, t1.a as a, t1.b as b, t1.c as c from t1",
	}, {
		sql:    "select * from t1, t2",
		expSQL: "select t1.a as a, t1.b as b, t1.c as c, t2.c1 as c1, t2.c2 as c2 from t1, t2",
	}, {
		sql:    "select t1.* from t1, t2",
		expSQL: "select t1.a as a, t1.b as b, t1.c as c from t1, t2",
	}, {
		sql:    "select *, t1.* from t1, t2",
		expSQL: "select t1.a as a, t1.b as b, t1.c as c, t2.c1 as c1, t2.c2 as c2, t1.a as a, t1.b as b, t1.c as c from t1, t2",
	}, { // aliased table
		sql:    "select * from t1 a, t2 b",
		expSQL: "select a.a as a, a.b as b, a.c as c, b.c1 as c1, b.c2 as c2 from t1 as a, t2 as b",
	}, { // t3 is non-authoritative table
		sql:    "select * from t3",
		expSQL: "select * from t3",
	}, { // t3 is non-authoritative table
		sql:    "select * from t1, t2, t3",
		expSQL: "select * from t1, t2, t3",
	}, { // t3 is non-authoritative table
		sql:    "select t1.*, t2.*, t3.* from t1, t2, t3",
		expSQL: "select t1.a as a, t1.b as b, t1.c as c, t2.c1 as c1, t2.c2 as c2, t3.* from t1, t2, t3",
	}, {
		sql:    "select foo.* from t1, t2",
		expErr: "Unknown table 'foo'",
	}}
	for _, tcase := range tcases {
		t.Run(tcase.sql, func(t *testing.T) {
			ast, err := sqlparser.Parse(tcase.sql)
			require.NoError(t, err)
			selectStatement, isSelectStatement := ast.(*sqlparser.Select)
			require.True(t, isSelectStatement, "analyzer expects a select statement")
			semTable, err := semantics.Analyze(selectStatement, cDB, schemaInfo)
			require.NoError(t, err)
			expandedSelect, err := rewrite(selectStatement, semTable, nil)
			if tcase.expErr == "" {
				require.NoError(t, err)
				assert.Equal(t, tcase.expSQL, sqlparser.String(expandedSelect))
			} else {
				require.EqualError(t, err, tcase.expErr)
			}
		})
	}
}

func TestSemTableDependenciesAfterExpandStar(t *testing.T) {
	schemaInfo := &semantics.FakeSI{Tables: map[string]*vindexes.Table{
		"t1": {
			Name: sqlparser.NewTableIdent("t1"),
			Columns: []vindexes.Column{{
				Name: sqlparser.NewColIdent("a"),
				Type: sqltypes.VarChar,
			}},
			ColumnListAuthoritative: true,
		}}}
	tcases := []struct {
		sql         string
		expSQL      string
		sameTbl     int
		otherTbl    int
		expandedCol int
	}{{
		sql:      "select a, * from t1",
		expSQL:   "select a, t1.a as a from t1",
		otherTbl: -1, sameTbl: 0, expandedCol: 1,
	}, {
		sql:      "select t2.a, t1.a, t1.* from t1, t2",
		expSQL:   "select t2.a, t1.a, t1.a as a from t1, t2",
		otherTbl: 0, sameTbl: 1, expandedCol: 2,
	}, {
		sql:      "select t2.a, t.a, t.* from t1 t, t2",
		expSQL:   "select t2.a, t.a, t.a as a from t1 as t, t2",
		otherTbl: 0, sameTbl: 1, expandedCol: 2,
	}}
	for _, tcase := range tcases {
		t.Run(tcase.sql, func(t *testing.T) {
			ast, err := sqlparser.Parse(tcase.sql)
			require.NoError(t, err)
			selectStatement, isSelectStatement := ast.(*sqlparser.Select)
			require.True(t, isSelectStatement, "analyzer expects a select statement")
			semTable, err := semantics.Analyze(selectStatement, "", schemaInfo)
			require.NoError(t, err)
			expandedSelect, err := rewrite(selectStatement, semTable, nil)
			require.NoError(t, err)
			assert.Equal(t, tcase.expSQL, sqlparser.String(expandedSelect))
			if tcase.otherTbl != -1 {
				assert.NotEqual(t,
					semTable.BaseTableDependencies(expandedSelect.SelectExprs[tcase.otherTbl].(*sqlparser.AliasedExpr).Expr),
					semTable.BaseTableDependencies(expandedSelect.SelectExprs[tcase.expandedCol].(*sqlparser.AliasedExpr).Expr),
				)
			}
			if tcase.sameTbl != -1 {
				assert.Equal(t,
					semTable.BaseTableDependencies(expandedSelect.SelectExprs[tcase.sameTbl].(*sqlparser.AliasedExpr).Expr),
					semTable.BaseTableDependencies(expandedSelect.SelectExprs[tcase.expandedCol].(*sqlparser.AliasedExpr).Expr),
				)
			}
		})
	}
}

func TestSubqueryRewrite(t *testing.T) {
	tcases := []struct {
		input  string
		output string
	}{{
		input:  "select 1 from t1",
		output: "select 1 from t1",
	}, {
		input:  "select (select 1) from t1",
		output: "select :__sq1 from t1",
	}, {
		input: "select 1 from t1 where exists (select 1)",
		output: "select 1 from t1 where :__sq_has_values1",
	}, {
		input:  "select id from t1 where id in (select 1)",
		output: "select id from t1 where id in ::__sq1",
	}, {
		input:  "select id from t1 where id not in (select 1)",
		output: "select id from t1 where id not in ::__sq1",
	}, {
		input:  "select id from t1 where id = (select 1)",
		output: "select id from t1 where id = :__sq1",
	}, {
		input:  "select id from t1 where id >= (select 1)",
		output: "select id from t1 where id >= :__sq1",
	}, {
		input:  "select id from t1 where t1.id = (select 1 from t2 where t2.id = t1.id)",
		output: "select id from t1 where t1.id = :__sq1",
	}, {
		input: "select id from t1 join t2 where t1.id = t2.id and exists (select 1)",
		output: "select id from t1 join t2 where t1.id = t2.id and :__sq_has_values1",
	}, {
		input: "select id from t1 where not exists (select 1)",
		output: "select id from t1 where not :__sq_has_values1",
	}, {
		input: "select id from t1 where not exists (select 1) and exists (select 2)",
		output: "select id from t1 where not :__sq_has_values1 and :__sq_has_values2",
	}, {
		input:  "select (select 1), (select 2) from t1 join t2 on t1.id = (select 1) where t1.id in (select 1)",
		output: "select :__sq2, :__sq3 from t1 join t2 on t1.id = :__sq1 where t1.id in ::__sq4",
	}}
	for _, tcase := range tcases {
		t.Run(tcase.input, func(t *testing.T) {
			ast, err := sqlparser.Parse(tcase.input)
			require.NoError(t, err)
			selectStatement, isSelectStatement := ast.(*sqlparser.Select)
			require.True(t, isSelectStatement, "analyzer expects a select statement")
			semTable, err := semantics.Analyze(selectStatement, "", &semantics.FakeSI{})
			require.NoError(t, err)
			rewrittenAST, err := rewrite(selectStatement, semTable, sqlparser.NewReservedVars("vtg", map[string]struct{}{}))
			require.NoError(t, err)
			assert.Equal(t, tcase.output, sqlparser.String(rewrittenAST))
		})
	}
}
