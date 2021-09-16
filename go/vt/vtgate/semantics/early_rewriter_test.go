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

package semantics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestExpandStar(t *testing.T) {
	schemaInfo := &FakeSI{
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
		expSQL: "select a, b, c from t1",
	}, {
		sql:    "select t1.* from t1",
		expSQL: "select a, b, c from t1",
	}, {
		sql:    "select *, 42, t1.* from t1",
		expSQL: "select a, b, c, 42, a, b, c from t1",
	}, {
		sql:    "select 42, t1.* from t1",
		expSQL: "select 42, a, b, c from t1",
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
			_, err = Analyze(selectStatement, cDB, schemaInfo)
			if tcase.expErr == "" {
				require.NoError(t, err)
				assert.Equal(t, tcase.expSQL, sqlparser.String(selectStatement))
			} else {
				require.EqualError(t, err, tcase.expErr)
			}
		})
	}
}

func TestOrderByGroupByLiteral(t *testing.T) {
	schemaInfo := &FakeSI{
		Tables: map[string]*vindexes.Table{},
	}
	cDB := "db"
	tcases := []struct {
		sql    string
		expSQL string
		expErr string
	}{{
		sql:    "select 1 as id from t1 order by 1",
		expSQL: "select 1 as id from t1 order by id asc",
	}, {
		sql:    "select t1.col from t1 order by 1",
		expSQL: "select t1.col from t1 order by t1.col asc",
	}, {
		sql:    "select t1.col from t1 group by 1",
		expSQL: "select t1.col from t1 group by t1.col",
	}, {
		sql:    "select t1.col as xyz from t1 group by 1",
		expSQL: "select t1.col as xyz from t1 group by xyz",
	}, {
		sql:    "select t1.col as xyz, count(*) from t1 group by 1 order by 2",
		expSQL: "select t1.col as xyz, count(*) from t1 group by xyz order by count(*) asc",
	}, {
		sql:    "select id from t1 group by 2",
		expErr: "Unknown column '2' in 'group statement'",
	}, {
		sql:    "select id from t1 order by 2",
		expErr: "Unknown column '2' in 'order clause'",
	}, {
		sql:    "select *, id from t1 order by 2",
		expErr: "cannot use column offsets in order clause when using `*`",
	}, {
		sql:    "select *, id from t1 group by 2",
		expErr: "cannot use column offsets in group statement when using `*`",
	}}
	for _, tcase := range tcases {
		t.Run(tcase.sql, func(t *testing.T) {
			ast, err := sqlparser.Parse(tcase.sql)
			require.NoError(t, err)
			selectStatement := ast.(*sqlparser.Select)
			_, err = Analyze(selectStatement, cDB, schemaInfo)
			if tcase.expErr == "" {
				require.NoError(t, err)
				assert.Equal(t, tcase.expSQL, sqlparser.String(selectStatement))
			} else {
				require.EqualError(t, err, tcase.expErr)
			}
		})
	}
}

func TestSemTableDependenciesAfterExpandStar(t *testing.T) {
	schemaInfo := &FakeSI{Tables: map[string]*vindexes.Table{
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
		expSQL:   "select a, a from t1",
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
			semTable, err := Analyze(selectStatement, "", schemaInfo)
			require.NoError(t, err)
			assert.Equal(t, tcase.expSQL, sqlparser.String(selectStatement))
			if tcase.otherTbl != -1 {
				assert.NotEqual(t,
					semTable.RecursiveDeps(selectStatement.SelectExprs[tcase.otherTbl].(*sqlparser.AliasedExpr).Expr),
					semTable.RecursiveDeps(selectStatement.SelectExprs[tcase.expandedCol].(*sqlparser.AliasedExpr).Expr),
				)
			}
			if tcase.sameTbl != -1 {
				assert.Equal(t,
					semTable.RecursiveDeps(selectStatement.SelectExprs[tcase.sameTbl].(*sqlparser.AliasedExpr).Expr),
					semTable.RecursiveDeps(selectStatement.SelectExprs[tcase.expandedCol].(*sqlparser.AliasedExpr).Expr),
				)
			}
		})
	}
}
