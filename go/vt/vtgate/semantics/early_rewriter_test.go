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
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/sqlparser"
	"vitess.io/vitess/go/vt/vtgate/vindexes"
)

func TestExpandStar(t *testing.T) {
	ks := &vindexes.Keyspace{
		Name:    "main",
		Sharded: true,
	}
	schemaInfo := &FakeSI{
		Tables: map[string]*vindexes.Table{
			"t1": {
				Keyspace: ks,
				Name:     sqlparser.NewIdentifierCS("t1"),
				Columns: []vindexes.Column{{
					Name: sqlparser.NewIdentifierCI("a"),
					Type: sqltypes.VarChar,
				}, {
					Name: sqlparser.NewIdentifierCI("b"),
					Type: sqltypes.VarChar,
				}, {
					Name: sqlparser.NewIdentifierCI("c"),
					Type: sqltypes.VarChar,
				}, {
					Name:      sqlparser.NewIdentifierCI("secret"),
					Type:      sqltypes.Decimal,
					Invisible: true,
				}},
				ColumnListAuthoritative: true,
			},
			"t2": {
				Keyspace: ks,
				Name:     sqlparser.NewIdentifierCS("t2"),
				Columns: []vindexes.Column{{
					Name: sqlparser.NewIdentifierCI("c1"),
					Type: sqltypes.VarChar,
				}, {
					Name: sqlparser.NewIdentifierCI("c2"),
					Type: sqltypes.VarChar,
				}},
				ColumnListAuthoritative: true,
			},
			"t3": { // non authoritative table.
				Keyspace: ks,
				Name:     sqlparser.NewIdentifierCS("t3"),
				Columns: []vindexes.Column{{
					Name: sqlparser.NewIdentifierCI("col"),
					Type: sqltypes.VarChar,
				}},
				ColumnListAuthoritative: false,
			},
			"t4": {
				Keyspace: ks,
				Name:     sqlparser.NewIdentifierCS("t4"),
				Columns: []vindexes.Column{{
					Name: sqlparser.NewIdentifierCI("c1"),
					Type: sqltypes.VarChar,
				}, {
					Name: sqlparser.NewIdentifierCI("c4"),
					Type: sqltypes.VarChar,
				}},
				ColumnListAuthoritative: true,
			},
			"t5": {
				Keyspace: ks,
				Name:     sqlparser.NewIdentifierCS("t5"),
				Columns: []vindexes.Column{{
					Name: sqlparser.NewIdentifierCI("a"),
					Type: sqltypes.VarChar,
				}, {
					Name: sqlparser.NewIdentifierCI("b"),
					Type: sqltypes.VarChar,
				}},
				ColumnListAuthoritative: true,
			},
		},
	}
	cDB := "db"
	tcases := []struct {
		sql      string
		expSQL   string
		expErr   string
		expanded string
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
		sql:      "select * from t1, t2",
		expSQL:   "select t1.a, t1.b, t1.c, t2.c1, t2.c2 from t1, t2",
		expanded: "main.t1.a, main.t1.b, main.t1.c, main.t2.c1, main.t2.c2",
	}, {
		sql:    "select t1.* from t1, t2",
		expSQL: "select t1.a, t1.b, t1.c from t1, t2",
	}, {
		sql:    "select *, t1.* from t1, t2",
		expSQL: "select t1.a, t1.b, t1.c, t2.c1, t2.c2, t1.a, t1.b, t1.c from t1, t2",
	}, { // aliased table
		sql:    "select * from t1 a, t2 b",
		expSQL: "select a.a, a.b, a.c, b.c1, b.c2 from t1 as a, t2 as b",
	}, { // t3 is non-authoritative table
		sql:    "select * from t3",
		expSQL: "select * from t3",
	}, { // t3 is non-authoritative table
		sql:    "select * from t1, t2, t3",
		expSQL: "select * from t1, t2, t3",
	}, { // t3 is non-authoritative table
		sql:    "select t1.*, t2.*, t3.* from t1, t2, t3",
		expSQL: "select t1.a, t1.b, t1.c, t2.c1, t2.c2, t3.* from t1, t2, t3",
	}, {
		sql:    "select foo.* from t1, t2",
		expErr: "Unknown table 'foo'",
	}, {
		sql:    "select * from t1 join t2 on t1.a = t2.c1",
		expSQL: "select t1.a, t1.b, t1.c, t2.c1, t2.c2 from t1 join t2 on t1.a = t2.c1",
	}, {
		sql:    "select * from t1 left join t2 on t1.a = t2.c1",
		expSQL: "select t1.a, t1.b, t1.c, t2.c1, t2.c2 from t1 left join t2 on t1.a = t2.c1",
	}, {
		sql:    "select * from t1 right join t2 on t1.a = t2.c1",
		expSQL: "select t1.a, t1.b, t1.c, t2.c1, t2.c2 from t1 right join t2 on t1.a = t2.c1",
	}, {
		sql:      "select * from t2 join t4 using (c1)",
		expSQL:   "select t2.c1, t2.c2, t4.c4 from t2 join t4 on t2.c1 = t4.c1",
		expanded: "main.t2.c1, main.t2.c2, main.t4.c4",
	}, {
		sql:    "select * from t2 join t4 using (c1) join t2 as X using (c1)",
		expSQL: "select t2.c1, t2.c2, t4.c4, X.c2 from t2 join t4 on t2.c1 = t4.c1 join t2 as X on t2.c1 = t4.c1 and t2.c1 = X.c1 and t4.c1 = X.c1",
	}, {
		sql:    "select * from t2 join t4 using (c1), t2 as t2b join t4 as t4b using (c1)",
		expSQL: "select t2.c1, t2.c2, t4.c4, t2b.c1, t2b.c2, t4b.c4 from t2 join t4 on t2.c1 = t4.c1, t2 as t2b join t4 as t4b on t2b.c1 = t4b.c1",
	}, {
		sql:      "select * from t1 join t5 using (b)",
		expSQL:   "select t1.b, t1.a, t1.c, t5.a from t1 join t5 on t1.b = t5.b",
		expanded: "main.t1.a, main.t1.b, main.t1.c, main.t5.a",
	}, {
		sql:    "select * from t1 join t5 using (b) having b = 12",
		expSQL: "select t1.b, t1.a, t1.c, t5.a from t1 join t5 on t1.b = t5.b having t1.b = 12",
	}, {
		sql:    "select 1 from t1 join t5 using (b) where b = 12",
		expSQL: "select 1 from t1 join t5 on t1.b = t5.b where t1.b = 12",
	}, {
		sql:    "select * from (select 12) as t",
		expSQL: "select `12` from (select 12 from dual) as t",
	}, {
		sql:    "SELECT * FROM (SELECT *, 12 AS foo FROM t3) as results",
		expSQL: "select * from (select *, 12 as foo from t3) as results",
	}, {
		// if we are only star-expanding authoritative tables, we don't need to stop the expansion
		sql:    "SELECT * FROM (SELECT t2.*, 12 AS foo FROM t3, t2) as results",
		expSQL: "select c1, c2, foo from (select t2.c1, t2.c2, 12 as foo from t3, t2) as results",
	}}
	for _, tcase := range tcases {
		t.Run(tcase.sql, func(t *testing.T) {
			ast, err := sqlparser.NewTestParser().Parse(tcase.sql)
			require.NoError(t, err)
			selectStatement, isSelectStatement := ast.(*sqlparser.Select)
			require.True(t, isSelectStatement, "analyzer expects a select statement")
			st, err := Analyze(selectStatement, cDB, schemaInfo)
			if tcase.expErr == "" {
				require.NoError(t, err)
				require.NoError(t, st.NotUnshardedErr)
				require.NoError(t, st.NotSingleRouteErr)
				assert.Equal(t, tcase.expSQL, sqlparser.String(selectStatement))
				assertExpandedColumns(t, st, tcase.expanded)
			} else {
				require.EqualError(t, err, tcase.expErr)
			}
		})
	}
}

func assertExpandedColumns(t *testing.T, st *SemTable, expandedColumns string) {
	t.Helper()
	if expandedColumns == "" {
		return
	}
	var expanded []string
	for tbl, cols := range st.ExpandedColumns {
		for _, col := range cols {
			col.Qualifier = tbl
			expanded = append(expanded, sqlparser.String(col))
		}
	}
	sort.Strings(expanded)
	assert.Equal(t, expandedColumns, strings.Join(expanded, ", "))
}

func TestRewriteJoinUsingColumns(t *testing.T) {
	schemaInfo := &FakeSI{
		Tables: map[string]*vindexes.Table{
			"t1": {
				Name: sqlparser.NewIdentifierCS("t1"),
				Columns: []vindexes.Column{{
					Name: sqlparser.NewIdentifierCI("a"),
					Type: sqltypes.VarChar,
				}, {
					Name: sqlparser.NewIdentifierCI("b"),
					Type: sqltypes.VarChar,
				}, {
					Name: sqlparser.NewIdentifierCI("c"),
					Type: sqltypes.VarChar,
				}},
				ColumnListAuthoritative: true,
			},
			"t2": {
				Name: sqlparser.NewIdentifierCS("t2"),
				Columns: []vindexes.Column{{
					Name: sqlparser.NewIdentifierCI("a"),
					Type: sqltypes.VarChar,
				}, {
					Name: sqlparser.NewIdentifierCI("b"),
					Type: sqltypes.VarChar,
				}, {
					Name: sqlparser.NewIdentifierCI("c"),
					Type: sqltypes.VarChar,
				}},
				ColumnListAuthoritative: true,
			},
			"t3": {
				Name: sqlparser.NewIdentifierCS("t3"),
				Columns: []vindexes.Column{{
					Name: sqlparser.NewIdentifierCI("a"),
					Type: sqltypes.VarChar,
				}, {
					Name: sqlparser.NewIdentifierCI("b"),
					Type: sqltypes.VarChar,
				}, {
					Name: sqlparser.NewIdentifierCI("c"),
					Type: sqltypes.VarChar,
				}},
				ColumnListAuthoritative: true,
			},
		},
	}
	cDB := "db"
	tcases := []struct {
		sql    string
		expSQL string
		expErr string
	}{{
		sql:    "select 1 from t1 join t2 using (a) where a = 42",
		expSQL: "select 1 from t1 join t2 on t1.a = t2.a where t1.a = 42",
	}, {
		sql:    "select 1 from t1 join t2 using (a), t3 where a = 42",
		expErr: "Column 'a' in field list is ambiguous",
	}, {
		sql:    "select 1 from t1 join t2 using (a), t1 as b join t3 on (a) where a = 42",
		expErr: "Column 'a' in field list is ambiguous",
	}, {
		sql:    "select 1 from t1 left join t2 using (a) where a = 42",
		expSQL: "select 1 from t1 left join t2 on t1.a = t2.a where t1.a = 42",
	}}
	for _, tcase := range tcases {
		t.Run(tcase.sql, func(t *testing.T) {
			ast, err := sqlparser.NewTestParser().Parse(tcase.sql)
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

func TestGroupByColumnName(t *testing.T) {
	schemaInfo := &FakeSI{
		Tables: map[string]*vindexes.Table{
			"t1": {
				Name: sqlparser.NewIdentifierCS("t1"),
				Columns: []vindexes.Column{{
					Name: sqlparser.NewIdentifierCI("id"),
					Type: sqltypes.Int32,
				}, {
					Name: sqlparser.NewIdentifierCI("col1"),
					Type: sqltypes.Int32,
				}},
				ColumnListAuthoritative: true,
			},
			"t2": {
				Name: sqlparser.NewIdentifierCS("t2"),
				Columns: []vindexes.Column{{
					Name: sqlparser.NewIdentifierCI("id"),
					Type: sqltypes.Int32,
				}, {
					Name: sqlparser.NewIdentifierCI("col2"),
					Type: sqltypes.Int32,
				}},
				ColumnListAuthoritative: true,
			},
		},
	}
	cDB := "db"
	tcases := []struct {
		sql     string
		expSQL  string
		expDeps TableSet
		expErr  string
		warning string
	}{{
		sql:     "select t3.col from t3 group by kj",
		expSQL:  "select t3.col from t3 group by kj",
		expDeps: TS0,
	}, {
		sql:     "select t2.col2 as xyz from t2 group by xyz",
		expSQL:  "select t2.col2 as xyz from t2 group by t2.col2",
		expDeps: TS0,
	}, {
		sql:    "select id from t1 group by unknown",
		expErr: "Unknown column 'unknown' in 'group statement'",
	}, {
		sql:    "select t1.c as x, sum(t2.id) as x from t1 join t2 group by x",
		expErr: "VT03005: cannot group on 'x'",
	}, {
		sql:     "select t1.col1, sum(t2.id) as col1 from t1 join t2 group by col1",
		expSQL:  "select t1.col1, sum(t2.id) as col1 from t1 join t2 group by col1",
		expDeps: TS0,
		warning: "Column 'col1' in group statement is ambiguous",
	}, {
		sql:     "select t2.col2 as id, sum(t2.id) as x from t1 join t2 group by id",
		expSQL:  "select t2.col2 as id, sum(t2.id) as x from t1 join t2 group by t2.col2",
		expDeps: TS1,
	}, {
		sql:    "select sum(t2.col2) as id, sum(t2.id) as x from t1 join t2 group by id",
		expErr: "VT03005: cannot group on 'id'",
	}, {
		sql:    "select count(*) as x from t1 group by x",
		expErr: "VT03005: cannot group on 'x'",
	}}
	for _, tcase := range tcases {
		t.Run(tcase.sql, func(t *testing.T) {
			ast, err := sqlparser.NewTestParser().Parse(tcase.sql)
			require.NoError(t, err)
			selectStatement := ast.(*sqlparser.Select)
			st, err := AnalyzeStrict(selectStatement, cDB, schemaInfo)
			if tcase.expErr == "" {
				require.NoError(t, err)
				assert.Equal(t, tcase.expSQL, sqlparser.String(selectStatement))
				gb := selectStatement.GroupBy
				deps := st.RecursiveDeps(gb[0])
				assert.Equal(t, tcase.expDeps, deps)
				assert.Equal(t, tcase.warning, st.Warning)
			} else {
				require.EqualError(t, err, tcase.expErr)
			}
		})
	}
}

func TestGroupByLiteral(t *testing.T) {
	schemaInfo := &FakeSI{
		Tables: map[string]*vindexes.Table{},
	}
	cDB := "db"
	tcases := []struct {
		sql     string
		expSQL  string
		expDeps TableSet
		expErr  string
	}{{
		sql:     "select t1.col from t1 group by 1",
		expSQL:  "select t1.col from t1 group by t1.col",
		expDeps: TS0,
	}, {
		sql:     "select t1.col as xyz from t1 group by 1",
		expSQL:  "select t1.col as xyz from t1 group by t1.col",
		expDeps: TS0,
	}, {
		sql:    "select id from t1 group by 2",
		expErr: "Unknown column '2' in 'group clause'",
	}, {
		sql:    "select *, id from t1 group by 2",
		expErr: "cannot use column offsets in group clause when using `*`",
	}}
	for _, tcase := range tcases {
		t.Run(tcase.sql, func(t *testing.T) {
			ast, err := sqlparser.NewTestParser().Parse(tcase.sql)
			require.NoError(t, err)
			selectStatement := ast.(*sqlparser.Select)
			st, err := Analyze(selectStatement, cDB, schemaInfo)
			if tcase.expErr == "" {
				require.NoError(t, err)
				assert.Equal(t, tcase.expSQL, sqlparser.String(selectStatement))
				gb := selectStatement.GroupBy
				deps := st.RecursiveDeps(gb[0])
				assert.Equal(t, tcase.expDeps, deps)
			} else {
				require.EqualError(t, err, tcase.expErr)
			}
		})
	}
}

func TestOrderByLiteral(t *testing.T) {
	schemaInfo := &FakeSI{
		Tables: map[string]*vindexes.Table{},
	}
	cDB := "db"
	tcases := []struct {
		sql     string
		expSQL  string
		expDeps TableSet
		expErr  string
	}{{
		sql:     "select 1 as id from t1 order by 1",
		expSQL:  "select 1 as id from t1 order by '' asc",
		expDeps: NoTables,
	}, {
		sql:     "select t1.col from t1 order by 1",
		expSQL:  "select t1.col from t1 order by t1.col asc",
		expDeps: TS0,
	}, {
		sql:     "select t1.col from t1 order by 1.0",
		expSQL:  "select t1.col from t1 order by 1.0 asc",
		expDeps: NoTables,
	}, {
		sql:     "select t1.col from t1 order by 'fubick'",
		expSQL:  "select t1.col from t1 order by 'fubick' asc",
		expDeps: NoTables,
	}, {
		sql:     "select t1.col as foo from t1 order by 1",
		expSQL:  "select t1.col as foo from t1 order by t1.col asc",
		expDeps: TS0,
	}, {
		sql:     "select t1.col as xyz, count(*) from t1 group by 1 order by 2",
		expSQL:  "select t1.col as xyz, count(*) from t1 group by t1.col order by count(*) asc",
		expDeps: TS0,
	}, {
		sql:    "select id from t1 order by 2",
		expErr: "Unknown column '2' in 'order clause'",
	}, {
		sql:    "select *, id from t1 order by 2",
		expErr: "cannot use column offsets in order clause when using `*`",
	}, {
		sql:     "select id from t1 order by 1 collate utf8_general_ci",
		expSQL:  "select id from t1 order by id collate utf8_general_ci asc",
		expDeps: TS0,
	}, {
		sql:     "select id from `user` union select 1 from dual order by 1",
		expSQL:  "select id from `user` union select 1 from dual order by id asc",
		expDeps: TS0,
	}, {
		sql:    "select id from t1 order by 2",
		expErr: "Unknown column '2' in 'order clause'",
	}, {
		sql:     "select a.id, b.id from user as a, user_extra as b union select 1, 2 order by 1",
		expSQL:  "select a.id, b.id from `user` as a, user_extra as b union select 1, 2 from dual order by id asc",
		expDeps: TS0,
	}, {
		sql:     "select a.id, b.id from user as a, user_extra as b union select 1, 2 order by 2",
		expSQL:  "select a.id, b.id from `user` as a, user_extra as b union select 1, 2 from dual order by id asc",
		expDeps: TS1,
	}, {
		sql:     "select user.id as foo from user union select col from user_extra order by 1",
		expSQL:  "select `user`.id as foo from `user` union select col from user_extra order by foo asc",
		expDeps: MergeTableSets(TS0, TS1),
	}}
	for _, tcase := range tcases {
		t.Run(tcase.sql, func(t *testing.T) {
			ast, err := sqlparser.NewTestParser().Parse(tcase.sql)
			require.NoError(t, err)
			selectStatement := ast.(sqlparser.SelectStatement)
			st, err := Analyze(selectStatement, cDB, schemaInfo)
			if tcase.expErr == "" {
				require.NoError(t, err)
				assert.Equal(t, tcase.expSQL, sqlparser.String(selectStatement))
				ordering := selectStatement.GetOrderBy()
				deps := st.RecursiveDeps(ordering[0].Expr)
				assert.Equal(t, tcase.expDeps, deps)
			} else {
				require.EqualError(t, err, tcase.expErr)
			}
		})
	}
}

func TestHavingColumnName(t *testing.T) {
	schemaInfo := getSchemaWithKnownColumns()
	cDB := "db"
	tcases := []struct {
		sql     string
		expSQL  string
		expDeps TableSet
		expErr  string
		warning string
	}{{
		sql:     "select id, sum(foo) as sumOfFoo from t1 having sumOfFoo > 1",
		expSQL:  "select id, sum(foo) as sumOfFoo from t1 having sum(t1.foo) > 1",
		expDeps: TS0,
	}, {
		sql:    "select id as X, sum(foo) as X from t1 having X > 1",
		expErr: "Column 'X' in field list is ambiguous",
	}, {
		sql:     "select id, sum(t1.foo) as foo from t1 having sum(foo) > 1",
		expSQL:  "select id, sum(t1.foo) as foo from t1 having sum(foo) > 1",
		expDeps: TS0,
		warning: "Column 'foo' in having clause is ambiguous",
	}, {
		sql:    "select id, sum(t1.foo) as XYZ from t1 having sum(XYZ) > 1",
		expErr: "Invalid use of group function",
	}, {
		sql:     "select foo + 2 as foo from t1 having foo = 42",
		expSQL:  "select foo + 2 as foo from t1 having t1.foo + 2 = 42",
		expDeps: TS0,
	}, {
		sql:    "select count(*), ename from emp group by ename having comm > 1000",
		expErr: "Unknown column 'comm' in 'having clause'",
	}, {
		sql:     "select sal, ename from emp having empno > 1000",
		expSQL:  "select sal, ename from emp having empno > 1000",
		expDeps: TS0,
	}, {
		sql:    "select foo, count(*) foo from t1 group by foo having foo > 1000",
		expErr: "Column 'foo' in field list is ambiguous",
	}, {
		sql:     "select foo, count(*) foo from t1, emp group by foo having sum(sal) > 1000",
		expSQL:  "select foo, count(*) as foo from t1, emp group by foo having sum(sal) > 1000",
		expDeps: TS1,
		warning: "Column 'foo' in group statement is ambiguous",
	}, {
		sql:     "select foo as X, sal as foo from t1, emp having sum(X) > 1000",
		expSQL:  "select foo as X, sal as foo from t1, emp having sum(t1.foo) > 1000",
		expDeps: TS0,
	}, {
		sql:     "select count(*) a from someTable having a = 10",
		expSQL:  "select count(*) as a from someTable having count(*) = 10",
		expDeps: TS0,
	}, {
		sql:     "select count(*) from emp having ename = 10",
		expSQL:  "select count(*) from emp having ename = 10",
		expDeps: TS0,
	}, {
		sql:     "select sum(sal) empno from emp where ename > 0 having empno = 2",
		expSQL:  "select sum(sal) as empno from emp where ename > 0 having sum(emp.sal) = 2",
		expDeps: TS0,
	}, {
		// test with missing schema info
		sql:    "select foo, count(bar) as x from someTable group by foo having id > avg(baz)",
		expErr: "Unknown column 'id' in 'having clause'",
	}, {
		sql:     "select t1.foo as alias, count(bar) as x from t1 group by foo having foo+54 = 56",
		expSQL:  "select t1.foo as alias, count(bar) as x from t1 group by foo having foo + 54 = 56",
		expDeps: TS0,
	}, {
		sql:     "select 1 from t1 group by foo having foo = 1 and count(*) > 1",
		expSQL:  "select 1 from t1 group by foo having foo = 1 and count(*) > 1",
		expDeps: TS0,
	}}

	for _, tcase := range tcases {
		t.Run(tcase.sql, func(t *testing.T) {
			ast, err := sqlparser.NewTestParser().Parse(tcase.sql)
			require.NoError(t, err)
			selectStatement := ast.(*sqlparser.Select)
			semTbl, err := AnalyzeStrict(selectStatement, cDB, schemaInfo)
			if tcase.expErr == "" {
				require.NoError(t, err)
				assert.Equal(t, tcase.expSQL, sqlparser.String(selectStatement))
				assert.Equal(t, tcase.expDeps, semTbl.RecursiveDeps(selectStatement.Having.Expr))
				assert.Equal(t, tcase.warning, semTbl.Warning, "warning")
			} else {
				require.EqualError(t, err, tcase.expErr)
			}
		})
	}
}

func getSchemaWithKnownColumns() *FakeSI {
	schemaInfo := &FakeSI{
		Tables: map[string]*vindexes.Table{
			"t1": {
				Keyspace: &vindexes.Keyspace{Name: "ks", Sharded: true},
				Name:     sqlparser.NewIdentifierCS("t1"),
				Columns: []vindexes.Column{{
					Name: sqlparser.NewIdentifierCI("id"),
					Type: sqltypes.VarChar,
				}, {
					Name: sqlparser.NewIdentifierCI("foo"),
					Type: sqltypes.VarChar,
				}, {
					Name: sqlparser.NewIdentifierCI("bar"),
					Type: sqltypes.VarChar,
				}},
				ColumnListAuthoritative: true,
			},
			"emp": {
				Keyspace: &vindexes.Keyspace{Name: "ks", Sharded: true},
				Name:     sqlparser.NewIdentifierCS("emp"),
				Columns: []vindexes.Column{{
					Name: sqlparser.NewIdentifierCI("empno"),
					Type: sqltypes.Int64,
				}, {
					Name: sqlparser.NewIdentifierCI("ename"),
					Type: sqltypes.VarChar,
				}, {
					Name: sqlparser.NewIdentifierCI("sal"),
					Type: sqltypes.Int64,
				}},
				ColumnListAuthoritative: true,
			},
		},
	}
	return schemaInfo
}

func TestOrderByColumnName(t *testing.T) {
	schemaInfo := getSchemaWithKnownColumns()
	cDB := "db"
	tcases := []struct {
		sql     string
		expSQL  string
		expErr  string
		warning string
		deps    TableSet
	}{{
		sql:    "select id, sum(foo) as sumOfFoo from t1 order by sumOfFoo",
		expSQL: "select id, sum(foo) as sumOfFoo from t1 order by sum(t1.foo) asc",
		deps:   TS0,
	}, {
		sql:    "select id, sum(foo) as sumOfFoo from t1 order by sumOfFoo + 1",
		expSQL: "select id, sum(foo) as sumOfFoo from t1 order by sum(t1.foo) + 1 asc",
		deps:   TS0,
	}, {
		sql:    "select id, sum(foo) as sumOfFoo from t1 order by abs(sumOfFoo)",
		expSQL: "select id, sum(foo) as sumOfFoo from t1 order by abs(sum(t1.foo)) asc",
		deps:   TS0,
	}, {
		sql:    "select id, sum(foo) as sumOfFoo from t1 order by max(sumOfFoo)",
		expErr: "Invalid use of group function",
	}, {
		sql:     "select id, sum(foo) as foo from t1 order by foo + 1",
		expSQL:  "select id, sum(foo) as foo from t1 order by foo + 1 asc",
		deps:    TS0,
		warning: "Column 'foo' in order by statement is ambiguous",
	}, {
		sql:     "select id, sum(foo) as foo from t1 order by foo",
		expSQL:  "select id, sum(foo) as foo from t1 order by sum(t1.foo) asc",
		deps:    TS0,
		warning: "Column 'foo' in order by statement is ambiguous",
	}, {
		sql:     "select id, lower(min(foo)) as foo from t1 order by min(foo)",
		expSQL:  "select id, lower(min(foo)) as foo from t1 order by min(foo) asc",
		deps:    TS0,
		warning: "Column 'foo' in order by statement is ambiguous",
	}, {
		sql:     "select id, lower(min(foo)) as foo from t1 order by foo",
		expSQL:  "select id, lower(min(foo)) as foo from t1 order by lower(min(t1.foo)) asc",
		deps:    TS0,
		warning: "Column 'foo' in order by statement is ambiguous",
	}, {
		sql:     "select id, lower(min(foo)) as foo from t1 order by abs(foo)",
		expSQL:  "select id, lower(min(foo)) as foo from t1 order by abs(foo) asc",
		deps:    TS0,
		warning: "Column 'foo' in order by statement is ambiguous",
	}, {
		sql:     "select id, t1.bar as foo from t1 group by id order by min(foo)",
		expSQL:  "select id, t1.bar as foo from t1 group by id order by min(foo) asc",
		deps:    TS0,
		warning: "Column 'foo' in order by statement is ambiguous",
	}, {
		sql:    "select id, bar as id, count(*) from t1 order by id",
		expErr: "Column 'id' in field list is ambiguous",
	}, {
		sql:     "select id, id, count(*) from t1 order by id",
		expSQL:  "select id, id, count(*) from t1 order by t1.id asc",
		deps:    TS0,
		warning: "Column 'id' in order by statement is ambiguous",
	}, {
		sql:     "select id, count(distinct foo) k from t1 group by id order by k",
		expSQL:  "select id, count(distinct foo) as k from t1 group by id order by count(distinct t1.foo) asc",
		deps:    TS0,
		warning: "Column 'id' in group statement is ambiguous",
	}, {
		sql:    "select user.id as foo from user union select col from user_extra order by foo",
		expSQL: "select `user`.id as foo from `user` union select col from user_extra order by foo asc",
		deps:   MergeTableSets(TS0, TS1),
	}, {
		sql:    "select foo as X, sal as foo from t1, emp order by sum(X)",
		expSQL: "select foo as X, sal as foo from t1, emp order by sum(t1.foo) asc",
		deps:   TS0,
	}}
	for _, tcase := range tcases {
		t.Run(tcase.sql, func(t *testing.T) {
			ast, err := sqlparser.NewTestParser().Parse(tcase.sql)
			require.NoError(t, err)
			selectStatement := ast.(sqlparser.SelectStatement)
			semTable, err := AnalyzeStrict(selectStatement, cDB, schemaInfo)
			if tcase.expErr == "" {
				require.NoError(t, err)
				assert.Equal(t, tcase.expSQL, sqlparser.String(selectStatement))
				orderByExpr := selectStatement.GetOrderBy()[0].Expr
				assert.Equal(t, tcase.deps, semTable.RecursiveDeps(orderByExpr))
				assert.Equal(t, tcase.warning, semTable.Warning)
			} else {
				require.EqualError(t, err, tcase.expErr)
			}
		})
	}
}

func TestSemTableDependenciesAfterExpandStar(t *testing.T) {
	schemaInfo := &FakeSI{Tables: map[string]*vindexes.Table{
		"t1": {
			Name: sqlparser.NewIdentifierCS("t1"),
			Columns: []vindexes.Column{{
				Name: sqlparser.NewIdentifierCI("a"),
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
		expSQL:   "select t2.a, t1.a, t1.a from t1, t2",
		otherTbl: 0, sameTbl: 1, expandedCol: 2,
	}, {
		sql:      "select t2.a, t.a, t.* from t1 t, t2",
		expSQL:   "select t2.a, t.a, t.a from t1 as t, t2",
		otherTbl: 0, sameTbl: 1, expandedCol: 2,
	}}
	for _, tcase := range tcases {
		t.Run(tcase.sql, func(t *testing.T) {
			ast, err := sqlparser.NewTestParser().Parse(tcase.sql)
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

func TestRewriteNot(t *testing.T) {
	ks := &vindexes.Keyspace{
		Name:    "main",
		Sharded: true,
	}
	schemaInfo := &FakeSI{
		Tables: map[string]*vindexes.Table{
			"t1": {
				Keyspace: ks,
				Name:     sqlparser.NewIdentifierCS("t1"),
				Columns: []vindexes.Column{{
					Name: sqlparser.NewIdentifierCI("a"),
					Type: sqltypes.VarChar,
				}, {
					Name: sqlparser.NewIdentifierCI("b"),
					Type: sqltypes.VarChar,
				}, {
					Name: sqlparser.NewIdentifierCI("c"),
					Type: sqltypes.VarChar,
				}},
				ColumnListAuthoritative: true,
			},
		},
	}
	cDB := "db"
	tcases := []struct {
		sql      string
		expected string
	}{{
		sql:      "select a,b,c from t1 where not a = 12",
		expected: "select a, b, c from t1 where a != 12",
	}, {
		sql:      "select a from t1 where not a > 12",
		expected: "select a from t1 where a <= 12",
	}}
	for _, tcase := range tcases {
		t.Run(tcase.sql, func(t *testing.T) {
			ast, err := sqlparser.NewTestParser().Parse(tcase.sql)
			require.NoError(t, err)
			selectStatement, isSelectStatement := ast.(*sqlparser.Select)
			require.True(t, isSelectStatement, "analyzer expects a select statement")
			st, err := Analyze(selectStatement, cDB, schemaInfo)

			require.NoError(t, err)
			require.NoError(t, st.NotUnshardedErr)
			require.NoError(t, st.NotSingleRouteErr)
			assert.Equal(t, tcase.expected, sqlparser.String(selectStatement))
		})
	}
}

// TestConstantFolding tests that the rewriter is able to do various constant foldings properly.
func TestConstantFolding(t *testing.T) {
	ks := &vindexes.Keyspace{
		Name:    "main",
		Sharded: true,
	}
	schemaInfo := &FakeSI{
		Tables: map[string]*vindexes.Table{
			"t1": {
				Keyspace: ks,
				Name:     sqlparser.NewIdentifierCS("t1"),
				Columns: []vindexes.Column{{
					Name: sqlparser.NewIdentifierCI("a"),
					Type: sqltypes.VarChar,
				}, {
					Name: sqlparser.NewIdentifierCI("b"),
					Type: sqltypes.VarChar,
				}, {
					Name: sqlparser.NewIdentifierCI("c"),
					Type: sqltypes.VarChar,
				}},
				ColumnListAuthoritative: true,
			},
		},
	}
	cDB := "db"
	tcases := []struct {
		sql    string
		expSQL string
	}{{
		sql:    "select 1 from t1 where (a, b) in ::fkc_vals and (2 is null or (1 is null or a in (1)))",
		expSQL: "select 1 from t1 where (a, b) in ::fkc_vals and a in (1)",
	}, {
		sql:    "select 1 from t1 where (false or (false or a in (1)))",
		expSQL: "select 1 from t1 where a in (1)",
	}}
	for _, tcase := range tcases {
		t.Run(tcase.sql, func(t *testing.T) {
			ast, err := sqlparser.NewTestParser().Parse(tcase.sql)
			require.NoError(t, err)
			_, err = Analyze(ast, cDB, schemaInfo)
			require.NoError(t, err)
			require.Equal(t, tcase.expSQL, sqlparser.String(ast))
		})
	}
}

// TestCTEToDerivedTableRewrite checks that CTEs are correctly rewritten to derived tables
func TestCTEToDerivedTableRewrite(t *testing.T) {
	cDB := "db"
	tcases := []struct {
		sql    string
		expSQL string
	}{{
		sql:    "with x as (select 1 as id) select * from x",
		expSQL: "select id from (select 1 as id from dual) as x",
	}, {
		sql:    "with x as (select 1 as id), z as (select id + 1 from x) select * from z",
		expSQL: "select `id + 1` from (select id + 1 from (select 1 as id from dual) as x) as z",
	}, {
		sql:    "with x(id) as (select 1) select * from x",
		expSQL: "select id from (select 1 from dual) as x(id)",
	}}
	for _, tcase := range tcases {
		t.Run(tcase.sql, func(t *testing.T) {
			ast, err := sqlparser.NewTestParser().Parse(tcase.sql)
			require.NoError(t, err)
			_, err = Analyze(ast, cDB, fakeSchemaInfo())
			require.NoError(t, err)
			require.Equal(t, tcase.expSQL, sqlparser.String(ast))
		})
	}
}

// TestDeleteTargetTableRewrite checks that delete target rewrite is done correctly.
func TestDeleteTargetTableRewrite(t *testing.T) {
	cDB := "db"
	tcases := []struct {
		sql    string
		target string
	}{{
		sql:    "delete from t1",
		target: "t1",
	}, {
		sql:    "delete from t1 XYZ",
		target: "XYZ",
	}, {
		sql:    "delete t2 from t1 t1, t t2",
		target: "t2",
	}, {
		sql:    "delete t2,t1 from t t1, t t2",
		target: "t2, t1",
	}}
	for _, tcase := range tcases {
		t.Run(tcase.sql, func(t *testing.T) {
			ast, err := sqlparser.NewTestParser().Parse(tcase.sql)
			require.NoError(t, err)
			_, err = Analyze(ast, cDB, fakeSchemaInfo())
			require.NoError(t, err)
			require.Equal(t, tcase.target, sqlparser.String(ast.(*sqlparser.Delete).Targets))
		})
	}
}
